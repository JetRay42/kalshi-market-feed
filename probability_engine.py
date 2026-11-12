#!/usr/bin/env python3
"""Read-only sports/bookmaker & weather probability research overlays.

Input: existing Kalshi category data/feeds/{sports,weather_politics}/latest.json.
Output: data/probabilities/{sports,weather}/latest.json and summaries.
No account access, orders, authentication to Kalshi, or claims of guaranteed EV.
Python 3.11 standard library only.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import statistics
import time
import unicodedata
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

UTC = timezone.utc
USER_AGENT = 'kalshi-public-independent-probability-research/1.0'
ODDS_API = 'https://api.the-odds-api.com/v4'
FORECAST_API = 'https://api.open-meteo.com/v1/forecast'
PREVIOUS_API = 'https://previous-runs-api.open-meteo.com/v1/forecast'
ARCHIVE_API = 'https://archive-api.open-meteo.com/v1/archive'


def iso_now():
    return datetime.now(UTC).isoformat(timespec='seconds').replace('+00:00', 'Z')


def parse_utc(s):
    if not s or not isinstance(s, str):
        return None
    try:
        d = datetime.fromisoformat(s.replace('Z', '+00:00'))
        return d.replace(tzinfo=UTC) if d.tzinfo is None else d.astimezone(UTC)
    except ValueError:
        return None


def finite(v):
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except (ValueError, TypeError, OverflowError):
        return None


def web_json(url, params=None):
    target = url + ('?' + urlencode(params) if params else '')
    req = Request(target, headers={'User-Agent': USER_AGENT, 'Accept': 'application/json'})
    with urlopen(req, timeout=22) as resp:
        data = json.load(resp)
    if not isinstance(data, (list, dict)):
        raise ValueError('Unexpected response')
    return data


def save(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False, allow_nan=False) + '\n', encoding='utf-8')


def load(path, fallback=None):
    if not path.exists():
        return fallback
    return json.loads(path.read_text(encoding='utf-8'))


def fee_dollars(cents, qty=1, role='taker'):
    """General US Kalshi fee formula; MARKET-SPECIFIC FEES OVERRIDE THIS.

    Ceil at aggregate/order quantity to next $0.01. Fee may change.
    """
    rate = Decimal('0.07') if role == 'taker' else Decimal('0.0175')
    p = Decimal(int(cents)) / Decimal(100)
    raw = rate * Decimal(qty) * p * (1 - p)
    return float(raw.quantize(Decimal('0.01'), rounding=ROUND_CEILING))


def max_research_limit(p_low, cushion=0.02, role='taker'):
    """1-share fee-adjusted price ceiling; never an executable price."""
    if p_low is None or not (0 <= p_low <= 1):
        return None
    affordable = [c for c in range(1, 100) if c/100 + fee_dollars(c, role=role) + cushion < p_low]
    return max(affordable) if affordable else None


def quote_valuation(q, p_low, p_high, status='research_only'):
    out = {}
    for side, lo, hi in [('YES', p_low, p_high), ('NO', 1-p_high, 1-p_low)]:
        cents = None
        ask = finite(q.get(side.lower()+'_ask'))
        if ask is not None and 0 < ask < 1:
            cents = round(100*ask)
        fee = fee_dollars(cents) if cents is not None else None
        edge = lo - cents/100 - fee if cents is not None else None
        out[side.lower()] = {
            'p_lower': round(max(0, lo), 4), 'p_upper': round(min(1, hi), 4),
            'historical_ask_cents': cents,
            'general_taker_fee_one_share_dollars': fee,
            'edge_lower_at_historical_ask_after_general_taker_fee_dollars': round(edge, 4) if edge is not None else None,
            'research_price_ceiling_cents_with_2pp_cushion': max_research_limit(lo),
            'status': status,
        }
    return out


def normalized(s):
    s = unicodedata.normalize('NFKD', str(s)).encode('ascii', 'ignore').decode().lower()
    return re.sub(r'[^a-z0-9]+', ' ', s).strip()


# Strict city matching prevents accidental collisions of two LA / NY teams.
ALIASES = {
    'Los Angeles Lakers': 'Los Angeles L', 'Los Angeles Clippers': 'Los Angeles C',
    'New York Knicks': 'New York K', 'Brooklyn Nets': 'Brooklyn',
    'Golden State Warriors': 'Golden State', 'Portland Trail Blazers': 'Portland',
    'Oklahoma City Thunder': 'Oklahoma City', 'Utah Jazz': 'Utah',
    'New York Giants': 'New York G', 'New York Jets': 'New York J',
    'Los Angeles Rams': 'Los Angeles R', 'Los Angeles Chargers': 'Los Angeles C',
    'Washington Commanders': 'Washington', 'New England Patriots': 'New England',
    'Tampa Bay Buccaneers': 'Tampa Bay', 'Green Bay Packers': 'Green Bay',
    'San Francisco 49ers': 'San Francisco', 'Kansas City Chiefs': 'Kansas City',
    'Las Vegas Raiders': 'Las Vegas', 'New Orleans Saints': 'New Orleans',
    'Arizona Cardinals': 'Arizona', 'Carolina Panthers': 'Carolina',
}


def city_from_team(team):
    if team in ALIASES:
        return normalized(ALIASES[team])
    words = str(team).split()
    return normalized(' '.join(words[:-1])) if len(words) >= 2 else normalized(team)


def event_date_from_ticker(ticker):
    m = re.search(r'-(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{2})', ticker or '', re.I)
    if not m:
        return None
    try:
        return datetime.strptime('20'+m.group(1)+m.group(2).upper()+m.group(3), '%Y%b%d').date()
    except ValueError:
        return None


def parse_matchup(q):
    s = q.get('rules_primary') or ''
    # Use contract text instead of ticker-only abbreviations.
    m = re.search(r'\bthe\s+([A-Za-z .]+?)\s+vs\s+([A-Za-z .]+?)\s+(?:Pro Basketball|Pro Football|professional basketball|professional football) game', s, re.I)
    if not m:
        return None
    left = normalized(m.group(1))
    right = normalized(m.group(2))
    return (left, right) if left and right and left != right else None


def align_event(q, event):
    teams = parse_matchup(q)
    if not teams:
        return False
    a, b = city_from_team(event.get('away_team','')), city_from_team(event.get('home_team',''))
    if set(teams) != {a, b}:
        return False
    d = event_date_from_ticker(q.get('event_ticker',''))
    commence = parse_utc(event.get('commence_time'))
    return bool(d and commence and abs((commence.date()-d).days) <= 1 and commence > datetime.now(UTC))


def market_spec(q, event):
    series = q.get('series_ticker','')
    raw_title = q.get('title','')
    title = normalized(raw_title)
    if series in ('KXNBAGAME','KXNFLGAME'):
        m = re.search(r'^(.*?) wins$', title)
        if not m: return None
        team = m.group(1)
        for et in [event.get('home_team'), event.get('away_team')]:
            if city_from_team(et or '') == team:
                return ('h2h', et, None, 'named')
        return None
    if series in ('KXNBASPREAD','KXNFLSPREAD'):
        m = re.search(r'^(.*?) wins by over (\d+(?:\.\d+)?) points$', raw_title, re.I)
        if not m: return None
        city, pt = normalized(m.group(1)), float(m.group(2))
        for et in [event.get('home_team'), event.get('away_team')]:
            if city_from_team(et or '') == city:
                return ('spreads', et, -pt, 'named')
        return None
    # Mainline totals: if a different settlement phrasing occurs, fail closed.
    if series in ('KXNBATOTAL','KXNFLTOTAL'):
        m = re.search(r'^(?:over|total (?:points )?(?:over|above)) (\d+(?:\.\d+)?)', raw_title, re.I)
        if m: return ('totals','Over',float(m.group(1)),'total')
    return None


def matched_book_probabilities(event, spec, max_age_minutes, now=None):
    now = now or datetime.now(UTC)
    key, selection, target, kind = spec[:4]
    player = normalized(spec[4]) if len(spec)>4 else None
    estimates = []
    for book in event.get('bookmakers',[]) or []:
        update = parse_utc(book.get('last_update'))
        if not update or not (0 <= (now-update).total_seconds() <= max_age_minutes*60):
            continue
        for market in book.get('markets',[]) or []:
            if market.get('key') != key: continue
            market_update = parse_utc(market.get('last_update'))
            if market_update and (now-market_update).total_seconds() > max_age_minutes*60: continue
            outcomes = market.get('outcomes',[]) or []
            if kind == 'player':
                a = next((o for o in outcomes if o.get('name')=='Over' and normalized(o.get('description',''))==player and finite(o.get('point'))==target),None)
                b = next((o for o in outcomes if o.get('name')=='Under' and normalized(o.get('description',''))==player and finite(o.get('point'))==target),None)
            elif key == 'totals':
                a = next((o for o in outcomes if o.get('name')=='Over' and finite(o.get('point'))==target),None)
                b = next((o for o in outcomes if o.get('name')=='Under' and finite(o.get('point'))==target),None)
            elif key == 'spreads':
                a = next((o for o in outcomes if o.get('name')==selection and finite(o.get('point'))==target),None)
                other = event['away_team'] if event['home_team']==selection else event['home_team']
                b = next((o for o in outcomes if o.get('name')==other and finite(o.get('point')) == -target),None)
            else:
                a = next((o for o in outcomes if o.get('name')==selection),None)
                other = event['away_team'] if event['home_team']==selection else event['home_team']
                b = next((o for o in outcomes if o.get('name')==other),None)
            ap,bp=finite((a or {}).get('price')),finite((b or {}).get('price'))
            if ap is None or bp is None or min(ap,bp)<=1.001: continue
            impa,impb=1/ap,1/bp
            estimates.append({'bookmaker':book.get('title') or book.get('key'),
                'no_vig_probability':round(impa/(impa+impb),5),
                'observed_at_utc':(market_update or update).isoformat().replace('+00:00','Z'),
                'decimal_yes':ap,'decimal_no':bp})
    # Distinct books only. Never use multiple markets of one book as independent evidence.
    unique = {x['bookmaker']:x for x in estimates}
    return list(unique.values())


def consensus_evidence(rows, min_books=2):
    if len(rows) < min_books:return None
    p=[x['no_vig_probability'] for x in rows]
    mid=statistics.median(p)
    spread=max(p)-min(p)
    # Explicit uncertainty floor, not a proven statistical confidence interval.
    pad=max(0.03,spread/2+0.015)
    return {'fair_probability_mid':round(mid,4), 'p_lower':round(max(0,mid-pad),4),
        'p_upper':round(min(1,mid+pad),4), 'bookmaker_count':len(rows),
        'cross_book_range':[round(min(p),4),round(max(p),4)],
        'uncertainty_note':'Heuristic conservative range from bookmaker disagreement and minimum ±3pp; not statistically validated.'}


PLAYER_SERIES = {
    'KXNBAPTS':'player_points','KXNBAREB':'player_rebounds',
    'KXNBAAST':'player_assists','KXNBA3PT':'player_threes',
    'KXNFLPASSYDS':'player_pass_yds','KXNFLRSHYDS':'player_rush_yds',
    'KXNFLRECYDS':'player_reception_yds','KXNFLREC':'player_receptions',
    'KXNFLPASSTDS':'player_pass_tds'
}


def player_spec(q):
    series=q.get('series_ticker')
    key=PLAYER_SERIES.get(series)
    if not key:return None
    m=re.match(r'([^:]+):\s*(\d+)\+\s*(?:points|rebs?|rebounds|assists|threes|three-pointers|receptions|passing yards|rushing yards|receiving yards|passing touchdowns|tds?)',q.get('title',''),re.I)
    if not m:return None
    player,num=m.group(1).strip(),int(m.group(2))
    # Integer N+ means exactly >N-0.5 in an integer-valued statistic.
    return (key,'Over',float(num)-0.5,'player',player)


def sports_research(quotes, cfg, odds_key, evidence_rows, errors, data_path=None):
    max_age=cfg.get('max_bookmaker_age_minutes',45)
    event_feeds=[]
    cache_file = (data_path/'odds_budget.json') if data_path is not None else None
    budget=load(cache_file,{}) if cache_file else {}
    current_time=datetime.now(UTC)
    month=current_time.strftime('%Y-%m')
    today=current_time.date().isoformat()
    if budget.get('month')!=month: budget={'month':month,'requests':0,'approx_credits_used':0}
    if budget.get('day')!=today: budget['day']=today;budget['day_calls']=0
    max_calls=int(cfg.get('max_odds_api_requests_per_month',120))
    day_limit=int(cfg.get('max_odds_api_requests_per_day',4))
    last_batch=parse_utc(budget.get('last_batch_at_utc'))
    spacing=float(cfg.get('min_odds_refresh_hours',12))
    refresh_due=not last_batch or (current_time-last_batch).total_seconds()>=spacing*3600
    def can_call():return (refresh_due and budget['requests']<max_calls and budget['day_calls']<day_limit)
    def mark_call(charge=3):
        budget['requests']+=1
        budget['day_calls']+=1
        budget['last_batch_at_utc']=iso_now()
        budget['approx_credits_used']+=charge
        if cache_file:save(cache_file,budget)
    if odds_key:
        for sport, series in [('basketball_nba', 'KXNBA'),('americanfootball_nfl','KXNFL')]:
            if not any(str(q.get('series_ticker','')).startswith(series) for q in quotes):continue
            if not can_call():
                errors.append('Sports odds refresh limited by spacing or request budget; no stale odds promoted to current probability')
                break
            try:
                mark_call()
                data=web_json(f'{ODDS_API}/sports/{sport}/odds',{
                    'apiKey':odds_key,'regions':'us','markets':'h2h,spreads,totals','oddsFormat':'decimal'})
                if isinstance(data,list):event_feeds.extend(data)
                else: errors.append(f'{sport} odds: unexpected data type')
            except (HTTPError,URLError,TimeoutError,ValueError,OSError) as e:
                errors.append(f'{sport} odds unavailable: {type(e).__name__}: {str(e)[:140]}')
    else:
        errors.append('THE_ODDS_API_KEY not configured: automated sport odds disabled; only manually supplied evidence is eligible.')
    prop_feeds={}
    # Optional player-prop endpoint: paid plan/coverage may be necessary.
    if odds_key and cfg.get('enable_paid_player_props',False):
        per_sport=int(cfg.get('max_prop_events_per_sport',2))
        for sport, prefix in [('basketball_nba','KXNBA'),('americanfootball_nfl','KXNFL')]:
            candidates=[]
            for ev in event_feeds:
                if not ev.get('id'):continue
                eligible=any(str(q.get('series_ticker','')).startswith(prefix) and player_spec(q)
                             and align_event(q,ev) for q in quotes)
                if eligible:candidates.append(ev)
            for ev in candidates[:per_sport]:
                if not can_call():break
                market_keys=sorted({player_spec(q)[0] for q in quotes if player_spec(q) and
                                    str(q.get('series_ticker','')).startswith(prefix) and align_event(q,ev)})
                try:
                    mark_call(max(1,len(market_keys)))
                    got=web_json(f'{ODDS_API}/sports/{sport}/events/{ev["id"]}/odds',{
                        'apiKey':odds_key,'regions':'us','markets':','.join(market_keys),'oddsFormat':'decimal'})
                    if isinstance(got,dict):
                        # Event detail may omit team/title fields; retain identity.
                        for field in ('away_team','home_team','commence_time'):
                            got.setdefault(field,ev.get(field))
                        prop_feeds[ev['id']]=got
                except (HTTPError,URLError,TimeoutError,ValueError,OSError) as e:
                    errors.append(f'{sport} prop odds unavailable: {type(e).__name__}: {str(e)[:110]}')
    manual=defaultdict(list)
    for r in evidence_rows or []:
        if not isinstance(r,dict) or not r.get('settlement_matched') or not r.get('bookmaker') or not r.get('source_url'):continue
        stamp=parse_utc(r.get('observed_at_utc'))
        if not stamp or not (0<=(datetime.now(UTC)-stamp).total_seconds()<=max_age*60):continue
        a,b=finite(r.get('yes_decimal')),finite(r.get('no_decimal'))
        if a is None or b is None or min(a,b)<=1.001:continue
        p=(1/a)/(1/a+1/b)
        manual[str(r.get('ticker'))].append({'bookmaker':r['bookmaker'], 'no_vig_probability':round(p,5),
                         'observed_at_utc':r['observed_at_utc'], 'decimal_yes':a,'decimal_no':b,
                         'source_url':r['source_url']})
    result=[]
    for q in quotes:
        # Historical quote could reflect already-started games; only future matched events.
        matches=[]
        for ev in event_feeds:
            if align_event(q,ev):
                sp=market_spec(q,ev)
                if sp:
                    matches.extend(matched_book_probabilities(ev,sp,max_age))
        for ev in event_feeds:
            if ev.get('id') in prop_feeds and align_event(q,ev):
                sp=player_spec(q)
                if sp:matches.extend(matched_book_probabilities(prop_feeds[ev['id']],sp,max_age))
        matches.extend(manual.get(q.get('ticker'),[]))
        # Manual source must be matched to *exact* contract by human; do not mix if conflicting.
        unique={x['bookmaker']:x for x in matches}
        ev=consensus_evidence(list(unique.values()),cfg.get('min_independent_bookmakers',2))
        if not ev: continue
        p_lo,p_hi=ev['p_lower'],ev['p_upper']
        result.append({'ticker':q['ticker'],'title':q.get('title'), 'event_ticker':q.get('event_ticker'),
             'kalshi_observed_at_utc':q.get('observed_at_utc'),
             'model':'exact-outcome matched bookmaker no-vig consensus',
             'model_status':'research_only; recheck exact contract rules, game start and live books',
             **ev, 'sources':list(unique.values()), 'valuation':quote_valuation(q,p_lo,p_hi)})
    return result


def hourly_max_by_day(payload, key):
    hourly=payload.get('hourly',{})
    times=hourly.get('time') or []
    vals=hourly.get(key) or []
    buckets=defaultdict(list)
    for t,x in zip(times,vals):
        n=finite(x)
        if n is not None:
            buckets[str(t)[:10]].append(n)
    # Local-day forecast maxima from hourly prediction (not daily model output).
    return {d:max(v) for d,v in buckets.items() if len(v)>=18}


def calibration_from_docs(previous,archived, horizon):
    key=f'temperature_2m_previous_day{horizon}'
    predicted=hourly_max_by_day(previous,key)
    daily=archived.get('daily',{})
    actual={day:finite(v) for day,v in zip(daily.get('time',[]),daily.get('temperature_2m_max',[]))}
    errors=[actual[d]-m for d,m in predicted.items() if d in actual and actual[d] is not None]
    if len(errors)<35:raise ValueError(f'only {len(errors)} complete calibration days; need >=35')
    return {'n_days':len(errors),'forecast_horizon_days':horizon,
            'mean_error_f':round(statistics.mean(errors),3),
            'std_error_f':round(statistics.stdev(errors),3),
            'mae_f':round(statistics.mean(map(abs,errors)),3),
            'dataset':'Open-Meteo previous-run forecasts vs gridded historical reanalysis; NOT Kalshi Weather Company settlement observations'}


def ensure_calibration(loc, cfg, cache, errors, today):
    history_days=int(cfg.get('weather_calibration_days',100))
    cache.mkdir(parents=True,exist_ok=True)
    path=cache/(loc['code']+'.json')
    older=load(path,{})
    prior=parse_utc(older.get('generated_at_utc'))
    if prior and (datetime.now(UTC)-prior).days<7 and older.get('horizons'):
        return older['horizons']
    # API history can lag current date. Keep a seven-day buffer.
    end=today-timedelta(days=8)
    start=end-timedelta(days=history_days)
    params={'latitude':loc['lat'],'longitude':loc['lon'],'timezone':loc['timezone'],
            'start_date':start.isoformat(),'end_date':end.isoformat(),'temperature_unit':'fahrenheit'}
    try:
        previous=web_json(PREVIOUS_API,{**params,
             'hourly':'temperature_2m_previous_day1,temperature_2m_previous_day2,temperature_2m_previous_day3'})
        archived=web_json(ARCHIVE_API,{**params,'daily':'temperature_2m_max'})
        horizons={}
        for h in (1,2,3):
            try:horizons[str(h)]=calibration_from_docs(previous,archived,h)
            except (ValueError,KeyError,TypeError) as e:errors.append(f'{loc["code"]} h{h}: {e}')
        if horizons:
            save(path,{'generated_at_utc':iso_now(),'location':loc['code'],'horizons':horizons})
            return horizons
    except (HTTPError,URLError,TimeoutError,ValueError,OSError) as e:
        errors.append(f'weather calibration {loc["code"]}: {type(e).__name__}: {str(e)[:110]}')
    return older.get('horizons',{})  # old calibrated source still documented as stale


def parse_weather_condition(q):
    s=(q.get('rules_primary') or '')
    if 'maximum temperature' not in s.lower():return None
    m=re.search(r'between\s+(-?\d+)\s*[-–]\s*(-?\d+)',s,re.I)
    if m:
        a,b=map(int,m.groups())
        if b-a!=1:return None
        # Degree readouts treated as rounded integers; source-dependent rounding unverified.
        return ('bin',a-0.5,b+0.5)
    m=re.search(r'(greater than|above)\s+(-?\d+)',s,re.I)
    if m:return ('above',int(m.group(2))+0.5,None)
    m=re.search(r'(less than|below)\s+(-?\d+)',s,re.I)
    if m:return ('below',None,int(m.group(2))-0.5)
    return None


def normal_cdf(x,mu,sigma):
    return 0.5*(1+math.erf((x-mu)/(math.sqrt(2)*sigma)))


def condition_prob(spec,mu,sigma):
    kind,lo,hi=spec
    if kind=='bin':return max(0,normal_cdf(hi,mu,sigma)-normal_cdf(lo,mu,sigma))
    if kind=='above':return 1-normal_cdf(lo,mu,sigma)
    return normal_cdf(hi,mu,sigma)


def weather_probability(spec,forecast_f,cal):
    # Empirical *gridded* error distribution, with extra conservatism for provider mismatch.
    bias=cal['mean_error_f']
    sigma=max(2.5,cal['std_error_f']*1.15)
    mu=forecast_f+bias
    middle=condition_prob(spec,mu,sigma)
    variants=[condition_prob(spec,mu+shift,sigma*scale)
              for shift in (-1.5,0,1.5) for scale in (0.8,1,1.25)]
    return {'p_mid':round(middle,4),'p_lower':round(max(0,min(variants)),4),
            'p_upper':round(min(1,max(variants)),4), 'mean_f':round(mu,2),
            'sd_f':round(sigma,2),'confidence':'proxy_only_unvalidated_station',
            'uncertainty_note':'Heuristic sensitivity range (forecast mean ±1.5°F, dispersion 0.8–1.25x); not an empirical confidence interval or Kalshi-station-validated model.'}


def weather_research(quotes, cfg, errors, output_dir):
    forecast_by_loc={}
    calibrations={}
    today=datetime.now(UTC).date()
    for loc in cfg.get('weather_locations',[]):
        try:
            raw=web_json(FORECAST_API,{'latitude':loc['lat'],'longitude':loc['lon'],
              'daily':'temperature_2m_max','timezone':loc['timezone'],
              'temperature_unit':'fahrenheit','forecast_days':5})
            daily=raw.get('daily',{})
            forecast_by_loc[loc['code']]={str(d):finite(v) for d,v in zip(
                daily.get('time',[]),daily.get('temperature_2m_max',[]))}
        except (HTTPError,URLError,TimeoutError,ValueError,OSError) as e:
            errors.append(f'weather forecast {loc["code"]}: {type(e).__name__}: {str(e)[:120]}')
            continue
        local_today=datetime.now(ZoneInfo(loc['timezone'])).date()
        calibrations[loc['code']]=ensure_calibration(loc,cfg,output_dir/'calibration',errors,local_today)
    results=[]
    for q in quotes:
        condition=parse_weather_condition(q)
        if not condition:continue
        rules=q.get('rules_primary') or ''
        loc=next((l for l in cfg.get('weather_locations',[]) if re.search(r'\b'+re.escape(l['settlement_code'])+r'\b',rules,re.I)),None)
        if not loc:continue
        d=event_date_from_ticker(q.get('event_ticker',''))
        local_today=datetime.now(ZoneInfo(loc['timezone'])).date()
        if not d or d<=local_today:continue  # never value a past or same-date partial-day high
        days=(d-local_today).days
        cal=calibrations.get(loc['code'],{}).get(str(days))
        point=forecast_by_loc.get(loc['code'],{}).get(d.isoformat())
        if not cal or point is None:continue
        p=weather_probability(condition,point,cal)
        results.append({'ticker':q['ticker'],'title':q.get('title'),'event_date':d.isoformat(),
            'settlement_code':loc['settlement_code'],'location':loc['name'],
            'kalshi_observed_at_utc':q.get('observed_at_utc'),
            'forecast_provider':'Open-Meteo gridded forecast; NOT Kalshi settlement source',
            'forecast_source_url':'https://open-meteo.com/en/docs',
            'calibration':cal,'weather_condition':condition,
            'forecast_max_f':round(point,2),**p,
            'model_status':'RESEARCH ONLY: Weather Company final station reading not independently validated; never automate as confirmed +EV',
            'valuation':quote_valuation(q,p['p_lower'],p['p_upper'])})
    return results


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--config',default='probability_config.json')
    ap.add_argument('--data-root',default='data')
    ap.add_argument('--sports-evidence',default='sports_benchmarks.json')
    args=ap.parse_args()
    cfg=load(Path(args.config),{})
    root=Path(args.data_root)
    dest=root/'probabilities'
    sports=load(root/'feeds/sports/latest.json',{})
    weather=load(root/'feeds/weather_politics/latest.json',{})
    if not sports.get('quotes') and not weather.get('quotes'):
        raise RuntimeError('Both input feeds missing: refusing to overwrite valid previous outputs')
    key=os.environ.get('THE_ODDS_API_KEY','').strip()
    sports_errors=[]
    weather_errors=[]
    if sports.get('quotes'):
        s=sports_research(sports['quotes'],cfg,key,load(Path(args.sports_evidence),[]),sports_errors,dest)
        sport_obj={'schema_version':1,'generated_at_utc':iso_now(),'input_feed_generated_at_utc':sports.get('generated_at_utc'),
          'source_type':'independent bookmaker odds; not Kalshi price','research_only':True,
          'warnings':['Exact market settlement and bookmaker lines must match; single-provider consensus is not a validated predictive model.',
                      'Fee calculation uses general one-share taker approximation; actual Kalshi fee quote may vary.'],
          'estimated_contracts':len(s),'evaluations':s,'errors':sports_errors}
        save(dest/'sports/latest.json',sport_obj)
    if weather.get('quotes'):
        w=weather_research(weather['quotes'],cfg,weather_errors,dest)
        weather_obj={'schema_version':1,'generated_at_utc':iso_now(),'input_feed_generated_at_utc':weather.get('generated_at_utc'),
          'source_type':'Open-Meteo forecast + 1-3 day historical gridded error calibration', 'research_only':True,
          'warnings':['Open-Meteo model/reanalysis grid is not Weather Company CLIMxxx settlement station data.',
                     'Normal error assumption, rounding and uncertainty range are provisional and not verified against Kalshi settlement.'],
          'estimated_contracts':len(w),'evaluations':w,'errors':weather_errors}
        save(dest/'weather/latest.json',weather_obj)
    print(f"Sports: {len(s) if sports.get('quotes') else 0} model estimates; Weather: {len(w) if weather.get('quotes') else 0} model estimates")
    for title,errs in [('Sports',sports_errors),('Weather',weather_errors)]:
        for err in errs[:8]:print(f'{title} warning: {err}')


if __name__=='__main__':main()
