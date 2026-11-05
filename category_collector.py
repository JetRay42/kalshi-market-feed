#!/usr/bin/env python3
"""Generate three independently sampled, read-only Kalshi feeds.

Requires the existing collector.py from JetRay42/kalshi-market-feed.
No account credentials and no trading or order placement.
"""
from __future__ import annotations
import argparse
import json
import re
import time
import math
from datetime import datetime, timezone
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import quote

import collector as core

PROFILE_NAMES = ("sports", "pop_culture", "weather_politics")
VALID_TICKER = re.compile(r"[A-Z0-9_-]{2,100}\Z")


def cleaned_tickers(values):
    return list(dict.fromkeys(str(x).strip().upper() for x in values
                              if isinstance(x, str) and VALID_TICKER.fullmatch(x.strip().upper())))


def choose_rotating(items, count, window, salt=0):
    """Rotate deterministically to avoid repeating the same small slice forever."""
    if not items or count <= 0:
        return []
    offset = ((window + salt) * count) % len(items)
    rotated = items[offset:] + items[:offset]
    return rotated[:count]


def discover(category, keywords, deny_terms, count, window, errors):
    """Discover real series, favour recurring topical series over alphabetic order.

    Title/tag and declared frequency are discovery heuristics, NOT evidence of EV.
    A larger probe pool is intentionally returned; only a bounded number is queried.
    """
    try:
        payload = core.get('/series', {'category': category})
        rows = payload.get('series') or []
        if not isinstance(rows, list):
            raise ValueError('series response is not a list')
    except Exception as exc:
        errors.append(f'{category}: discovery unavailable: {exc}')
        return []
    needles = [str(k).lower() for k in keywords if k]
    denies = [str(k).lower() for k in deny_terms if k]
    candidates = []
    for obj in rows:
        if not isinstance(obj, dict):
            continue
        ticker = str(obj.get('ticker') or '').upper()
        if not VALID_TICKER.fullmatch(ticker):
            continue
        title = str(obj.get('title') or '')
        tags = ' '.join(map(str, obj.get('tags') or []))
        descriptor = (title + ' ' + tags + ' ' + ticker).lower()
        if needles and not any(k in descriptor for k in needles):
            continue
        if denies and any(k in descriptor for k in denies):
            continue
        hits = sum(1 for k in needles if k in descriptor)
        # Recurring measurement is typically easier to independently check than
        # hypothetical multi-year celebrity/awards markets. Not a hard exclusion.
        cadence = str(obj.get('frequency') or '').lower()
        cadence_bonus = {'daily': 4, 'weekly': 4, 'monthly': 2}.get(cadence, 0)
        specificity = min(3, max((len(k.split()) for k in needles if k in descriptor), default=0))
        relevance = cadence_bonus + min(hits, 4) + specificity
        candidates.append((ticker, relevance))
    # Rotate among equal-quality series across runs, not just A-Z winners.
    grouped = defaultdict(list)
    for ticker, relevance in sorted(set(candidates)):
        grouped[relevance].append(ticker)
    ordered = []
    for score in sorted(grouped, reverse=True):
        group = grouped[score]
        offset = (window + len(category)) % len(group)
        ordered.extend(group[offset:] + group[:offset])
    return ordered[:max(0, count)]


_EVENT_DATE = re.compile(r'-(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{2})', re.I)


def event_days(market, now):
    """Best-effort timing HINT, not verified event start or resolution time."""
    identifier = str(market.get('event_ticker') or market.get('ticker') or '')
    match = _EVENT_DATE.search(identifier)
    if match:
        try:
            dt = datetime.strptime('20' + match.group(1) + match.group(2).upper() + match.group(3), '%Y%b%d').replace(tzinfo=timezone.utc)
            return (dt.timestamp() - now) / 86400
        except ValueError:
            pass
    try:
        ts = core.parse_time(market.get('close_time'))
        return (ts - now) / 86400 if math.isfinite(ts) else 999
    except (AttributeError, TypeError, ValueError):
        return 999


def horizon_bucket(days):
    """Prioritize events close enough for measurable information, not only long-dated ideas."""
    if -1.5 <= days <= 2: return 0
    if -1.5 <= days <= 10: return 1
    if -1.5 <= days <= 30: return 2
    if -1.5 <= days <= 90: return 3
    return 4


def positive_float(value):
    try:
        return max(0.0, float(value)) if value is not None else 0.0
    except (ValueError, TypeError, OverflowError):
        return 0.0


def fetch_open_markets(kind, ticker, max_pages, errors, now):
    records, cursor = [], None
    for _ in range(max_pages):
        params = {kind: ticker, 'status': 'open', 'limit': 200}
        if cursor:
            params['cursor'] = cursor
        try:
            doc = core.get('/markets', params)
        except Exception as exc:
            errors.append(f'{kind} {ticker}: {exc}')
            break
        for market in doc.get('markets', []) or []:
            if isinstance(market, dict) and core.market_active(market, now):
                records.append(market)
        new_cursor = doc.get('cursor') or None
        if not new_cursor or new_cursor == cursor:
            break
        cursor = new_cursor
    return records


def rotate_events(markets, take, window):
    """Diversify events and strikes, prioritizing relevant near-term market groups.

    Market closing times can be administrative long-stops; inferred dates from
    event tickers are a screening hint, never a verified event start.
    """
    grouped = defaultdict(list)
    for market in markets:
        event = str(market.get('event_ticker') or market.get('ticker') or '')
        if event:
            grouped[event].append(market)
    if not grouped or take <= 0:
        return []
    now = time.time()
    by_horizon = defaultdict(list)
    for event, group in grouped.items():
        first_days = min(event_days(m, now) for m in group)
        bucket = horizon_bucket(first_days)
        # Prefer families with real activity within the same timing bracket.
        vol = max((positive_float(m.get('volume_24h_fp')) for m in group), default=0)
        by_horizon[bucket].append((event, vol))
        group.sort(key=lambda m: core.rank(m, now))
        offset = (window + len(event)) % len(group)
        grouped[event] = group[offset:] + group[:offset]
    keys = []
    for bucket in sorted(by_horizon):
        ordered = [event for event, _ in sorted(by_horizon[bucket], key=lambda t: (-t[1],t[0]))]
        # Only cycle through plausible alternatives within each horizon band.
        offset = (window * max(1, min(take, 7))) % len(ordered)
        keys.extend(ordered[offset:] + ordered[:offset])
    picked = []
    while len(picked) < take:
        advanced = False
        for event in keys:
            if grouped[event]:
                picked.append(grouped[event].pop(0))
                advanced = True
                if len(picked) >= take:
                    break
        if not advanced:
            break
    return picked


def research_quality(row, now):
    """A research-attention score. NEVER a win probability or expected value."""
    yes_bid, yes_ask = row.get('yes_bid'), row.get('yes_ask')
    no_bid, no_ask = row.get('no_bid'), row.get('no_ask')
    quotes = []
    for bid, ask, size in ((yes_bid, yes_ask, row.get('yes_ask_size')),
                           (no_bid, no_ask, row.get('no_ask_size'))):
        if bid is not None and ask is not None:
            spread = positive_float(ask) - positive_float(bid)
            if -0.0001 <= spread <= 1:
                quotes.append((spread, positive_float(size)))
    if not quotes:
        return None
    spread, depth = min(quotes, key=lambda q: (q[0], -q[1]))
    if depth <= 0:
        return None
    score = 0
    if spread <= .02: score += 5
    elif spread <= .05: score += 3
    elif spread <= .10: score += 1
    if depth >= 20: score += 4
    elif depth >= 5: score += 3
    elif depth >= 1: score += 1
    vol24 = positive_float(row.get('volume_24h_fp'))
    if vol24 >= 1000: score += 4
    elif vol24 >= 100: score += 3
    elif vol24 >= 1: score += 2
    days = event_days(row, now)
    score += {0:5, 1:4, 2:2, 3:1}.get(horizon_bucket(days),0)
    return dict(research_score=score, min_quoted_spread_cents=round(100*spread,1),
                best_quoted_ask_size=depth, timing_hint_days=round(days,1) if abs(days)<900 else None)


def research_queue(feed, count=28):
    """Diversified candidates for downstream research, NOT price or side recommendations."""
    now = time.time()
    families = defaultdict(list)
    for row in feed.get('quotes', []):
        signals = research_quality(row, now)
        if signals is not None:
            candidate = dict(row)
            candidate.update(signals)
            families[str(row.get('series_ticker') or 'UNKNOWN')].append(candidate)
    for group in families.values():
        group.sort(key=lambda v: (-v['research_score'], -positive_float(v.get('volume_24h_fp')), v['ticker']))
    shortlist = []
    event_seen = Counter()
    family_count = Counter()
    # Round-robin across families: don't let 16 thresholds from a single
    # Rotten Tomatoes film crowd out 10 different music/chart research ideas.
    ordered = sorted(families, key=lambda k: (-families[k][0]['research_score'],k))
    for round_no in range(10):
        for family in ordered:
            for candidate in families[family]:
                if candidate.get('_picked'): continue
                event = str(candidate.get('event_ticker') or candidate['ticker'])
                if event_seen[event] >= 2: continue
                if family_count[family] >= 5: continue
                candidate['_picked'] = True
                event_seen[event] += 1
                family_count[family] += 1
                shortlist.append(candidate)
                break
            if len(shortlist) >= count:
                break
        if len(shortlist) >= count: break
    for item in shortlist:
        item.pop('_picked', None)
    return dict(schema_version=1, profile=feed['profile'],
                generated_at_utc=feed['generated_at_utc'],
                warning='RESEARCH SHORTLIST ONLY. Scores are NOT probabilities, EV estimates, or trade recommendations. Quotes are historical; verify current books, contract rules, sources and fees before any order.',
                ranking_factors='Displayed spread/depth, volume and inferred timing, with event/series diversification. Never use ranking to set a trade price.',
                total_sampled=feed['successful_orderbooks'], candidates=shortlist)


def collect_one(name, profile, shared, root):
    started = core.stamp()
    now = time.time()
    errors, selected, source = [], {}, {}
    cap = max(25, min(int(profile.get('max_orderbooks', 150)), 350))
    per_series = max(1, min(int(profile.get('per_series', 12)), 60))
    dynamic_per_series = max(1, min(int(profile.get('dynamic_per_series', per_series)), 60))
    overrides = profile.get('per_series_overrides', {})
    window = int(now // (30 * 60))  # Change sample twice hourly (more frequent than scan cadence).
    pages = max(1, min(int(profile.get('max_pages', 3)), 5))
    series = cleaned_tickers(profile.get('base_series', []))
    watch = profile.get('priority', {})
    if name == 'sports':
        # Preserve original researched tickers without editing the user's old watchlist.
        old_path = root / 'watchlist.json'
        if old_path.exists():
            old = core.load_watchlist(old_path)
            watch = {
                key: cleaned_tickers(watch.get(key, []) + old.get(key, []))
                for key in ('exact_tickers', 'event_tickers', 'series_tickers')
            }
    series += [s for s in cleaned_tickers(watch.get('series_tickers', [])) if s not in series]

    def include(m, reason):
        ticker = str(m.get('ticker') or '')
        if ticker and core.market_active(m, now) and ticker not in selected and len(selected) < cap:
            selected[ticker] = m
            source[ticker] = reason

    for ticker in cleaned_tickers(watch.get('exact_tickers', [])):
        try:
            include((core.get('/markets/' + quote(ticker, safe='')).get('market') or {}), 'priority_exact')
        except Exception as exc:
            errors.append(f'Exact market {ticker}: {exc}')
    for event in cleaned_tickers(watch.get('event_tickers', [])):
        for m in rotate_events(fetch_open_markets('event_ticker', event, pages, errors, now),
                               min(per_series * 2, 24), window):
            include(m, 'priority_event')

    # Static base series are known recurring contracts. Dynamic series are discovered
    # by API category/title/tag; avoid assuming tickers or relying on old watchlists.
    dynamic, dynamic_groups = [], []
    for item in profile.get('discovery', []):
        dynamic_groups.append(discover(
            str(item['category']), item.get('keywords', []),
            item.get('exclude', []), max(0, int(item.get('max_series', 8))) * max(1, min(int(profile.get('discovery_probe_multiplier', 2)), 4)),
            window, errors))
    # Interleave categories so weather discovery cannot crowd out politics.
    for i in range(max((len(group) for group in dynamic_groups), default=0)):
        for group in dynamic_groups:
            if i < len(group) and group[i] not in series and group[i] not in dynamic:
                dynamic.append(group[i])
    dynamic = dynamic[:max(0, min(int(profile.get('max_dynamic_series_attempts', 28)), 45))]
    # Always reserve slots for the known core sports/other families, before
    # spending the remaining budget on discovered niche series.
    combined = [(s, 'base') for s in series] + [(s, 'dynamic') for s in dynamic]
    considered, market_counts = [], {}
    for s, tier in combined:
        if len(selected) >= cap:
            break
        markets = fetch_open_markets('series_ticker', s, pages, errors, now)
        considered.append(s)
        market_counts[s] = len(markets)
        allocation = (max(1, min(int(overrides.get(s, per_series)), 60))
                      if tier == 'base' else dynamic_per_series)
        for m in rotate_events(markets, allocation, window):
            include(m, 'series:' + s)

    def book_for(pair):
        ticker, m = pair
        try:
            book = core.get('/markets/' + quote(ticker, safe='') + '/orderbook')
            return dict(
                ticker=ticker, event_ticker=m.get('event_ticker'),
                series_ticker=m.get('series_ticker') or ticker.split('-')[0],
                title=m.get('title') or '', yes_sub_title=m.get('yes_sub_title'),
                status=m.get('status'), close_time=m.get('close_time'),
                volume_fp=m.get('volume_fp'), volume_24h_fp=m.get('volume_24h_fp'),
                rules_primary=m.get('rules_primary'), rules_secondary=m.get('rules_secondary'),
                selection_reason=source[ticker], observed_at_utc=core.stamp(),
                **core.best_quotes(book)), None
        except Exception as exc:
            return None, f'Book {ticker}: {exc}'

    rows = []
    with ThreadPoolExecutor(max_workers=max(1, min(int(shared.get('parallel_books', 4)), 6))) as pool:
        futures = [pool.submit(book_for, pair) for pair in selected.items()]
        for future in as_completed(futures):
            row, err = future.result()
            if row:
                rows.append(row)
            if err:
                errors.append(err)

    if not rows:
        raise RuntimeError(f'{name}: no market order books fetched; preserving old feed; ' + '; '.join(errors[:4]))
    rows.sort(key=lambda r: (0 if r['selection_reason'].startswith('priority_') else 1,
                             r['series_ticker'], r['ticker']))
    return dict(
        schema_version=3, profile=name, generated_at_utc=core.stamp(),
        started_at_utc=started, source='Kalshi public Trade API',
        warning='Research snapshots ONLY; verify live prices, contract terms and fees before any order.',
        selected_market_count=len(selected), successful_orderbooks=len(rows),
        series_considered=considered, series_market_counts=market_counts,
        discovery_attempted=sum(1 for x in considered if x in dynamic),
        coverage_by_series=dict(Counter(q['series_ticker'] for q in rows)),
        quotes=rows, errors=errors[:100])


def write_feed(feed, folder):
    folder.mkdir(parents=True, exist_ok=True)
    # Write after all successful collection: do not overwrite with an empty report.
    (folder / 'latest.json').write_text(json.dumps(feed, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    (folder / 'latest.md').write_text(core.report(feed), encoding='utf-8')
    queue = research_queue(feed)
    (folder / 'research_queue.json').write_text(json.dumps(queue, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    lines = [f'# {feed["profile"].replace("_", " ").title()} — research shortlist', '',
             f'Feed generated (UTC): {feed["generated_at_utc"]}', '',
             '**Screening priority only — NOT a trade recommendation or estimate of expected value.**', '',
             '| Priority | Kalshi ticker | Research score | Bid/ask (YES, ¢) | Bid/ask (NO, ¢) |',
             '|---:|---|---:|---|---|']
    for rank, row in enumerate(queue['candidates'], 1):
        fmt = lambda value: f'{100*positive_float(value):.0f}' if value is not None else '—'
        lines.append(f'| {rank} | `{row["ticker"]}` | {row["research_score"]} | '
                     f'{fmt(row.get("yes_bid"))} / {fmt(row.get("yes_ask"))} | '
                     f'{fmt(row.get("no_bid"))} / {fmt(row.get("no_ask"))} |')
    (folder / 'research_queue.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--profiles', default='category_profiles.json')
    p.add_argument('--output', default='data/feeds')
    p.add_argument('--only', choices=PROFILE_NAMES)
    args = p.parse_args()
    configs = json.loads(Path(args.profiles).read_text(encoding='utf-8'))
    root = Path(args.profiles).resolve().parent
    profiles = configs['profiles']
    failures = []
    for name in (args.only,) if args.only else PROFILE_NAMES:
        try:
            result = collect_one(name, profiles[name], configs.get('shared', {}), root)
            write_feed(result, Path(args.output) / name)
            print(f'{name}: {result["successful_orderbooks"]} books, '
                  f'{len(result["series_considered"])} series considered, '
                  f'{len(result["errors"])} warnings', flush=True)
        except Exception as exc:
            failures.append(f'{name}: {exc}')
            print(f'FAILED {name}: {exc}', flush=True)
    if failures:
        raise SystemExit('Some category feeds failed; last good files preserved. ' + '; '.join(failures))


if __name__ == '__main__':
    main()
