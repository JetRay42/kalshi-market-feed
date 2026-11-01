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
    """Look up real series identifiers; never invent dynamic Kalshi tickers."""
    try:
        payload = core.get('/series', {'category': category})
        rows = payload.get('series') or []
        if not isinstance(rows, list):
            raise ValueError('series response is not a list')
    except Exception as exc:
        errors.append(f'{category}: discovery unavailable: {exc}')
        return []
    pattern = re.compile('|'.join(re.escape(k) for k in keywords), re.I) if keywords else None
    reject = re.compile('|'.join(re.escape(k) for k in deny_terms), re.I) if deny_terms else None
    candidates = []
    for s in rows:
        if not isinstance(s, dict):
            continue
        ticker = str(s.get('ticker') or '').upper()
        descriptor = ' '.join([str(s.get('title') or ''),
                               ' '.join(map(str, s.get('tags') or [])), ticker])
        if not VALID_TICKER.fullmatch(ticker):
            continue
        if pattern and not pattern.search(descriptor):
            continue
        if reject and reject.search(descriptor):
            continue
        candidates.append(ticker)
    return choose_rotating(sorted(set(candidates)), count, window, len(category))


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
    """Sample across events *and* thresholds; don't use just the first few strikes.

    For each event, rotate which strike is picked first. Visit each event once
    before drawing additional strikes. Rotate event order between runs.
    """
    grouped = defaultdict(list)
    for m in markets:
        group = m.get('event_ticker') or m['ticker']
        grouped[str(group)].append(m)
    if not grouped:
        return []
    keys = sorted(grouped)
    event_offset = (window * max(1, take)) % len(keys)
    keys = keys[event_offset:] + keys[:event_offset]
    for k in keys:
        group = sorted(grouped[k], key=lambda m: core.rank(m, time.time()))
        offset = window % len(group)
        grouped[k] = group[offset:] + group[:offset]
    picked = []
    while len(picked) < take:
        advanced = False
        for k in keys:
            if grouped[k]:
                picked.append(grouped[k].pop(0))
                advanced = True
                if len(picked) >= take:
                    break
        if not advanced:
            break
    return picked


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
            item.get('exclude', []), max(0, int(item.get('max_series', 8))),
            window, errors))
    # Interleave categories so weather discovery cannot crowd out politics.
    for i in range(max((len(group) for group in dynamic_groups), default=0)):
        for group in dynamic_groups:
            if i < len(group) and group[i] not in series and group[i] not in dynamic:
                dynamic.append(group[i])
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
        coverage_by_series=dict(Counter(q['series_ticker'] for q in rows)),
        quotes=rows, errors=errors[:100])


def write_feed(feed, folder):
    folder.mkdir(parents=True, exist_ok=True)
    # Write after all successful collection: do not overwrite with an empty report.
    (folder / 'latest.json').write_text(json.dumps(feed, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    (folder / 'latest.md').write_text(core.report(feed), encoding='utf-8')


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
