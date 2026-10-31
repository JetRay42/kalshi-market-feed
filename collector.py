#!/usr/bin/env python3
"""Read-only Kalshi market sampler. No account authentication or trading code."""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

API_BASES = (
    'https://external-api.kalshi.com/trade-api/v2',
    'https://api.elections.kalshi.com/trade-api/v2',
)
USER_AGENT = 'public-kalshi-research-sampler/2.0'


def stamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')


def parse_time(ts: object) -> float:
    if not ts or not isinstance(ts, str):
        return float('inf')
    try:
        return datetime.fromisoformat(ts.replace('Z', '+00:00')).timestamp()
    except ValueError:
        return float('inf')


def dec(x: object) -> Decimal | None:
    try:
        return Decimal(str(x)) if x is not None and x != '' else None
    except (ValueError, TypeError, InvalidOperation):
        return None


def money(v: Decimal | None) -> str | None:
    return f'{v:.4f}' if v is not None else None


def best_bid(levels: object) -> tuple[Decimal, Decimal] | None:
    """Books list only YES and NO bids; don't presume ordering of price levels."""
    if not isinstance(levels, list):
        return None
    valid = []
    for level in levels:
        if not isinstance(level, (list, tuple)) or len(level) < 2:
            continue
        p, q = dec(level[0]), dec(level[1])
        if p is not None and q is not None and 0 <= p <= 1 and q > 0:
            valid.append((p, q))
    return max(valid, key=lambda x: x[0]) if valid else None


def best_quotes(payload: dict) -> dict:
    book = payload.get('orderbook_fp') or {}
    y, n = best_bid(book.get('yes_dollars')), best_bid(book.get('no_dollars'))
    return {
        'yes_bid': money(y[0]) if y else None,
        'yes_ask': money(1 - n[0]) if n else None,
        'yes_ask_size': str(n[1]) if n else None,
        'no_bid': money(n[0]) if n else None,
        'no_ask': money(1 - y[0]) if y else None,
        'no_ask_size': str(y[1]) if y else None,
        'yes_bid_size': str(y[1]) if y else None,
        'no_bid_size': str(n[1]) if n else None,
    }


def get(path: str, params: dict | None = None) -> dict:
    suffix = path + ('?' + urlencode(params) if params else '')
    issues = []
    for base in API_BASES:
        for attempt in range(2):
            try:
                req = Request(base + suffix, headers={'User-Agent': USER_AGENT, 'Accept': 'application/json'})
                with urlopen(req, timeout=16) as response:
                    doc = json.load(response)
                if not isinstance(doc, dict):
                    raise ValueError('API returned a non-object payload')
                return doc
            except HTTPError as exc:
                issues.append(f'HTTP {exc.code} on {path}')
                if exc.code in (400, 404):
                    break
                if attempt == 0:
                    time.sleep(0.4)
            except (URLError, TimeoutError, ValueError, OSError) as exc:
                issues.append(f'{type(exc).__name__} on {path}')
                if attempt == 0:
                    time.sleep(0.4)
    raise RuntimeError('; '.join(issues[-3:]))


def market_active(m: dict, now: float) -> bool:
    return bool(m.get('ticker') and m.get('market_type', 'binary') == 'binary'
                and m.get('status', 'open') == 'open'
                and parse_time(m.get('close_time')) > now)


def rank(m: dict, now: float) -> tuple:
    """Prioritize future near-term catalysts and 24h market activity, not old events."""
    close = parse_time(m.get('close_time'))
    if close == float('inf'):
        window = 4
    elif close <= now + 3 * 86400:
        window = 0
    elif close <= now + 10 * 86400:
        window = 1
    elif close <= now + 30 * 86400:
        window = 2
    else:
        window = 3
    vol24 = dec(m.get('volume_24h_fp') or 0) or Decimal('0')
    volume = dec(m.get('volume_fp') or 0) or Decimal('0')
    return (window, -vol24, -volume, close, m['ticker'])


def load_watchlist(path: Path) -> dict:
    if not path.exists():
        return {'exact_tickers': [], 'event_tickers': [], 'series_tickers': []}
    data = json.loads(path.read_text(encoding='utf-8'))
    return {k: [str(x).strip().upper() for x in data.get(k, []) if isinstance(x, str) and re.fullmatch(r'[A-Z0-9_-]{2,100}', x.strip().upper())]
            for k in ('exact_tickers', 'event_tickers', 'series_tickers')}


def discover_series(config: dict, errors: list[str]) -> list[str]:
    """Find live sports prop/spread/total families by descriptions, not guessed tickers."""
    if not config.get('auto_discover_sports_series', True):
        return []
    try:
        data = get('/series', {'category': 'Sports'})
    except RuntimeError as exc:
        errors.append('Sports series discovery unavailable: ' + str(exc))
        return []
    patterns = config.get('sports_series_keywords', [])
    regex = re.compile('|'.join(re.escape(x) for x in patterns), re.I) if patterns else None
    found = []
    for s in data.get('series', []):
        ticker = str(s.get('ticker') or '').upper()
        descr = ' '.join([str(s.get('title') or ''), ' '.join(map(str, s.get('tags') or [])), ticker])
        if ticker and regex and regex.search(descr):
            found.append((ticker, str(s.get('title') or '')))
    # Rotate through discovered families over successive runs for broader coverage.
    # Always keep deterministic ordering inside a particular 10-minute interval.
    found.sort()
    if not found:
        errors.append('No sports series matched keyword discovery; using configured series only')
        return []
    max_series = max(1, min(int(config.get('max_dynamic_series', 18)), 50))
    window = int(time.time() // 600)
    offset = (window * max_series) % len(found)
    ordered = found[offset:] + found[:offset]
    return [ticker for ticker, _ in ordered[:max_series]]


def paged_markets(kind: str, ticker: str, cfg: dict, now: float, errors: list[str]) -> list[dict]:
    """Fetch up to N pages, so newer props aren't stranded past the first page."""
    data = []
    cursor = None
    max_pages = max(1, min(int(cfg.get('max_pages_per_series', 2)), 4))
    for _ in range(max_pages):
        args = {kind: ticker, 'status': 'open', 'limit': 200}
        if cursor:
            args['cursor'] = cursor
        try:
            response = get('/markets', args)
        except RuntimeError as exc:
            errors.append(f'{kind} {ticker}: {exc}')
            break
        data.extend(m for m in response.get('markets', []) if market_active(m, now))
        cursor = response.get('cursor')
        if not cursor:
            break
    return sorted(data, key=lambda m: rank(m, now))


def collect(cfg: dict, watch: dict) -> dict:
    now = time.time()
    started = stamp()
    errors = []
    selected: dict[str, dict] = {}
    sources = {}
    scanned = Counter()
    cap = max(25, min(int(cfg.get('max_orderbooks', 200)), 400))

    def choose(m: dict, why: str):
        t = str(m.get('ticker') or '')
        if t and market_active(m, now) and t not in selected:
            selected[t] = m
            sources[t] = why
            scanned[why] += 1

    # Exact researched contracts first; the user can edit watchlist.json without changing code.
    for ticker in watch.get('exact_tickers', []):
        try:
            choose(get('/markets/' + quote(ticker, safe='')).get('market') or {}, 'watchlist_exact')
        except RuntimeError as exc:
            errors.append(f'Exact {ticker}: {exc}')

    # Prioritized entire event: e.g. a named NFL game plus its binary markets.
    for event in watch.get('event_tickers', []):
        for m in paged_markets('event_ticker', event, cfg, now, errors):
            choose(m, 'watchlist_event')
            if len(selected) >= cap:
                break

    # Static well-known categories plus user-specified families.
    base_series = list(dict.fromkeys(list(cfg.get('base_series', [])) + watch.get('series_tickers', [])))
    dynamic = [s for s in discover_series(cfg, errors) if s not in base_series]
    series = base_series + dynamic
    per_series = max(2, min(int(cfg.get('markets_per_series', 12)), 30))
    for s in series:
        if len(selected) >= cap:
            break
        for m in paged_markets('series_ticker', s, cfg, now, errors)[:per_series]:
            choose(m, 'series:' + s)
            if len(selected) >= cap:
                break

    def fetch_one(item: tuple[str, dict]) -> tuple[dict | None, str | None]:
        ticker, market = item
        try:
            book = get('/markets/' + quote(ticker, safe='') + '/orderbook')
            observed = stamp()
            return ({
                'ticker': ticker,
                'event_ticker': market.get('event_ticker'),
                'series_ticker': market.get('series_ticker') or ticker.split('-')[0],
                'title': market.get('title') or '',
                'yes_sub_title': market.get('yes_sub_title'),
                'status': market.get('status'),
                'close_time': market.get('close_time'),
                'volume_fp': market.get('volume_fp'),
                'volume_24h_fp': market.get('volume_24h_fp'),
                'selection_reason': sources[ticker],
                'observed_at_utc': observed,
                **best_quotes(book),
            }, None)
        except RuntimeError as exc:
            return None, f'Book {ticker}: {exc}'

    rows = []
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(fetch_one, item) for item in list(selected.items())[:cap]]
        for future in as_completed(futures):
            quote_data, err = future.result()
            if quote_data:
                rows.append(quote_data)
            if err:
                errors.append(err)
    # Stable presentation: priority tickets first, then ticker. Per-market timestamps preserved.
    priority = {'watchlist_exact': 0, 'watchlist_event': 1}
    rows.sort(key=lambda m: (priority.get(m['selection_reason'], 2), m['series_ticker'], m['ticker']))
    if not rows:
        raise RuntimeError('No books retrieved; existing published snapshot is retained. ' + '; '.join(errors[:5]))
    return {
        'schema_version': 2,
        'generated_at_utc': stamp(),
        'started_at_utc': started,
        'source': 'Kalshi public Trade API, research-only, unauthenticated',
        'warning': 'Snapshot is not a guaranteed executable quote; fees, exact settlement and live market status must be checked before ordering.',
        'selected_market_count': len(selected),
        'successful_orderbooks': len(rows),
        'series_considered': series,
        'selection_counts': dict(scanned),
        'quotes': rows,
        'errors': errors[:100],
    }


def report(snapshot: dict) -> str:
    counts = Counter(m['series_ticker'] for m in snapshot['quotes'])
    lines = [
        '# Kalshi public order-book snapshots (v2)', '',
        f"Updated UTC: **{snapshot['generated_at_utc']}** · Books: **{snapshot['successful_orderbooks']}** of **{snapshot['selected_market_count']}** selected · Warnings: **{len(snapshot['errors'])}**", '',
        '> Research snapshots only: no live execution guarantee. Recheck market status, fees and settlement rules before placing any order.', '',
        '**Coverage by series:** ' + ', '.join(f'{x}: {n}' for x, n in sorted(counts.items())), '',
        '| Ticker | Reason | Contract | YES bid | YES ask | YES ask size | NO bid | NO ask | NO ask size | Observed UTC |',
        '|---|---|---|---:|---:|---:|---:|---:|---:|---|',
    ]
    for m in snapshot['quotes']:
        cell = lambda key: str(m[key]) if m.get(key) is not None else '—'
        title = str(m['title']).replace('|', '/').replace('\n', ' ')[:65]
        lines.append('| ' + ' | '.join([
            m['ticker'], m['selection_reason'], title,
            *[cell(k) for k in ('yes_bid', 'yes_ask', 'yes_ask_size', 'no_bid', 'no_ask', 'no_ask_size', 'observed_at_utc')],
        ]) + ' |')
    lines.extend(['', 'Full machine-readable data: [`latest.json`](latest.json).', ''])
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config.json')
    parser.add_argument('--watchlist', default='watchlist.json')
    parser.add_argument('--out', default='data')
    args = parser.parse_args()
    cfg = json.loads(Path(args.config).read_text(encoding='utf-8'))
    watch = load_watchlist(Path(args.watchlist))
    result = collect(cfg, watch)
    folder = Path(args.out)
    folder.mkdir(parents=True, exist_ok=True)
    (folder / 'latest.json').write_text(json.dumps(result, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    (folder / 'latest.md').write_text(report(result), encoding='utf-8')
    print(f"Collected {result['successful_orderbooks']} books across {len(result['series_considered'])} series; {len(result['errors'])} warnings")


if __name__ == '__main__':
    main()
