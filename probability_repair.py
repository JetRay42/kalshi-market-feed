#!/usr/bin/env python3
"""Safe v1.1 research orchestration for the existing probability_engine.py.

Fixes early odds cooldown during a multi-sport batch, preserves timestamped
historical research, and supplements same-day-only weather samples with
future-day open contracts. No authenticated Kalshi API and no order placement.
"""
from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from zoneinfo import ZoneInfo

import collector as kalshi
import probability_engine as prior

UTC = timezone.utc
ODDS_SPORTS = (("basketball_nba", "KXNBA"), ("americanfootball_nfl", "KXNFL"))
WEATHER_SERIES = {"CLIMSP": "KXHIGHTMIN", "CLIMDW": "KXHIGHCHI",
                  "CLIATL": "KXHIGHTATL", "CLIPHX": "KXHIGHTPHX"}


def timestamp_now():
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def error_label(exc):
    # HTTPError text can include URLs WITH API KEYS: never publish str(exc).
    if isinstance(exc, HTTPError):
        return f"HTTP {exc.code}"
    return type(exc).__name__


def age_minutes(stamp, now=None):
    dt = prior.parse_utc(stamp)
    if not dt:
        return None
    mins = ((now or datetime.now(UTC)) - dt).total_seconds() / 60
    return round(mins, 2) if mins >= 0 else None


def load_json(path, fallback):
    return prior.load(path, fallback)


def sports_odds_batch(quotes, config, api_key, output_root, now=None):
    """One batch window, up to both sports. Respect v1's existing quota ceilings."""
    now = now or datetime.now(UTC)
    budget_file = output_root / "odds_budget.json"
    cache_file = output_root / "sports" / "odds_source_cache.json"
    budget = load_json(budget_file, {}) or {}
    cache = load_json(cache_file, {}) or {}
    month, day = now.strftime("%Y-%m"), now.date().isoformat()
    if budget.get("month") != month:
        budget = {"month": month, "requests": 0, "approx_credits_used": 0}
    if budget.get("day") != day:
        budget["day"] = day
        budget["day_calls"] = 0
    requested = [sport for sport, prefix in ODDS_SPORTS
                 if any(str(q.get("series_ticker", "")).startswith(prefix) for q in quotes)]
    remaining_month = max(0, int(config.get("max_odds_api_requests_per_month", 120)) - int(budget.get("requests", 0)))
    remaining_day = max(0, int(config.get("max_odds_api_requests_per_day", 4)) - int(budget.get("day_calls", 0)))
    last_batch = prior.parse_utc(budget.get("last_batch_at_utc"))
    spacing_minutes = max(60, float(config.get("min_odds_refresh_hours", 12)) * 60)
    due = not last_batch or (now - last_batch).total_seconds() / 60 >= spacing_minutes
    refreshed, issues = [], []
    fetched_events = []
    # A batch is a SINGLE cooldown window. Don't update and recheck the cooldown
    # between NBA and NFL, which is the original bug.
    if api_key and due and requested and remaining_month and remaining_day:
        for sport in requested[:min(remaining_month, remaining_day)]:
            budget["requests"] = int(budget.get("requests", 0)) + 1
            budget["day_calls"] = int(budget.get("day_calls", 0)) + 1
            budget["approx_credits_used"] = int(budget.get("approx_credits_used", 0)) + 3
            try:
                rows = prior.web_json(f"{prior.ODDS_API}/sports/{sport}/odds",
                            {"apiKey": api_key, "regions": "us",
                             "markets": "h2h,spreads,totals", "oddsFormat": "decimal"})
                if isinstance(rows, list):
                    fetched_events.extend(x for x in rows if isinstance(x, dict))
                    refreshed.append(sport)
                else:
                    issues.append(f"{sport}: non-list API payload")
            except (HTTPError, URLError, TimeoutError, ValueError, OSError) as exc:
                issues.append(f"{sport}: {error_label(exc)}")
        budget["last_batch_at_utc"] = timestamp_now()
        prior.save(budget_file, budget)
        if refreshed:
            # Preserve per-sport groups when a partial batch occurs.
            by_sport = dict(cache.get("events_by_sport") or {})
            for sport in refreshed:
                # Order from API is not guaranteed; locate by the sport field
                # where provided; otherwise fall back to sequential queried data.
                by_sport[sport] = [x for x in fetched_events if x.get("sport_key") == sport]
            # Some provider responses omit sport_key: persist all newly fetched
            # records as a fallback without corrupting other sport groups.
            all_events = list(fetched_events) + [x for group in by_sport.values() for x in group]
            dedup = {x.get("id") or repr(x): x for x in all_events}
            cache = {"fetched_at_utc": timestamp_now(),
                     "events": list(dedup.values()), "events_by_sport": by_sport,
                     "warning": "Public bookmaker odds are historical research data, not current execution prices."}
            prior.save(cache_file, cache)
    else:
        if not api_key:
            issues.append("THE_ODDS_API_KEY not configured; automated odds unavailable")
        elif not due:
            issues.append("odds refresh on cooldown; using timestamped cached evidence if available")
        elif not remaining_month or not remaining_day:
            issues.append("bookmaker request limit reached; using timestamped cached evidence if available")
        elif not requested:
            issues.append("no NFL/NBA contract families in current feed")
    # Even during a no-refresh run, retain a cache as historical RESEARCH only.
    # Source bookmaker timestamps inside each market are checked below.
    source_events = cache.get("events") if cache else []
    if not source_events and fetched_events:
        source_events = fetched_events
    return source_events or [], refreshed, issues, budget, cache.get("fetched_at_utc")


def odds_evaluations(quotes, events, config, source_cache_time):
    """Separate fresh model matches from stale candidate-only history."""
    now = datetime.now(UTC)
    max_age = float(config.get("max_bookmaker_age_minutes", 45))
    freshness_cutoff = min(max_age, 45)
    fresh, historical = [], []
    for q in quotes:
        matching_fresh, matching_historical = [], []
        for event in events:
            if not prior.align_event(q, event):
                continue
            spec = prior.market_spec(q, event)
            if not spec:
                continue
            matching_fresh.extend(prior.matched_book_probabilities(event, spec, freshness_cutoff, now))
            matching_historical.extend(prior.matched_book_probabilities(event, spec, 12 * 60, now))
        # All results are based on the exact contract and distinct bookmakers.
        for dest, matches, status in ((fresh, matching_fresh, "FRESH research estimate, check live Kalshi"),
                                      (historical, matching_historical, "HISTORICAL / WATCH ONLY; no actionable entry")):
            distinct = {x["bookmaker"]: x for x in matches}
            consensus = prior.consensus_evidence(list(distinct.values()),
                                config.get("min_independent_bookmakers", 2))
            if not consensus:
                continue
            lo, hi = consensus["p_lower"], consensus["p_upper"]
            item = {"ticker": q["ticker"], "title": q.get("title"),
                    "event_ticker": q.get("event_ticker"),
                    "kalshi_observed_at_utc": q.get("observed_at_utc"),
                    "bookmaker_cache_generated_at_utc": source_cache_time,
                    "model": "exact-outcome matched bookmaker no-vig consensus",
                    "model_status": status, **consensus,
                    "sources": list(distinct.values()),
                    "valuation": prior.quote_valuation(q, lo, hi)}
            if dest is historical:
                # Even if the provider probabilities are still useful as a
                # historical reference, don't present stale ask comparison as
                # an actionable edge in a model output.
                item.pop("valuation", None)
                item["warning"] = "Historical bookmaker data; do not place an order without refreshed evidence"
            dest.append(item)
    # A fresh result is already in the historical set; avoid duplication.
    fresh_tickers = {x["ticker"] for x in fresh}
    return fresh, [x for x in historical if x["ticker"] not in fresh_tickers]


def supplement_weather(quotes, config, errors, now=None):
    """Discover 1-3-day-ahead temperature contracts absent from rotating feed."""
    now = now or datetime.now(UTC)
    existing = {q.get("ticker") for q in quotes}
    forward = []
    diagnostic = {"existing_weather_sample": len(quotes),
                  "active_future_market_candidates": 0,
                  "supplemented": 0, "quoted_supplements": 0,
                  "sources_queried": 0}
    locs = [l for l in config.get("weather_locations", [])
            if l.get("settlement_code") in WEATHER_SERIES]
    # One bounded list request per city; no reliance on close_time being event day.
    for loc in locs:
        series = WEATHER_SERIES[loc["settlement_code"]]
        diagnostic["sources_queried"] += 1
        local_date = now.astimezone(ZoneInfo(loc["timezone"])).date()
        options = []
        listed = []
        cursor = None
        # Calendar-rotating series sometimes fill the first page with today's
        # contracts; inspect at most two pages without expanding API load too far.
        for _ in range(2):
            params = {"series_ticker": series, "status": "open", "limit": 200}
            if cursor:
                params["cursor"] = cursor
            try:
                response = kalshi.get("/markets", params)
            except Exception as exc:
                errors.append(f"future weather discovery {series}: {error_label(exc)}")
                break
            listed.extend(response.get("markets", []) or [])
            next_cursor = response.get("cursor") or None
            if not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor
        for m in listed:
            if not isinstance(m, dict) or not kalshi.market_active(m, now.timestamp()):
                continue
            event_date = prior.event_date_from_ticker(m.get("event_ticker", ""))
            if not event_date or not (1 <= (event_date - local_date).days <= 3):
                continue
            if loc["settlement_code"] not in (m.get("rules_primary") or ""):
                continue
            if not prior.parse_weather_condition(m):
                continue
            options.append(m)
        diagnostic["active_future_market_candidates"] += len(options)
        options.sort(key=lambda m: (-float(m.get("volume_24h_fp") or 0), m.get("ticker", "")))
        # Cap book calls: <=3 per city/run, minimize 429 and workflow time.
        for m in options[:3]:
            ticker = m.get("ticker")
            if not ticker or ticker in existing:
                continue
            q = {"ticker": ticker, "event_ticker": m.get("event_ticker"),
                 "series_ticker": m.get("series_ticker") or series,
                 "title": m.get("title", ""), "rules_primary": m.get("rules_primary", ""),
                 "rules_secondary": m.get("rules_secondary", ""),
                 "status": m.get("status"), "close_time": m.get("close_time"),
                 "selection_reason": "next_day_weather_supplement",
                 "observed_at_utc": None,
                 "yes_bid": None, "yes_ask": None, "no_bid": None, "no_ask": None}
            try:
                book = kalshi.get("/markets/" + quote(ticker, safe="") + "/orderbook")
                q.update(kalshi.best_quotes(book))
                q["observed_at_utc"] = timestamp_now()
                diagnostic["quoted_supplements"] += 1
            except Exception as exc:
                errors.append(f"weather book {ticker}: {error_label(exc)}")
            forward.append(q)
            existing.add(ticker)
    diagnostic["supplemented"] = len(forward)
    return list(quotes) + forward, diagnostic


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="probability_config.json")
    ap.add_argument("--data-root", default="data")
    ap.add_argument("--sports-evidence", default="sports_benchmarks.json")
    args = ap.parse_args(argv)
    cfg = load_json(Path(args.config), {})
    root = Path(args.data_root)
    output = root / "probabilities"
    sports = load_json(root / "feeds/sports/latest.json", {})
    weather = load_json(root / "feeds/weather_politics/latest.json", {})
    if not sports.get("quotes") and not weather.get("quotes"):
        raise RuntimeError("Both category feeds missing. Previous probability files preserved.")
    sports_report, weather_report = None, None
    if sports.get("quotes"):
        events, refreshed, errors, budget, cache_stamp = sports_odds_batch(
            sports["quotes"], cfg, os.getenv("THE_ODDS_API_KEY", "").strip(), output)
        fresh, historical = odds_evaluations(sports["quotes"], events, cfg, cache_stamp)
        if fresh:
            prior.save(output / "sports/last_fresh.json",
                       {"generated_at_utc": timestamp_now(), "evaluations": fresh,
                        "input_feed_generated_at_utc": sports.get("generated_at_utc")})
        last_fresh = load_json(output / "sports/last_fresh.json", {}) or {}
        sports_report = {"schema_version": 2, "generated_at_utc": timestamp_now(),
            "input_feed_generated_at_utc": sports.get("generated_at_utc"),
            "source_type": "exact-matched public sportsbook consensus; research only",
            "research_only": True, "estimated_contracts": len(fresh),
            "evaluations": fresh, "historical_evaluations": historical[:25],
            "last_fresh_estimates_generated_at_utc": last_fresh.get("generated_at_utc"),
            "last_fresh_estimate_count": len(last_fresh.get("evaluations", [])),
            "odds_cache_generated_at_utc": cache_stamp,
            "refreshed_sports": refreshed, "errors": errors,
            "budget": {k: budget.get(k) for k in
                       ("month", "requests", "approx_credits_used", "day", "day_calls", "last_batch_at_utc")},
            "warnings": ["Cached bookmaker estimates must NEVER be treated as current prices or a verified trade edge.",
                         "One-share general Kalshi fees are approximate; check actual contract fees and rules."]}
        prior.save(output / "sports/latest.json", sports_report)
    if weather.get("quotes"):
        weather_errors = []
        rows, diagnostics = supplement_weather(weather["quotes"], cfg, weather_errors)
        estimates = prior.weather_research(rows, cfg, weather_errors, output)
        weather_report = {"schema_version": 2, "generated_at_utc": timestamp_now(),
            "input_feed_generated_at_utc": weather.get("generated_at_utc"),
            "source_type": "Open-Meteo gridded forecasts plus next-day Kalshi market discovery",
            "research_only": True, "estimated_contracts": len(estimates),
            "evaluations": estimates, "diagnostics": diagnostics,
            "errors": weather_errors,
            "warnings": ["WEATHER RESEARCH ONLY: gridded forecasts not calibrated to Weather Company settlement stations.",
                         "Model uncertainty and forecast errors do not establish live positive expected value."]}
        prior.save(output / "weather/latest.json", weather_report)
    print("Sports", sports_report["estimated_contracts"] if sports_report else "no input",
          "fresh estimates; Weather", weather_report["estimated_contracts"] if weather_report else "no input",
          "research estimates; no trades placed")
    for label, report in (("Sports", sports_report), ("Weather", weather_report)):
        if report:
            for issue in (report["errors"] or [])[:4]:
                print(label + " notice:", issue)


if __name__ == "__main__":
    main()
