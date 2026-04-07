import json
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

BREADTH_PATH = Path("data/breadth-history.json")
OUTPUT_PATH = Path("data/fundamentals.json")

TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
COMPANY_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

LOOKBACK_ROWS = int(os.getenv("FUNDAMENTALS_LOOKBACK_ROWS", "45"))
MAX_SYMBOLS = int(os.getenv("FUNDAMENTALS_MAX_SYMBOLS", "300"))
MAX_PERIODS = int(os.getenv("FUNDAMENTALS_MAX_PERIODS", "6"))
REFRESH_AFTER_DAYS = int(os.getenv("FUNDAMENTALS_REFRESH_AFTER_DAYS", "14"))
REQUEST_SLEEP = float(os.getenv("FUNDAMENTALS_REQUEST_SLEEP", "0.2"))

ANNUAL_FORMS = {"10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A"}
FLOW_MIN_DAYS = 300
FLOW_MAX_DAYS = 390
NOW_UTC = datetime.now(timezone.utc)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "themikeygamble fundamentals tracker (public GitHub project) contact: github-actions",
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json, text/plain, */*",
})

METRIC_SPECS = {
    "revenue": {
        "unit": "USD",
        "instant": False,
        "concepts": [
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "RevenueFromContractWithCustomerIncludingAssessedTax",
            "SalesRevenueNet",
            "Revenues",
        ],
    },
    "operating_income": {
        "unit": "USD",
        "instant": False,
        "concepts": ["OperatingIncomeLoss"],
    },
    "net_income": {
        "unit": "USD",
        "instant": False,
        "concepts": ["NetIncomeLoss", "ProfitLoss"],
    },
    "operating_cash_flow": {
        "unit": "USD",
        "instant": False,
        "concepts": [
            "NetCashProvidedByUsedInOperatingActivities",
            "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
        ],
    },
    "gross_profit": {
        "unit": "USD",
        "instant": False,
        "concepts": ["GrossProfit"],
    },
    "operating_expense": {
        "unit": "USD",
        "instant": False,
        "concepts": ["OperatingExpenses"],
    },
    "eps": {
        "unit": "USD/shares",
        "instant": False,
        "concepts": [
            "EarningsPerShareDiluted",
            "EarningsPerShareBasicAndDiluted",
            "EarningsPerShareBasic",
        ],
    },
    "total_assets": {
        "unit": "USD",
        "instant": True,
        "concepts": ["Assets"],
    },
    "total_liabilities": {
        "unit": "USD",
        "instant": True,
        "concepts": ["Liabilities"],
    },
    "cash": {
        "unit": "USD",
        "instant": True,
        "concepts": [
            "CashAndCashEquivalentsAtCarryingValue",
            "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
            "CashCashEquivalentsAndShortTermInvestments",
        ],
    },
    "working_capital": {
        "unit": "USD",
        "instant": True,
        "concepts": ["WorkingCapital"],
    },
    "assets_current": {
        "unit": "USD",
        "instant": True,
        "concepts": ["AssetsCurrent"],
    },
    "liabilities_current": {
        "unit": "USD",
        "instant": True,
        "concepts": ["LiabilitiesCurrent"],
    },
    "share_count": {
        "unit": "shares",
        "instant": False,
        "concepts": [
            "WeightedAverageNumberOfDilutedSharesOutstanding",
            "WeightedAverageNumberOfSharesOutstandingDiluted",
            "WeightedAverageNumberOfShareOutstandingBasicAndDiluted",
            "CommonStockSharesOutstanding",
            "EntityCommonStockSharesOutstanding",
        ],
    },
}


def load_json(path):
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))


def fetch_json(url):
    response = SESSION.get(url, timeout=90)
    response.raise_for_status()
    return response.json()


def derive_universe(breadth_payload):
    rows = breadth_payload.get("rows", [])[:LOOKBACK_ROWS]
    scores = defaultdict(float)

    for row_index, row in enumerate(rows):
        recency_score = max(1.0, LOOKBACK_ROWS - row_index)
        for entries in (row.get("lists") or {}).values():
            for item in entries:
                symbol = normalize_symbol(item)
                if not symbol:
                    continue
                scores[symbol] += recency_score

    prioritized = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
    universe = [symbol for symbol, _ in prioritized[:MAX_SYMBOLS]]
    print(f"Selected {len(universe)} symbols from the latest {len(rows)} breadth rows")
    return universe


def normalize_symbol(item):
    if isinstance(item, str):
        return item.strip().upper()
    symbol = item.get("symbol") or item.get("ticker")
    if not symbol:
        return ""
    return str(symbol).strip().upper()


def load_ticker_map():
    payload = fetch_json(TICKERS_URL)
    mapping = {}

    entries = payload.values() if isinstance(payload, dict) else payload
    for item in entries:
        symbol = str(item.get("ticker", "")).strip().upper()
        cik = str(item.get("cik_str", "")).strip()
        if symbol and cik:
            mapping[symbol] = {
                "cik": cik.zfill(10),
                "company_name": item.get("title", "").strip(),
            }

    print(f"Loaded {len(mapping)} SEC ticker mappings")
    return mapping


def should_refresh(existing_entry):
    if not existing_entry:
        return True
    refreshed_at = existing_entry.get("refreshed_at_utc")
    if not refreshed_at:
        return True
    try:
        refresh_dt = datetime.fromisoformat(refreshed_at)
    except ValueError:
        return True
    if refresh_dt.tzinfo is None:
        refresh_dt = refresh_dt.replace(tzinfo=timezone.utc)
    return (NOW_UTC - refresh_dt) >= timedelta(days=REFRESH_AFTER_DAYS)


def extract_metric_series(companyfacts, spec):
    facts = companyfacts.get("facts", {}).get("us-gaap", {})

    for concept in spec["concepts"]:
        concept_payload = facts.get(concept)
        if not concept_payload:
            continue

        units = concept_payload.get("units", {})
        if spec["unit"] not in units:
            continue

        series = build_series(units[spec["unit"]], spec["instant"])
        if series:
            return series

    return []


def build_series(items, instant):
    bucket = {}

    for item in items:
        if item.get("form") not in ANNUAL_FORMS:
            continue

        end_date = item.get("end")
        value = item.get("val")
        if end_date is None or value in (None, ""):
            continue

        if not instant:
            if not is_annual_duration(item):
                continue

        fiscal_year = derive_fiscal_year(item)
        key = str(fiscal_year or end_date[:4])
        candidate = {
            "label": f"FY{key}",
            "fy": key,
            "end": end_date,
            "filed": item.get("filed"),
            "frame": item.get("frame"),
            "value": round_number(value),
            "sort_key": (
                item.get("fy") or 0,
                item.get("filed") or "",
                item.get("frame") or "",
            ),
        }

        existing = bucket.get(key)
        if existing is None or candidate["sort_key"] > existing["sort_key"]:
            bucket[key] = candidate

    series = sorted(bucket.values(), key=lambda point: point["fy"])
    for point in series:
        point.pop("sort_key", None)
    return series[-MAX_PERIODS:]


def is_annual_duration(item):
    start_date = item.get("start")
    end_date = item.get("end")
    if not start_date or not end_date:
        return False
    try:
        start = datetime.fromisoformat(start_date)
        end = datetime.fromisoformat(end_date)
    except ValueError:
        return False
    duration_days = (end - start).days
    return FLOW_MIN_DAYS <= duration_days <= FLOW_MAX_DAYS


def derive_fiscal_year(item):
    fy = item.get("fy")
    if fy not in (None, ""):
        return str(fy)
    end_date = item.get("end", "")
    return end_date[:4] if len(end_date) >= 4 else None


def round_number(value):
    number = float(value)
    if abs(number) >= 1:
        return round(number, 2)
    return round(number, 4)


def keyed_series(series):
    return {point["fy"]: point for point in series}


def compute_working_capital(metrics):
    direct = metrics.get("working_capital")
    if direct:
        return direct

    assets_current = keyed_series(metrics.get("assets_current", []))
    liabilities_current = keyed_series(metrics.get("liabilities_current", []))
    years = sorted(set(assets_current) & set(liabilities_current))
    output = []
    for year in years[-MAX_PERIODS:]:
        value = assets_current[year]["value"] - liabilities_current[year]["value"]
        output.append({
            "label": f"FY{year}",
            "fy": year,
            "end": assets_current[year]["end"],
            "filed": assets_current[year]["filed"] or liabilities_current[year]["filed"],
            "value": round_number(value),
        })
    return output


def compute_margin_series(numerator_series, revenue_series):
    numerators = keyed_series(numerator_series)
    revenues = keyed_series(revenue_series)
    years = sorted(set(numerators) & set(revenues))
    output = []

    for year in years[-MAX_PERIODS:]:
        revenue = revenues[year]["value"]
        if revenue in (None, 0):
            continue
        margin = (numerators[year]["value"] / revenue) * 100
        output.append({
            "label": f"FY{year}",
            "fy": year,
            "end": revenues[year]["end"],
            "filed": revenues[year]["filed"] or numerators[year]["filed"],
            "value": round(margin, 2),
        })

    return output


def latest_value(series, field):
    if not series:
        return None
    return series[-1].get(field)


def build_symbol_payload(symbol, mapping_entry, companyfacts):
    metrics = {}
    for metric_name, spec in METRIC_SPECS.items():
        metrics[metric_name] = extract_metric_series(companyfacts, spec)

    metrics["working_capital"] = compute_working_capital(metrics)
    metrics["gross_margin"] = compute_margin_series(metrics.get("gross_profit", []), metrics.get("revenue", []))
    metrics["operating_margin"] = compute_margin_series(metrics.get("operating_income", []), metrics.get("revenue", []))
    metrics["net_margin"] = compute_margin_series(metrics.get("net_income", []), metrics.get("revenue", []))

    metrics.pop("assets_current", None)
    metrics.pop("liabilities_current", None)

    return {
        "symbol": symbol,
        "cik": mapping_entry["cik"],
        "company_name": companyfacts.get("entityName") or mapping_entry.get("company_name") or symbol,
        "exchange": latest_exchange(companyfacts),
        "refreshed_at_utc": NOW_UTC.isoformat(),
        "latest_fiscal_year": latest_value(metrics.get("revenue", []), "fy") or latest_value(metrics.get("total_assets", []), "fy"),
        "last_filed": latest_value(metrics.get("revenue", []), "filed") or latest_value(metrics.get("total_assets", []), "filed"),
        "metrics": metrics,
    }


def latest_exchange(companyfacts):
    dei_facts = companyfacts.get("facts", {}).get("dei", {})
    exchange_fact = dei_facts.get("SecurityExchangeName")
    if not exchange_fact:
        return ""
    units = exchange_fact.get("units", {})
    strings = units.get("pure") or []
    valid = [item for item in strings if item.get("form") in ANNUAL_FORMS and item.get("val")]
    if not valid:
        return ""
    valid.sort(key=lambda item: (item.get("fy") or 0, item.get("filed") or ""))
    return str(valid[-1].get("val", "")).strip()


def main():
    breadth_payload = load_json(BREADTH_PATH)
    if not breadth_payload or not breadth_payload.get("rows"):
        raise RuntimeError("Breadth history is required before fundamentals can be generated.")

    existing_payload = load_json(OUTPUT_PATH) or {"symbols": {}}
    existing_symbols = existing_payload.get("symbols", {})

    universe = derive_universe(breadth_payload)
    ticker_map = load_ticker_map()

    output_symbols = {}
    requested = 0

    for symbol in universe:
        mapping_entry = ticker_map.get(symbol)
        if not mapping_entry:
            print(f"Skipping {symbol}: no SEC ticker mapping")
            continue

        existing_entry = existing_symbols.get(symbol)
        if existing_entry and not should_refresh(existing_entry):
            output_symbols[symbol] = existing_entry
            continue

        requested += 1
        print(f"Fetching {symbol} ({requested})")
        try:
            companyfacts = fetch_json(COMPANY_FACTS_URL.format(cik=mapping_entry["cik"]))
            output_symbols[symbol] = build_symbol_payload(symbol, mapping_entry, companyfacts)
        except Exception as exc:
            print(f"Failed {symbol}: {exc}")
            if existing_entry:
                output_symbols[symbol] = existing_entry
        time.sleep(REQUEST_SLEEP)

    payload = {
        "generated_at_utc": NOW_UTC.isoformat(),
        "meta": {
            "source": "SEC EDGAR companyfacts",
            "universe_source": "Breadth-derived watchlist",
            "lookback_rows": LOOKBACK_ROWS,
            "max_symbols": MAX_SYMBOLS,
            "refresh_after_days": REFRESH_AFTER_DAYS,
        },
        "symbols": dict(sorted(output_symbols.items())),
    }

    save_json(OUTPUT_PATH, payload)
    print(f"Wrote {len(output_symbols)} symbols to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
