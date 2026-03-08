#!/usr/bin/env python3
"""
api_server.py — Live RRG computation backend.
Fetches fresh data from yfinance and computes relative rotation metrics on demand.
"""
import time
import hashlib
import json
import os
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── In-memory cache (symbol -> {data, timestamp}) ──
_price_cache = {}
CACHE_TTL = 300  # 5 minutes

# ── Load sector holdings ──
_holdings_path = os.path.join(os.path.dirname(__file__), "sector_holdings.json")
with open(_holdings_path) as _f:
    SECTOR_HOLDINGS = json.load(_f)

DEFAULT_SECTORS = {
    "XLK": {"name": "Technology", "color": "#00BCD4"},
    "XLF": {"name": "Financials", "color": "#2196F3"},
    "XLV": {"name": "Health Care", "color": "#E91E63"},
    "XLE": {"name": "Energy", "color": "#FF5722"},
    "XLY": {"name": "Consumer Discretionary", "color": "#FF9800"},
    "XLP": {"name": "Consumer Staples", "color": "#8BC34A"},
    "XLI": {"name": "Industrials", "color": "#9E9E9E"},
    "XLB": {"name": "Materials", "color": "#795548"},
    "XLU": {"name": "Utilities", "color": "#FFEB3B"},
    "XLRE": {"name": "Real Estate", "color": "#009688"},
    "XLC": {"name": "Communication Services", "color": "#9C27B0"},
    "BTC-USD": {"name": "Crypto", "color": "#F7931A"},
}

BENCHMARKS = {
    "SPY": "S&P 500",
    "VTI": "Total Market",
    "QQQ": "Nasdaq 100",
    "DIA": "Dow Jones",
    "IWM": "Russell 2000",
    "BTC-USD": "Bitcoin",
}

# Default tracked assets (beyond sector ETFs) — currently none, BTC moved to DEFAULT_SECTORS
DEFAULT_EXTRAS = {}

# Extra colors for user-added symbols
EXTRA_COLORS = [
    "#FF6B6B", "#4ECDC4", "#45B7D1", "#96CEB4", "#FFEAA7",
    "#DDA0DD", "#98D8C8", "#F7DC6F", "#BB8FCE", "#85C1E9",
    "#F8C471", "#82E0AA", "#F1948A", "#AED6F1", "#D2B4DE",
]
_color_index = 0


def _get_next_color():
    global _color_index
    c = EXTRA_COLORS[_color_index % len(EXTRA_COLORS)]
    _color_index += 1
    return c


def _cache_key(symbols, period, interval):
    raw = f"{sorted(symbols)}:{period}:{interval}"
    return hashlib.md5(raw.encode()).hexdigest()


def fetch_prices(symbols, period="2y", interval="1wk"):
    """Fetch weekly close prices, with caching."""
    key = _cache_key(symbols, period, interval)
    now = time.time()

    if key in _price_cache and (now - _price_cache[key]["ts"]) < CACHE_TTL:
        return _price_cache[key]["data"]

    try:
        data = yf.download(list(symbols), period=period, interval=interval, auto_adjust=True, progress=False)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch data: {exc}")

    if data.empty:
        raise HTTPException(status_code=404, detail="No data returned for the given symbols")

    if isinstance(data.columns, pd.MultiIndex):
        close = data["Close"]
    else:
        close = data

    close = close.dropna(how="all")
    _price_cache[key] = {"data": close, "ts": now}
    return close


def compute_single_rrg(sector_prices, benchmark_prices, tail_length=8):
    """Compute RS-Ratio and RS-Momentum for one symbol vs benchmark."""
    raw_rs = (sector_prices / benchmark_prices) * 100
    rs_smoothed = raw_rs.ewm(span=10, adjust=False).mean()

    rolling_mean = rs_smoothed.rolling(window=52, min_periods=20).mean()
    rolling_std = rs_smoothed.rolling(window=52, min_periods=20).std()

    # Avoid division by zero
    rolling_std = rolling_std.replace(0, np.nan)
    rs_ratio = 100 + ((rs_smoothed - rolling_mean) / rolling_std) * 2

    rs_momentum_raw = rs_ratio - rs_ratio.shift(1)
    mom_smoothed = rs_momentum_raw.ewm(span=5, adjust=False).mean()
    mom_mean = mom_smoothed.rolling(window=52, min_periods=20).mean()
    mom_std = mom_smoothed.rolling(window=52, min_periods=20).std()
    mom_std = mom_std.replace(0, np.nan)
    rs_momentum = 100 + ((mom_smoothed - mom_mean) / mom_std) * 2

    valid = rs_ratio.notna() & rs_momentum.notna()
    rs_r = rs_ratio[valid]
    rs_m = rs_momentum[valid]

    if len(rs_r) == 0:
        return None

    n = min(tail_length, len(rs_r))
    tail = []
    for i in range(-n, 0):
        tail.append({
            "date": rs_r.index[i].strftime("%Y-%m-%d"),
            "rs_ratio": round(float(rs_r.iloc[i]), 2),
            "rs_momentum": round(float(rs_m.iloc[i]), 2),
        })

    current = tail[-1] if tail else None
    if current:
        r, m = current["rs_ratio"], current["rs_momentum"]
        if r >= 100 and m >= 100:
            quadrant = "Leading"
        elif r >= 100 and m < 100:
            quadrant = "Weakening"
        elif r < 100 and m >= 100:
            quadrant = "Improving"
        else:
            quadrant = "Lagging"
    else:
        quadrant = "Unknown"

    return {"tail": tail, "current": current, "quadrant": quadrant}


# ── Endpoints ──

@app.get("/api/rrg")
def get_rrg(
    benchmark: str = Query("VTI"),
    tail: int = Query(8, ge=4, le=30),
    extra: str = Query("", description="Comma-separated extra symbols"),
):
    """Compute full RRG for all sectors + any extra symbols vs the given benchmark."""
    benchmark = benchmark.upper().strip()

    # Parse extra symbols
    extra_symbols = []
    if extra:
        extra_symbols = [s.strip().upper() for s in extra.split(",") if s.strip()]

    # Build symbol list — include default extras (like BTC) that aren't the benchmark
    default_extra_syms = [s for s in DEFAULT_EXTRAS if s != benchmark]
    all_symbols = list(DEFAULT_SECTORS.keys()) + [benchmark] + default_extra_syms + extra_symbols
    all_symbols = list(dict.fromkeys(all_symbols))  # deduplicate

    close = fetch_prices(all_symbols)

    if benchmark not in close.columns:
        raise HTTPException(status_code=404, detail=f"Benchmark {benchmark} not found in data")

    bench_prices = close[benchmark]

    results = {}

    # Compute for default sectors
    for sym, meta in DEFAULT_SECTORS.items():
        if sym not in close.columns:
            continue
        rrg = compute_single_rrg(close[sym], bench_prices, tail_length=tail)
        if rrg:
            results[sym] = {
                "symbol": sym,
                "name": meta["name"],
                "color": meta["color"],
                "isDefault": True,
                **rrg,
            }

    # Compute for default extras (e.g. BTC-USD) — always shown unless it's the benchmark
    for sym, meta in DEFAULT_EXTRAS.items():
        if sym == benchmark:
            continue
        if sym not in close.columns:
            continue
        rrg = compute_single_rrg(close[sym], bench_prices, tail_length=tail)
        if rrg:
            results[sym] = {
                "symbol": sym,
                "name": meta["name"],
                "color": meta["color"],
                "isDefault": True,
                **rrg,
            }

    # Compute for user-added extra symbols
    for sym in extra_symbols:
        if sym in DEFAULT_SECTORS or sym in DEFAULT_EXTRAS or sym == benchmark:
            continue
        if sym not in close.columns:
            continue
        rrg = compute_single_rrg(close[sym], bench_prices, tail_length=tail)
        if rrg:
            results[sym] = {
                "symbol": sym,
                "name": sym,
                "color": _get_next_color(),
                "isDefault": False,
                **rrg,
            }

    latest_date = None
    for sym, d in results.items():
        if d["tail"]:
            latest_date = d["tail"][-1]["date"]
            break

    return {
        "benchmark": benchmark,
        "benchmark_name": BENCHMARKS.get(benchmark, benchmark),
        "tail_length": tail,
        "latest_data_date": latest_date,
        "computed_at": datetime.now().isoformat(),
        "sectors": results,
    }


@app.get("/api/holdings")
def get_holdings():
    """Return sector holdings metadata (no price data)."""
    return SECTOR_HOLDINGS


@app.get("/api/rrg-stocks")
def get_rrg_stocks(
    symbols: str = Query(..., description="Comma-separated stock symbols"),
    benchmark: str = Query("VTI"),
    tail: int = Query(8, ge=4, le=30),
):
    """Compute RRG for a batch of individual stocks vs benchmark."""
    benchmark = benchmark.upper().strip()
    stock_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not stock_list:
        raise HTTPException(status_code=422, detail="No symbols provided")

    all_symbols = stock_list + [benchmark]
    all_symbols = list(dict.fromkeys(all_symbols))

    close = fetch_prices(all_symbols)

    if benchmark not in close.columns:
        raise HTTPException(status_code=404, detail=f"Benchmark {benchmark} not found")

    bench_prices = close[benchmark]
    results = {}

    for sym in stock_list:
        if sym == benchmark or sym not in close.columns:
            continue
        rrg = compute_single_rrg(close[sym], bench_prices, tail_length=tail)
        if rrg:
            results[sym] = {
                "symbol": sym,
                "name": sym,
                **rrg,
            }

    return {
        "benchmark": benchmark,
        "tail_length": tail,
        "computed_at": datetime.now().isoformat(),
        "stocks": results,
    }


class PortfolioRequest(BaseModel):
    symbols: list[str]
    benchmark: str = "VTI"
    tail: int = 8


@app.post("/api/rrg-portfolio")
def get_rrg_portfolio(req: PortfolioRequest):
    """Compute RRG for an equal-weighted portfolio composite vs benchmark."""
    benchmark = req.benchmark.upper().strip()
    stock_list = [s.strip().upper() for s in req.symbols if s.strip()]
    tail_length = max(4, min(30, req.tail))

    if not stock_list or len(stock_list) < 1:
        raise HTTPException(status_code=422, detail="Portfolio needs at least 1 symbol")

    all_symbols = stock_list + [benchmark]
    all_symbols = list(dict.fromkeys(all_symbols))

    close = fetch_prices(all_symbols)

    if benchmark not in close.columns:
        raise HTTPException(status_code=404, detail=f"Benchmark {benchmark} not found")

    # Build equal-weighted composite: average of normalised returns
    available = [s for s in stock_list if s in close.columns and s != benchmark]
    if not available:
        raise HTTPException(status_code=404, detail="No valid stock data for portfolio")

    # Normalise each stock to its first non-NaN value, then average
    normed = pd.DataFrame(index=close.index)
    for sym in available:
        prices = close[sym].dropna()
        if len(prices) < 20:
            continue
        normed[sym] = prices / prices.iloc[0] * 100

    if normed.empty or normed.shape[1] == 0:
        raise HTTPException(status_code=404, detail="Insufficient data for portfolio")

    composite = normed.mean(axis=1).dropna()
    bench_prices = close[benchmark]

    # Align indices
    common_idx = composite.index.intersection(bench_prices.dropna().index)
    composite = composite.loc[common_idx]
    bench_aligned = bench_prices.loc[common_idx]

    rrg = compute_single_rrg(composite, bench_aligned, tail_length=tail_length)
    if not rrg:
        raise HTTPException(status_code=404, detail="Could not compute RRG for portfolio")

    return {
        "benchmark": benchmark,
        "tail_length": tail_length,
        "computed_at": datetime.now().isoformat(),
        "portfolio": rrg,
        "symbols_used": list(normed.columns),
    }


class ValidateRequest(BaseModel):
    symbol: str


@app.post("/api/validate-symbol")
def validate_symbol(req: ValidateRequest):
    """Check if a symbol is valid by attempting to fetch its data."""
    sym = req.symbol.strip().upper()
    if not sym:
        raise HTTPException(status_code=422, detail="Empty symbol")
    try:
        info = yf.Ticker(sym)
        hist = info.history(period="1mo")
        if hist.empty:
            raise HTTPException(status_code=404, detail=f"No data found for {sym}")
        name = getattr(info, 'info', {}).get('shortName', sym)
        return {"symbol": sym, "name": name, "valid": True}
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=404, detail=f"Symbol {sym} not found")


@app.get("/api/health")
def health():
    return {"status": "ok", "time": datetime.now().isoformat()}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
