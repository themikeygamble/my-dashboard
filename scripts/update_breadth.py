import json
import math
import time
from datetime import datetime, timezone
from io import StringIO

import pandas as pd
import requests
import yfinance as yf

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
OUTPUT_PATH = "data/breadth-history.json"

LOOKBACK_PERIOD = "18mo"
MAX_HISTORY_ROWS = 180
BATCH_SIZE = 120
REQUEST_SLEEP = 0.4


def download_nasdaq_universe():
    r = requests.get(NASDAQ_LISTED_URL, timeout=60)
    r.raise_for_status()

    lines = r.text.strip().splitlines()
    header = lines[0].split("|")
    rows = [line.split("|") for line in lines[1:] if line and not line.startswith("File Creation Time")]
    df = pd.DataFrame(rows, columns=header)

    df["Symbol"] = df["Symbol"].astype(str).str.strip().str.upper()
    df["Test Issue"] = df["Test Issue"].astype(str).str.strip().str.upper()
    df["ETF"] = df["ETF"].astype(str).str.strip().str.upper()
    df["NextShares"] = df["NextShares"].astype(str).str.strip().str.upper()

    df = df[df["Test Issue"] == "N"].copy()
    df = df[df["ETF"] == "N"].copy()
    df = df[df["NextShares"] == "N"].copy()

    bad_chars = ["^", "$", "/"]
    for ch in bad_chars:
        df = df[~df["Symbol"].str.contains("\\" + ch, regex=True, na=False)]

    symbols = df["Symbol"].dropna().unique().tolist()
    symbols.sort()
    return symbols


def to_yf_symbol(symbol):
    return symbol.replace(".", "-").replace("/", "-")


def chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def extract_batch_history(batch_symbols):
    yf_symbols = [to_yf_symbol(s) for s in batch_symbols]
    raw_to_yf = dict(zip(batch_symbols, yf_symbols))
    yf_to_raw = {v: k for k, v in raw_to_yf.items()}

    data = yf.download(
        tickers=yf_symbols,
        period=LOOKBACK_PERIOD,
        interval="1d",
        auto_adjust=False,
        progress=False,
        group_by="ticker",
        threads=True,
        prepost=False
    )

    if data is None or data.empty:
        return pd.DataFrame(columns=["date", "symbol", "close", "adj_close"])

    frames = []

    if isinstance(data.columns, pd.MultiIndex):
        available = set(data.columns.get_level_values(0))
        for yf_symbol in yf_symbols:
            if yf_symbol not in available:
                continue

            sub = data[yf_symbol].copy()
            sub = sub.reset_index()

            if "Date" not in sub.columns:
                continue

            rename_map = {
                "Date": "date",
                "Close": "close",
                "Adj Close": "adj_close"
            }
            sub = sub.rename(columns=rename_map)

            keep_cols = [c for c in ["date", "close", "adj_close"] if c in sub.columns]
            sub = sub[keep_cols].copy()

            if "close" not in sub.columns:
                continue
            if "adj_close" not in sub.columns:
                sub["adj_close"] = sub["close"]

            sub["symbol"] = yf_to_raw[yf_symbol]
            frames.append(sub)
    else:
        sub = data.reset_index()
        rename_map = {
            "Date": "date",
            "Close": "close",
            "Adj Close": "adj_close"
        }
        sub = sub.rename(columns=rename_map)

        keep_cols = [c for c in ["date", "close", "adj_close"] if c in sub.columns]
        sub = sub[keep_cols].copy()

        if "close" in sub.columns:
            if "adj_close" not in sub.columns:
                sub["adj_close"] = sub["close"]
            sub["symbol"] = batch_symbols[0]
            frames.append(sub)

    if not frames:
        return pd.DataFrame(columns=["date", "symbol", "close", "adj_close"])

    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"]).dt.tz_localize(None)
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out["adj_close"] = pd.to_numeric(out["adj_close"], errors="coerce")
    out = out.dropna(subset=["date", "symbol", "close", "adj_close"]).copy()

    return out


def download_all_histories(symbols):
    frames = []

    for batch in chunked(symbols, BATCH_SIZE):
        try:
            batch_df = extract_batch_history(batch)
            if not batch_df.empty:
                frames.append(batch_df)
        except Exception as e:
            print(f"Batch failed ({batch[:3]}...): {e}")
        time.sleep(REQUEST_SLEEP)

    if not frames:
        return pd.DataFrame(columns=["date", "symbol", "close", "adj_close"])

    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values(["symbol", "date"]).drop_duplicates(["symbol", "date"], keep="last")
    return df


def download_nasdaq_composite():
    data = yf.download(
        tickers="^IXIC",
        period=LOOKBACK_PERIOD,
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
        prepost=False
    )

    if data is None or data.empty:
        return pd.DataFrame(columns=["date", "nasdaq_close"])

    data = data.reset_index()
    data = data.rename(columns={"Date": "date", "Close": "nasdaq_close"})
    data["date"] = pd.to_datetime(data["date"]).dt.tz_localize(None)
    data["nasdaq_close"] = pd.to_numeric(data["nasdaq_close"], errors="coerce")
    data = data.dropna(subset=["date", "nasdaq_close"]).copy()
    return data[["date", "nasdaq_close"]]


def ratio_to_json_value(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isinf(value):
        return "Infinity"
    return round(float(value), 2)


def safe_ratio(up_count, down_count):
    if down_count == 0 and up_count == 0:
        return None
    if down_count == 0:
        return float("inf")
    return up_count / down_count


def build_rows(stock_df, ixic_df):
    df = stock_df.copy()
    if df.empty:
        return []

    df = df.sort_values(["symbol", "date"]).copy()

    df["prev_close"] = df.groupby("symbol")["close"].shift(1)

    df["adj_21"] = df.groupby("symbol")["adj_close"].shift(21)
    df["adj_34"] = df.groupby("symbol")["adj_close"].shift(34)
    df["adj_63"] = df.groupby("symbol")["adj_close"].shift(63)

    df["ret_1d"] = (df["close"] / df["prev_close"]) - 1
    df["ret_21d"] = (df["adj_close"] / df["adj_21"]) - 1
    df["ret_34d"] = (df["adj_close"] / df["adj_34"]) - 1
    df["ret_63d"] = (df["adj_close"] / df["adj_63"]) - 1

    df["up4_today"] = df["ret_1d"] >= 0.04
    df["down4_today"] = df["ret_1d"] <= -0.04

    df["up25_quarter"] = df["ret_63d"] >= 0.25
    df["down25_quarter"] = df["ret_63d"] <= -0.25

    df["up25_month"] = df["ret_21d"] >= 0.25
    df["down25_month"] = df["ret_21d"] <= -0.25

    df["up50_month"] = df["ret_21d"] >= 0.50
    df["down50_month"] = df["ret_21d"] <= -0.50

    df["up13_34d"] = df["ret_34d"] >= 0.13
    df["down13_34d"] = df["ret_34d"] <= -0.13

    daily_counts = (
        df.groupby("date")[["up4_today", "down4_today"]]
        .sum()
        .sort_index()
        .copy()
    )

    daily_counts["up4_5d"] = daily_counts["up4_today"].rolling(5).sum()
    daily_counts["down4_5d"] = daily_counts["down4_today"].rolling(5).sum()
    daily_counts["up4_10d"] = daily_counts["up4_today"].rolling(10).sum()
    daily_counts["down4_10d"] = daily_counts["down4_today"].rolling(10).sum()

    daily_counts["ratio_5d"] = [
        safe_ratio(u, d)
        for u, d in zip(daily_counts["up4_5d"], daily_counts["down4_5d"])
    ]
    daily_counts["ratio_10d"] = [
        safe_ratio(u, d)
        for u, d in zip(daily_counts["up4_10d"], daily_counts["down4_10d"])
    ]

    ixic_map = {}
    if not ixic_df.empty:
        ixic_map = dict(zip(ixic_df["date"], ixic_df["nasdaq_close"]))

    indicator_keys = [
        "up4_today",
        "down4_today",
        "up25_quarter",
        "down25_quarter",
        "up25_month",
        "down25_month",
        "up50_month",
        "down50_month",
        "up13_34d",
        "down13_34d",
    ]

    unique_dates = sorted(df["date"].dropna().unique())
    if len(unique_dates) > MAX_HISTORY_ROWS:
        unique_dates = unique_dates[-MAX_HISTORY_ROWS:]

    rows = []

    for dt in unique_dates:
        day = df[df["date"] == dt].copy()

        lists = {}
        for key in indicator_keys:
            lists[key] = sorted(day.loc[day[key] == True, "symbol"].astype(str).unique().tolist())

        ratio_5d = daily_counts.at[dt, "ratio_5d"] if dt in daily_counts.index else None
        ratio_10d = daily_counts.at[dt, "ratio_10d"] if dt in daily_counts.index else None
        nasdaq_close = ixic_map.get(dt)

        row = {
            "date": pd.Timestamp(dt).strftime("%Y-%m-%d"),
            "nasdaq_close": None if pd.isna(nasdaq_close) else round(float(nasdaq_close), 2),
            "ratio_5d": ratio_to_json_value(ratio_5d),
            "ratio_10d": ratio_to_json_value(ratio_10d),
            "lists": lists
        }
        rows.append(row)

    rows.sort(key=lambda x: x["date"], reverse=True)
    return rows


def main():
    print("Downloading Nasdaq universe...")
    symbols = download_nasdaq_universe()
    print(f"Universe size: {len(symbols)}")

    print("Downloading stock histories...")
    stock_df = download_all_histories(symbols)

    print("Downloading Nasdaq Composite...")
    ixic_df = download_nasdaq_composite()

    rows = build_rows(stock_df, ixic_df)

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "rows": rows
    }

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    print(f"Wrote {len(rows)} rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
