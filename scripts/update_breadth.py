import json
import math
import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
import yfinance as yf

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"
OUTPUT_PATH = "data/breadth-history.json"

INDICATOR_BUFFER_DAYS = 120
PRICE_BATCH_SIZE = 120
REQUEST_SLEEP = 0.35

MIN_PRICE = 1.0
MIN_AVG_VOLUME = 100_000

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/plain,application/json,*/*"
})


def load_existing_data():
    if not os.path.exists(OUTPUT_PATH):
        print("No existing file found. Will do a full bootstrap.")
        return [], None

    with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
        payload = json.load(f)

    rows = payload.get("rows", [])
    if not rows:
        return [], None

    existing_dates = {r["date"] for r in rows}
    latest_date = max(existing_dates)
    print(f"Existing data found. {len(rows)} rows. Latest date: {latest_date}")
    return rows, latest_date


def download_nasdaq_universe():
    r = SESSION.get(NASDAQ_LISTED_URL, timeout=60)
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
        df = df[~df["Symbol"].str.contains("\\" + ch, regex=True, na=False)].copy()

    return df[["Symbol"]].copy()


def download_other_universe():
    r = SESSION.get(OTHER_LISTED_URL, timeout=60)
    r.raise_for_status()

    lines = r.text.strip().splitlines()
    header = lines[0].split("|")
    rows = [line.split("|") for line in lines[1:] if line and not line.startswith("File Creation Time")]
    df = pd.DataFrame(rows, columns=header)

    # Column names in otherlisted.txt
    df.columns = [c.strip() for c in df.columns]

    act_col = "ACT Symbol"
    if act_col not in df.columns:
        act_col = df.columns[0]

    df["Symbol"] = df[act_col].astype(str).str.strip().str.upper()
    df["ETF"] = df["ETF"].astype(str).str.strip().str.upper()
    df["Test Issue"] = df["Test Issue"].astype(str).str.strip().str.upper()

    # Only keep NYSE (N), NYSE ARCA (P), NYSE MKT/AMEX (A), BATS/CBOE (Z)
    df["Exchange"] = df["Exchange"].astype(str).str.strip().str.upper()
    df = df[df["Exchange"].isin(["N", "P", "A", "Z"])].copy()

    df = df[df["Test Issue"] == "N"].copy()
    df = df[df["ETF"] == "N"].copy()

    bad_chars = ["^", "$", "/"]
    for ch in bad_chars:
        df = df[~df["Symbol"].str.contains("\\" + ch, regex=True, na=False)].copy()

    return df[["Symbol"]].copy()


def filter_derivatives(df):
    symbol_set = set(df["Symbol"].tolist())

    def is_derivative(symbol):
        if len(symbol) < 2:
            return False
        # Warrants, Rights, Units where base symbol exists
        if symbol[-1] in ("W", "R", "U") and symbol[:-1] in symbol_set:
            return True
        # WS suffix (warrants)
        if len(symbol) >= 3 and symbol[-2:] == "WS" and symbol[:-2] in symbol_set:
            return True
        # Preferred shares — contain $ or - or end in P/A/B/C/D after a letter
        if "$" in symbol or "-" in symbol:
            return True
        return False

    before = len(df)
    df = df[~df["Symbol"].apply(is_derivative)].copy()
    after = len(df)
    print(f"Derivative filter removed {before - after} symbols (warrants/rights/units/preferred)")
    return df


def build_full_universe():
    print("Downloading Nasdaq universe...")
    nasdaq_df = download_nasdaq_universe()

    print("Downloading other exchange universe (NYSE, NYSE ARCA, AMEX, CBOE)...")
    other_df = download_other_universe()

    combined = pd.concat([nasdaq_df, other_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["Symbol"]).copy()
    combined = filter_derivatives(combined)

    symbols = combined["Symbol"].dropna().unique().tolist()
    symbols.sort()
    print(f"Total universe after filters: {len(symbols)} symbols")
    return symbols


def to_yf_symbol(symbol):
    return symbol.replace(".", "-").replace("/", "-")


def chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def extract_batch_history(batch_symbols, start_date, end_date):
    yf_symbols = [to_yf_symbol(s) for s in batch_symbols]
    raw_to_yf = dict(zip(batch_symbols, yf_symbols))
    yf_to_raw = {v: k for k, v in raw_to_yf.items()}

    data = yf.download(
        tickers=yf_symbols,
        start=start_date,
        end=end_date,
        interval="1d",
        auto_adjust=False,
        progress=False,
        group_by="ticker",
        threads=True,
        prepost=False
    )

    if data is None or data.empty:
        return pd.DataFrame(columns=["date", "symbol", "close", "adj_close", "volume"])

    frames = []

    if isinstance(data.columns, pd.MultiIndex):
        available = set(data.columns.get_level_values(0))

        for yf_symbol in yf_symbols:
            if yf_symbol not in available:
                continue

            sub = data[yf_symbol].copy().reset_index()
            if "Date" not in sub.columns:
                continue

            sub = sub.rename(columns={
                "Date": "date",
                "Close": "close",
                "Adj Close": "adj_close",
                "Volume": "volume"
            })

            keep_cols = [c for c in ["date", "close", "adj_close", "volume"] if c in sub.columns]
            sub = sub[keep_cols].copy()

            if "close" not in sub.columns:
                continue
            if "adj_close" not in sub.columns:
                sub["adj_close"] = sub["close"]
            if "volume" not in sub.columns:
                sub["volume"] = 0

            sub["symbol"] = yf_to_raw[yf_symbol]
            frames.append(sub)
    else:
        sub = data.reset_index().rename(columns={
            "Date": "date",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume"
        })

        keep_cols = [c for c in ["date", "close", "adj_close", "volume"] if c in sub.columns]
        sub = sub[keep_cols].copy()

        if "close" in sub.columns:
            if "adj_close" not in sub.columns:
                sub["adj_close"] = sub["close"]
            if "volume" not in sub.columns:
                sub["volume"] = 0
            sub["symbol"] = batch_symbols[0]
            frames.append(sub)

    if not frames:
        return pd.DataFrame(columns=["date", "symbol", "close", "adj_close", "volume"])

    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"]).dt.tz_localize(None)
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out["adj_close"] = pd.to_numeric(out["adj_close"], errors="coerce")
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce").fillna(0)
    out = out.dropna(subset=["date", "symbol", "close", "adj_close"]).copy()

    return out


def download_all_histories(symbols, start_date, end_date):
    frames = []
    total_batches = math.ceil(len(symbols) / PRICE_BATCH_SIZE)

    for i, batch in enumerate(chunked(symbols, PRICE_BATCH_SIZE)):
        try:
            batch_df = extract_batch_history(batch, start_date, end_date)
            if not batch_df.empty:
                frames.append(batch_df)
        except Exception as e:
            print(f"Price batch {i+1}/{total_batches} failed ({batch[:3]}...): {e}")

        if (i + 1) % 10 == 0:
            print(f"  Downloaded batch {i+1}/{total_batches}...")

        time.sleep(REQUEST_SLEEP)

    if not frames:
        return pd.DataFrame(columns=["date", "symbol", "close", "adj_close", "volume"])

    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values(["symbol", "date"]).drop_duplicates(["symbol", "date"], keep="last")
    return df


def apply_liquidity_filter(stock_df):
    if stock_df.empty:
        return stock_df

    stats = (
        stock_df.sort_values("date")
        .groupby("symbol")
        .tail(20)
        .groupby("symbol")
        .agg(
            last_close=("close", "last"),
            avg_volume=("volume", "mean")
        )
        .reset_index()
    )

    qualified = stats[
        (stats["last_close"] >= MIN_PRICE) &
        (stats["avg_volume"] >= MIN_AVG_VOLUME)
    ]["symbol"].tolist()

    removed = stock_df["symbol"].nunique() - len(qualified)
    print(f"Liquidity filter: kept {len(qualified)} symbols, removed {removed} (price < ${MIN_PRICE} or avg vol < {MIN_AVG_VOLUME:,})")

    return stock_df[stock_df["symbol"].isin(qualified)].copy()


def download_nasdaq_composite(start_date, end_date):
    data = yf.download(
        tickers="^IXIC",
        start=start_date,
        end=end_date,
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
        prepost=False
    )

    if data is None or data.empty:
        return pd.DataFrame(columns=["date", "nasdaq_close"])

    if isinstance(data.columns, pd.MultiIndex):
        if "Close" in data.columns.get_level_values(0):
            close = data["Close"]
        elif "Close" in data.columns.get_level_values(-1):
            close = data.xs("Close", axis=1, level=-1)
        else:
            return pd.DataFrame(columns=["date", "nasdaq_close"])
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
    else:
        if "Close" not in data.columns:
            return pd.DataFrame(columns=["date", "nasdaq_close"])
        close = data["Close"]

    out = close.rename("nasdaq_close").reset_index()

    if "Date" in out.columns:
        out = out.rename(columns={"Date": "date"})
    elif out.columns.tolist()[0] != "date":
        out.columns = ["date", "nasdaq_close"]

    out["date"] = pd.to_datetime(out["date"]).dt.tz_localize(None)
    out["nasdaq_close"] = pd.to_numeric(out["nasdaq_close"], errors="coerce")
    out = out.dropna(subset=["date", "nasdaq_close"]).copy()

    return out[["date", "nasdaq_close"]]


def ratio_to_json_value(value):
    if value is None or pd.isna(value):
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


def build_ranked_list(day_df, flag_col, ret_col):
    subset = day_df.loc[day_df[flag_col] == True, ["symbol", ret_col]].copy()

    if subset.empty:
        return []

    subset = subset.rename(columns={ret_col: "percent"})
    subset["symbol"] = subset["symbol"].astype(str)
    subset["percent"] = pd.to_numeric(subset["percent"], errors="coerce")
    subset = subset.dropna(subset=["symbol", "percent"]).copy()

    subset["percent"] = subset["percent"] * 100
    subset["abs_percent"] = subset["percent"].abs()

    subset = subset.sort_values(
        by=["abs_percent", "percent", "symbol"],
        ascending=[False, False, True]
    ).drop_duplicates(subset=["symbol"], keep="first")

    return [
        {
            "symbol": str(row.symbol),
            "percent": round(float(row.percent), 2)
        }
        for row in subset.itertuples(index=False)
    ]


def build_new_rows(stock_df, ixic_df, dates_to_build):
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

    rows = []

    for dt in sorted(dates_to_build):
        day = df[df["date"] == dt].copy()
        if day.empty:
            continue

        lists = {
            "up4_today": build_ranked_list(day, "up4_today", "ret_1d"),
            "down4_today": build_ranked_list(day, "down4_today", "ret_1d"),
            "up25_quarter": build_ranked_list(day, "up25_quarter", "ret_63d"),
            "down25_quarter": build_ranked_list(day, "down25_quarter", "ret_63d"),
            "up25_month": build_ranked_list(day, "up25_month", "ret_21d"),
            "down25_month": build_ranked_list(day, "down25_month", "ret_21d"),
            "up50_month": build_ranked_list(day, "up50_month", "ret_21d"),
            "down50_month": build_ranked_list(day, "down50_month", "ret_21d"),
            "up13_34d": build_ranked_list(day, "up13_34d", "ret_34d"),
            "down13_34d": build_ranked_list(day, "down13_34d", "ret_34d"),
        }

        ratio_5d = daily_counts.at[dt, "ratio_5d"] if dt in daily_counts.index else None
        ratio_10d = daily_counts.at[dt, "ratio_10d"] if dt in daily_counts.index else None
        nasdaq_close = ixic_map.get(dt)

        rows.append({
            "date": pd.Timestamp(dt).strftime("%Y-%m-%d"),
            "nasdaq_close": None if nasdaq_close is None or pd.isna(nasdaq_close) else round(float(nasdaq_close), 2),
            "ratio_5d": ratio_to_json_value(ratio_5d),
            "ratio_10d": ratio_to_json_value(ratio_10d),
            "lists": lists
        })

    return rows


def main():
    existing_rows, latest_date = load_existing_data()
    existing_dates = {r["date"] for r in existing_rows}

    today = datetime.now(timezone.utc).date()
    tomorrow = today + timedelta(days=1)

    if latest_date:
        fetch_start = (
            datetime.strptime(latest_date, "%Y-%m-%d").date()
            - timedelta(days=INDICATOR_BUFFER_DAYS)
        )
        print(f"Incremental mode: fetching from {fetch_start} to {tomorrow}")
    else:
        fetch_start = today - timedelta(days=548)
        print(f"Bootstrap mode: fetching from {fetch_start} to {tomorrow}")

    start_str = fetch_start.strftime("%Y-%m-%d")
    end_str = tomorrow.strftime("%Y-%m-%d")

    symbols = build_full_universe()

    print("Downloading stock histories...")
    stock_df = download_all_histories(symbols, start_str, end_str)

    print("Applying liquidity filter...")
    stock_df = apply_liquidity_filter(stock_df)

    print("Downloading Nasdaq Composite...")
    ixic_df = download_nasdaq_composite(start_str, end_str)

    available_dates = set(stock_df["date"].dt.strftime("%Y-%m-%d").unique()) if not stock_df.empty else set()
    dates_to_build = {
        pd.Timestamp(d)
        for d in available_dates
        if d not in existing_dates
    }

    if not dates_to_build:
        print("No new dates to add. Data is already up to date.")
        return

    print(f"Building {len(dates_to_build)} new row(s)...")
    new_rows = build_new_rows(stock_df, ixic_df, dates_to_build)

    all_rows = existing_rows + new_rows
    seen = set()
    merged = []
    for row in all_rows:
        if row["date"] not in seen:
            seen.add(row["date"])
            merged.append(row)

    merged.sort(key=lambda x: x["date"], reverse=True)

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "rows": merged
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    print(f"Done. Total rows: {len(merged)} (+{len(new_rows)} new)")


if __name__ == "__main__":
    main()

