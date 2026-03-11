import json
import os
import time

import yfinance as yf
import requests
import pandas as pd

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"
OUTPUT_PATH = "data/sector-map.json"
SLEEP = 0.5

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/plain,application/json,*/*"
})


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

    df.columns = [c.strip() for c in df.columns]

    act_col = "ACT Symbol"
    if act_col not in df.columns:
        act_col = df.columns[0]

    df["Symbol"] = df[act_col].astype(str).str.strip().str.upper()
    df["ETF"] = df["ETF"].astype(str).str.strip().str.upper()
    df["Test Issue"] = df["Test Issue"].astype(str).str.strip().str.upper()
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
        if symbol[-1] in ("W", "R", "U") and symbol[:-1] in symbol_set:
            return True
        if len(symbol) >= 3 and symbol[-2:] == "WS" and symbol[:-2] in symbol_set:
            return True
        if "$" in symbol or "-" in symbol:
            return True
        return False

    before = len(df)
    df = df[~df["Symbol"].apply(is_derivative)].copy()
    after = len(df)
    print(f"Derivative filter removed {before - after} symbols")
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
    print(f"Total universe: {len(symbols)} symbols")
    return symbols


def load_existing_map():
    if not os.path.exists(OUTPUT_PATH):
        return {}
    with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    symbols = build_full_universe()

    existing = load_existing_map()
    to_fetch = [s for s in symbols if s not in existing]
    print(f"Already cached: {len(existing)} | To fetch: {len(to_fetch)}")

    sector_map = dict(existing)
    total = len(to_fetch)

    for i, symbol in enumerate(to_fetch):
        try:
            info = yf.Ticker(symbol.replace(".", "-")).info
            sector = info.get("sector") or ""
            industry = info.get("industry") or ""
            sector_map[symbol] = {
                "sector": sector,
                "industry": industry
            }
        except Exception as e:
            print(f"  [{i+1}/{total}] {symbol} failed: {e}")
            sector_map[symbol] = {"sector": "", "industry": ""}

        if (i + 1) % 50 == 0:
            print(f"  Progress: {i+1}/{total}")
            os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
            with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
                json.dump(sector_map, f, ensure_ascii=False, separators=(",", ":"))

        time.sleep(SLEEP)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(sector_map, f, ensure_ascii=False, separators=(",", ":"))

    print(f"Done. Wrote {len(sector_map)} entries to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
