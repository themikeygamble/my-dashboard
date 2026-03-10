import json
import os
import time

import yfinance as yf
import requests
import pandas as pd

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
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
        df = df[~df["Symbol"].str.contains("\\" + ch, regex=True, na=False)]

    return df["Symbol"].dropna().unique().tolist()


def load_existing_map():
    if not os.path.exists(OUTPUT_PATH):
        return {}
    with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    print("Downloading Nasdaq universe...")
    symbols = download_nasdaq_universe()
    print(f"Total symbols: {len(symbols)}")

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
