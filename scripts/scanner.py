#!/usr/bin/env python3
"""
scripts/scanner.py
Dynamic swing trade screener — no hardcoded universe.
Pipeline:
  1. Pull high-volume US equities from Yahoo Finance screener endpoints
  2. Pre-filter: ADR > 5% AND dollar volume > $20M (fast, 20d data)
  3. Full multi-factor analysis on passing candidates

Scoring: Trend(25) + Momentum(20) + Volatility(15) + Volume(15) + RS_SPY(15) + PriceStructure(10) = 100
"""

import yfinance as yf
import pandas as pd
import numpy as np
import requests
import json
import os
import warnings
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings('ignore')

TODAY       = str(date.today())
DATA_DIR    = "data"
LATEST_FILE = f"{DATA_DIR}/screener_data.json"
DATED_FILE  = f"{DATA_DIR}/{TODAY}.json"
INDEX_FILE  = f"{DATA_DIR}/index.json"
ADR_MIN     = 5.0
VOL_MIN     = 20_000_000

YF_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1 — GET UNIVERSE FROM YAHOO FINANCE SCREENER
# ═══════════════════════════════════════════════════════════════════════════════

def get_yahoo_universe():
    """
    Pull candidates from Yahoo Finance predefined screeners.
    Only keeps EQUITY (common stock) and ETF quote types.
    Drops warrants, rights, units, preferred, foreign listings.
    """
    base      = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
    screeners = [
        'most_actives',
        'day_gainers',
        'day_losers',
        'growth_technology_stocks',
        'small_cap_gainers',
        'aggressive_small_caps',
    ]

    tickers = set()

    for scr in screeners:
        try:
            res = requests.get(
                base,
                headers=YF_HEADERS,
                params={'scrIds': scr, 'count': 250},
                timeout=15,
            )
            res.raise_for_status()
            data   = res.json()
            quotes = data['finance']['result'][0]['quotes']
            before = len(tickers)

            for q in quotes:
                sym    = q.get('symbol', '')
                q_type = q.get('quoteType', '')

                if (
                    sym
                    and q_type in ('EQUITY', 'ETF')
                    and '.' not in sym
                    and '^' not in sym
                    and '-' not in sym
                    and '+' not in sym
                    and not sym.endswith('W')
                    and not sym.endswith('R')
                    and not sym.endswith('U')
                    and len(sym) <= 5
                ):
                    tickers.add(sym)

            print(f"  {scr:<35} +{len(tickers) - before:>3}  (total {len(tickers)})")
        except Exception as e:
            print(f"  {scr:<35} FAILED — {e}")

    return sorted(tickers)


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2 — PRE-FILTER BY ADR AND DOLLAR VOLUME
# ═══════════════════════════════════════════════════════════════════════════════

def pre_filter(tickers, adr_min, vol_min, max_workers=20):
    """
    Fast pre-filter using only 20 days of data.
    Only tickers passing BOTH criteria proceed to full analysis.
    """

    def check(ticker):
        try:
            df = yf.Ticker(ticker).history(period="25d", auto_adjust=True)
            if len(df) < 10:
                return None
            adr    = ((df['High'] - df['Low']) / df['Close'] * 100).mean()
            dolvol = (df['Close'] * df['Volume']).tail(10).mean()
            if adr >= adr_min and float(dolvol) >= vol_min:
                return ticker
        except Exception:
            pass
        return None

    passing = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(check, t): t for t in tickers}
        for fut in as_completed(futures):
            result = fut.result()
            if result:
                passing.append(result)

    return sorted(passing)


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3 — FULL MULTI-FACTOR ANALYZER
# ═══════════════════════════════════════════════════════════════════════════════

class SwingTradeAnalyzer:

    # ── DATA ──────────────────────────────────────────────────────────────────
    def fetch_data(self, ticker):
        try:
            df = yf.Ticker(ticker).history(period="2y", auto_adjust=True)
            if len(df) < 60:
                return None
            df.index = pd.to_datetime(df.index)
            return df
        except Exception:
            return None

    def calculate_indicators(self, df):
        c, h, l = df['Close'], df['High'], df['Low']

        for n in [10, 20, 50, 200]:
            df[f'SMA{n}'] = c.rolling(n).mean()

        # RSI (14)
        delta         = c.diff()
        gain          = delta.clip(lower=0).rolling(14).mean()
        loss          = (-delta.clip(upper=0)).rolling(14).mean()
        df['RSI']     = 100 - (100 / (1 + gain / loss))

        # MACD (12/26/9)
        ema12             = c.ewm(span=12, adjust=False).mean()
        ema26             = c.ewm(span=26, adjust=False).mean()
        df['MACD']        = ema12 - ema26
        df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
        df['MACD_Hist']   = df['MACD'] - df['MACD_Signal']

        # Bollinger Bands (20, 2σ)
        bb_mid         = c.rolling(20).mean()
        bb_std         = c.rolling(20).std()
        df['BB_Upper'] = bb_mid + 2 * bb_std
        df['BB_Lower'] = bb_mid - 2 * bb_std
        df['BB_Width'] = (df['BB_Upper'] - df['BB_Lower']) / bb_mid

        # ATR (14)
        tr            = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
        df['ATR']     = tr.rolling(14).mean()
        df['ATR_Pct'] = df['ATR'] / c * 100

        df['Vol_MA20'] = df['Volume'].rolling(20).mean()
        return df

    def compute_adr_and_dolvol(self, df):
        adr    = ((df['High'] - df['Low']) / df['Close'] * 100).tail(20).mean()
        dolvol = (df['Close'] * df['Volume']).tail(10).mean()
        return round(float(adr), 2), round(float(dolvol), 0)

    # ── TREND (25 pts) ────────────────────────────────────────────────────────
    # Sub-scores: Price vs MAs (8) + MA Alignment & Slopes (7) + MA Surf (7) + Reclaim (3)
    def analyze_trend(self, df):
        row    = df.dropna(subset=['SMA10', 'SMA20', 'SMA50']).iloc[-1]
        price  = float(row['Close'])
        sma10  = float(row['SMA10'])
        sma20  = float(row['SMA20'])
        sma50  = float(row['SMA50'])
        sma200 = float(row['SMA200']) if not pd.isna(row.get('SMA200', float('nan'))) else None

        score, conditions = 0, []

        # Price vs MAs (0-8)
        checks  = [price > sma10, price > sma20, price > sma50]
        if sma200: checks.append(price > sma200)
        n_above, total = sum(checks), len(checks)

        if n_above == total:
            score += 8
            conditions.append(f"Price is above all {total} key SMAs — strong bullish positioning across all timeframes")
        elif n_above >= total - 1:
            score += 6
            conditions.append(f"Price is above {n_above}/{total} SMAs — mostly bullish with minor lag on one average")
        elif n_above == 2:
            score += 4
            conditions.append(f"Price is above {n_above}/{total} SMAs — mixed trend, no clear directional bias")
        elif n_above == 1:
            score += 2
            conditions.append(f"Price is above only {n_above}/{total} SMAs — below most key levels")
        else:
            score += 0
            conditions.append("Price is below all key SMAs — bearish or early-stage base building")

        # MA Alignment & Slopes (0-7)
        stacked = sma10 > sma20 > sma50
        if sma200: stacked = stacked and sma50 > sma200

        def slope(col, n):
            s = df[col].dropna()
            return float((s.iloc[-1] - s.iloc[-n]) / s.iloc[-n] * 100) if len(s) >= n else 0.0

        slopes_up = slope('SMA10', 5) > 0 and slope('SMA20', 5) > 0 and slope('SMA50', 10) > 0

        if stacked and slopes_up:
            score += 7
            conditions.append("Full bullish MA stack (10>20>50>200) with all averages sloping upward — ideal trend alignment")
        elif stacked or (sma10 > sma20 > sma50 and slopes_up):
            score += 5
            conditions.append("MAs show bullish stacking with rising slopes — constructive trend structure")
        elif sma10 > sma20 > sma50:
            score += 4
            conditions.append("Short-term MAs (10>20>50) stacked bullishly but longer-term alignment is mixed")
        elif slopes_up:
            score += 3
            conditions.append("MAs not fully stacked but all rising — trend turning, not yet ordered")
        else:
            score += 1
            conditions.append("MAs not bullishly stacked or sloping — trend structure is mixed or bearish")

        # MA Surf: proximity to 10 & 20 SMA (0-7)
        pct10 = (price - sma10) / sma10 * 100
        pct20 = (price - sma20) / sma20 * 100

        if 0 <= pct10 <= 3:       s10 = 4
        elif 0 <= pct10 <= 7:     s10 = 3
        elif 0 <= pct10 <= 15:    s10 = 2
        elif pct10 > 15:          s10 = 1
        else:                      s10 = 0

        if 0 <= pct20 <= 5:       s20 = 3
        elif 0 <= pct20 <= 12:    s20 = 2
        elif 0 <= pct20 <= 25:    s20 = 1
        else:                      s20 = 0

        score += s10 + s20
        conditions.append(f"10 SMA surf: {pct10:+.1f}% → {s10}/4 pts | 20 SMA surf: {pct20:+.1f}% → {s20}/3 pts")

        # MA Reclaim (0-3)
        c_ser = df['Close']

        def reclaimed(pr, ma, lb=5):
            for i in range(1, min(lb, len(pr) - 1)):
                if (not pd.isna(ma.iloc[-i]) and
                    pr.iloc[-i]     > ma.iloc[-i] and
                    pr.iloc[-(i+1)] <= ma.iloc[-(i+1)]):
                    return True
            return False

        if reclaimed(c_ser, df['SMA50']):
            score += 3
            conditions.append("Price recently reclaimed the 50-day SMA — high-quality trend resumption signal")
        elif reclaimed(c_ser, df['SMA20']):
            score += 2
            conditions.append("Price recently reclaimed the 20-day SMA — short-term trend structure improving")
        elif price > sma50:
            score += 1
            conditions.append("Price holding above the 50-day SMA — trend intact, no fresh reclaim event")
        else:
            score += 0
            conditions.append("No recent MA reclaim and price is below 50 SMA — trend not confirmed")

        pct    = score / 25
        status = "SUPPORTIVE" if pct >= 0.65 else "NEUTRAL" if pct >= 0.35 else "UNSUPPORTIVE"
        return {'score': min(25, score), 'max': 25, 'status': status, 'conditions': conditions}

    # ── MOMENTUM (20 pts) ─────────────────────────────────────────────────────
    # Sub-scores: RSI Zone (7) + RSI Trajectory (6) + MACD (7)
    def analyze_momentum(self, df):
        clean  = df.dropna(subset=['RSI', 'MACD', 'MACD_Signal', 'MACD_Hist'])
        row    = clean.iloc[-1]
        prev   = clean.iloc[-2] if len(clean) >= 2 else row
        rsi    = float(row['RSI'])
        rsi_s  = df['RSI'].dropna()
        rsi5a  = float(rsi_s.iloc[-6]) if len(rsi_s) >= 6 else rsi
        rsi_d  = rsi - rsi5a
        macd   = float(row['MACD'])
        sig    = float(row['MACD_Signal'])
        hist   = float(row['MACD_Hist'])
        hist_p = float(prev['MACD_Hist'])
        hist5  = float(df['MACD_Hist'].dropna().iloc[-5]) if len(df['MACD_Hist'].dropna()) >= 5 else hist

        score, conditions = 0, []

        # RSI Zone (0-7)
        if 55 <= rsi <= 75:
            score += 7
            conditions.append(f"RSI at {rsi:.1f} — bullish momentum zone (55–75), ideal for swing continuation")
        elif 45 <= rsi < 55:
            score += 5
            conditions.append(f"RSI at {rsi:.1f} — approaching bullish zone, momentum building from neutral")
        elif rsi > 75:
            score += 4
            conditions.append(f"RSI at {rsi:.1f} — overbought; strong momentum but pullback risk elevated")
        elif 30 <= rsi < 45:
            score += 2
            conditions.append(f"RSI at {rsi:.1f} — weak momentum, recovery not yet confirmed")
        else:
            score += 1
            conditions.append(f"RSI at {rsi:.1f} — oversold; potential reversal candidate but trend still bearish")

        # RSI Trajectory (0-6)
        cross50 = any(
            rsi_s.iloc[i] > 50 and rsi_s.iloc[i-1] <= 50
            for i in range(max(-6, -len(rsi_s)+1), 0)
        )
        if cross50 and rsi_d > 0:
            score += 6
            conditions.append(f"RSI recently crossed above 50 with upward trajectory ({rsi_d:+.1f} pts over 5d) — ignition signal")
        elif rsi > 50 and rsi_d > 2:
            score += 5
            conditions.append(f"RSI rising {rsi5a:.1f} → {rsi:.1f} above midline — momentum accelerating")
        elif rsi > 50 and rsi_d > 0:
            score += 4
            conditions.append(f"RSI above 50 and gently rising — bullish bias, not strongly accelerating yet")
        elif rsi > 50 and rsi_d < 0:
            score += 2
            conditions.append(f"RSI above 50 but declining — momentum fading, watch for reset to midline")
        else:
            score += 1
            conditions.append(f"RSI below 50 with no recovery signal — bearish momentum bias persists")

        # MACD (0-7)
        mc  = df['MACD'].dropna()
        sc2 = df['MACD_Signal'].dropna()
        rbc = any(
            mc.iloc[i] > sc2.iloc[i] and mc.iloc[i-1] <= sc2.iloc[i-1]
            for i in range(max(-6, -min(len(mc), len(sc2))+1), 0)
        )
        hist_exp        = abs(hist) > abs(hist_p) and hist > 0
        hist_neg_shrink = hist < 0 and abs(hist) < abs(hist5)

        if rbc and macd > 0 and hist_exp:
            score += 7
            conditions.append("MACD: Fresh bull cross above signal with expanding histogram above zero — strong confirmation")
        elif rbc and hist_exp:
            score += 6
            conditions.append("MACD: Recent bullish crossover with expanding histogram — momentum turning positive")
        elif macd > sig and hist_exp and macd > 0:
            score += 6
            conditions.append("MACD: Above signal with expanding positive histogram — momentum building above zero")
        elif macd > sig and macd > 0:
            score += 4
            conditions.append("MACD: Above signal in positive territory, histogram tightening — momentum stabilizing")
        elif macd > sig and macd <= 0:
            score += 3
            conditions.append("MACD: Crossed above signal but still below zero — early bullish cross, unconfirmed")
        elif hist_neg_shrink:
            score += 2
            conditions.append("MACD: Negative histogram contracting — bearish momentum decelerating, watch for crossover")
        else:
            score += 0
            conditions.append("MACD: Below signal with expanding negative histogram — bearish momentum dominant")

        pct    = score / 20
        status = "SUPPORTIVE" if pct >= 0.65 else "NEUTRAL" if pct >= 0.35 else "UNSUPPORTIVE"
        return {'score': min(20, score), 'max': 20, 'status': status, 'conditions': conditions}

    # ── VOLATILITY (15 pts) ───────────────────────────────────────────────────
    # Sub-scores: BB Width (6) + Range Compression (5) + Tight Days (4)
    def analyze_volatility(self, df):
        clean = df.dropna(subset=['BB_Width', 'ATR'])
        score, conditions = 0, []

        bb_now   = float(clean['BB_Width'].iloc[-1])
        bb_60d   = float(clean['BB_Width'].tail(60).mean())
        bb_ratio = bb_now / bb_60d if bb_60d > 0 else 1.0

        rng   = (df['High'] - df['Low']).tail(20) / df['Close'].tail(20) * 100
        r5    = float(rng.tail(5).mean())
        r20   = float(rng.mean())
        rng_r = r5 / r20 if r20 > 0 else 1.0

        # BB Width Compression (0-6)
        if bb_ratio < 0.55:
            score += 6
            conditions.append(f"Bollinger Bands severely compressed ({bb_ratio:.0%} of 60d avg) — mature squeeze, high expansion potential")
        elif bb_ratio < 0.72:
            score += 5
            conditions.append(f"Bollinger Bands notably tight ({bb_ratio:.0%} of 60d avg) — significant volatility compression")
        elif bb_ratio < 0.88:
            score += 3
            conditions.append(f"Bollinger Bands slightly below avg ({bb_ratio:.0%}) — mild compression developing")
        elif bb_ratio < 1.10:
            score += 2
            conditions.append(f"Bollinger Band width near historical average ({bb_ratio:.0%}) — no squeeze present")
        else:
            score += 0
            conditions.append(f"Bollinger Bands expanded ({bb_ratio:.0%} of 60d avg) — volatility releasing, not compressing")

        # Range Compression (0-5)
        if rng_r < 0.58:
            score += 5
            conditions.append(f"Daily ranges sharply compressed (recent {r5:.1f}% vs 20d avg {r20:.1f}%) — stock in a tight coil")
        elif rng_r < 0.75:
            score += 4
            conditions.append(f"Price ranges meaningfully narrower than history ({r5:.1f}% vs {r20:.1f}%) — tightening into a setup")
        elif rng_r < 0.90:
            score += 2
            conditions.append(f"Price ranges slightly below average ({r5:.1f}% vs {r20:.1f}%) — modest compression developing")
        else:
            score += 1
            conditions.append(f"Daily ranges at or above 20d average ({r5:.1f}% vs {r20:.1f}%) — no meaningful compression")

        # Tight Days Counter (0-4) — sessions where H-L < ATR14
        tight_count = 0
        for i in range(-10, 0):
            try:
                if (float(df['High'].iloc[i]) - float(df['Low'].iloc[i])) < float(clean['ATR'].iloc[i]):
                    tight_count += 1
            except (IndexError, ValueError):
                pass

        score += min(4, round(tight_count * 4 / 10))
        conditions.append(
            f"{tight_count}/10 sessions had ranges below ATR — "
            + ("exceptional coiling" if tight_count >= 8
               else "strong compression, inside-ish days stacking" if tight_count >= 6
               else "moderate tightening underway" if tight_count >= 4
               else "early signs of compression" if tight_count >= 2
               else "no meaningful compression recently")
        )

        pct    = score / 15
        status = "SUPPORTIVE" if pct >= 0.65 else "NEUTRAL" if pct >= 0.35 else "UNSUPPORTIVE"
        return {'score': min(15, score), 'max': 15, 'status': status, 'conditions': conditions}

    # ── VOLUME (15 pts) ───────────────────────────────────────────────────────
    # Sub-scores: Accum/Dist (6) + Volume Spikes (4) + Dry-Up (3) + Advances (2)
    def analyze_volume(self, df):
        clean    = df.dropna(subset=['Vol_MA20'])
        recent   = clean.tail(20)
        vol_ma20 = float(clean['Vol_MA20'].iloc[-1])
        score, conditions = 0, []

        # Accumulation/Distribution (0-6)
        up = recent[recent['Close'] > recent['Open']]
        dn = recent[recent['Close'] <= recent['Open']]
        uv = float(up['Volume'].mean()) if len(up) > 0 else 0
        dv = float(dn['Volume'].mean()) if len(dn) > 0 else 1

        if uv > 0 and dv > 0:
            acc = uv / dv
            if acc > 1.6:
                score += 6
                conditions.append(f"Up-day volume {acc:.1f}x higher than down-day volume — clear institutional accumulation signature")
            elif acc > 1.25:
                score += 4
                conditions.append(f"Up-day volume moderately exceeds down-day ({acc:.1f}x) — mild accumulation bias")
            elif acc > 0.85:
                score += 2
                conditions.append(f"Up/down volume near parity ({acc:.1f}x) — neutral, no clear accumulation signal")
            else:
                score += 0
                conditions.append(f"Down-day volume exceeds up-day ({acc:.1f}x) — potential distribution pattern")
        else:
            score += 2
            conditions.append("Insufficient data to determine accumulation/distribution pattern")

        # Unusual Volume Spikes (0-4)
        r5     = clean.tail(5)
        spikes = r5['Volume'] / vol_ma20
        mx_spk = float(spikes.max())

        if mx_spk > 2.5:
            idx   = spikes.idxmax()
            is_up = float(clean.loc[idx, 'Close']) > float(clean.loc[idx, 'Open'])
            if is_up:
                score += 4
                conditions.append(f"Volume spike {mx_spk:.1f}x avg on an up-day (last 5 sessions) — potential institutional buy program")
            else:
                score += 0
                conditions.append(f"Volume spike {mx_spk:.1f}x avg on a down-day — possible distribution or stop-hunt")
        elif mx_spk > 1.5:
            score += 2
            conditions.append(f"Moderate volume pickup ({mx_spk:.1f}x avg) in recent sessions — notable but not extraordinary")
        else:
            score += 1
            conditions.append(f"No unusual volume spikes in last 5 sessions — volume quiet near average")

        # Volume Dry-Up During Consolidation (0-3)
        p5v  = float(clean['Volume'].tail(10).head(5).mean())
        r5v  = float(clean['Volume'].tail(5).mean())
        vr   = r5v / vol_ma20
        vchg = (r5v / p5v - 1) * 100 if p5v > 0 else 0
        prng = float(
            (clean['Close'].tail(5).max() - clean['Close'].tail(5).min())
            / clean['Close'].tail(5).mean() * 100
        )

        if prng < 3.0 and r5v < p5v * 0.80:
            score += 3
            conditions.append(f"Volume drying up ({vchg:.0f}% vs prior week) while price coils in {prng:.1f}% range — textbook pre-breakout setup")
        elif vr < 0.65:
            score += 2
            conditions.append(f"Volume well below 20d avg ({vr:.2f}x) — low-volume consolidation, constructive")
        elif vr < 0.85:
            score += 1
            conditions.append(f"Volume modestly below average ({vr:.2f}x) — mild dry-up")
        else:
            score += 0
            conditions.append(f"Volume elevated ({vr:.2f}x avg) — watch price direction for context")

        # Volume on Advances (0-2)
        adv = clean[clean['Close'] > clean['Open']].tail(3)
        if len(adv) > 0:
            avr = float((adv['Volume'] / vol_ma20).mean())
            if avr > 1.5:
                score += 2
                conditions.append(f"Recent bullish candles backed by {avr:.1f}x avg volume — buying demand confirmed")
            elif avr > 1.0:
                score += 1
                conditions.append(f"Recent advances on average-to-above volume ({avr:.1f}x) — buying present but not exceptional")
            else:
                score += 0
                conditions.append(f"Recent advances on below-average volume ({avr:.1f}x) — weak buying conviction")

        pct    = score / 15
        status = "SUPPORTIVE" if pct >= 0.65 else "NEUTRAL" if pct >= 0.35 else "UNSUPPORTIVE"
        return {'score': min(15, score), 'max': 15, 'status': status, 'conditions': conditions}

    # ── RELATIVE STRENGTH vs SPY (15 pts) ─────────────────────────────────────
    def analyze_rs_spy(self, df, spy_df):
        conditions = []
        try:
            c    = float(df['Close'].iloc[-1])
            c63  = float(df['Close'].iloc[-63])  if len(df) >= 63  else None
            c126 = float(df['Close'].iloc[-126]) if len(df) >= 126 else None
            c189 = float(df['Close'].iloc[-189]) if len(df) >= 189 else None
            c252 = float(df['Close'].iloc[-252]) if len(df) >= 252 else None

            if not all([c63, c126, c189, c252]):
                return {'score': 7, 'max': 15, 'status': 'NEUTRAL',
                        'conditions': ['Insufficient history for RS calculation — defaulting to neutral']}

            stock_rs = (2 * c/c63) + (c/c126) + (c/c189) + (c/c252)

            if spy_df is not None and len(spy_df) >= 252:
                sc    = float(spy_df['Close'].iloc[-1])
                sc63  = float(spy_df['Close'].iloc[-63])
                sc126 = float(spy_df['Close'].iloc[-126])
                sc189 = float(spy_df['Close'].iloc[-189])
                sc252 = float(spy_df['Close'].iloc[-252])
                spy_rs    = (2 * sc/sc63) + (sc/sc126) + (sc/sc189) + (sc/sc252)
                perf_diff = (c/c252 - sc/sc252) * 100
            else:
                spy_rs    = 5.0
                perf_diff = 0.0

            diff       = stock_rs - spy_rs
            base_score = max(0, min(15, round(7.5 + diff * 12)))

            if diff > 0.4:
                cond, status = f"IBD RS: {perf_diff:+.1f}% vs SPY (12mo weighted) — significant outperformance, institutional sponsorship likely", "SUPPORTIVE"
            elif diff > 0.1:
                cond, status = f"IBD RS: {perf_diff:+.1f}% vs SPY (12mo weighted) — modest outperformance vs benchmark", "SUPPORTIVE"
            elif diff > -0.1:
                cond, status = f"IBD RS: {perf_diff:+.1f}% vs SPY (12mo weighted) — roughly in-line with market", "NEUTRAL"
            elif diff > -0.4:
                cond, status = f"IBD RS: {perf_diff:+.1f}% vs SPY (12mo weighted) — modestly underperforming the market", "NEUTRAL"
            else:
                cond, status = f"IBD RS: {perf_diff:+.1f}% vs SPY (12mo weighted) — significantly underperforming, weak relative strength", "UNSUPPORTIVE"

            conditions.append(cond)

        except Exception:
            return {'score': 5, 'max': 15, 'status': 'NEUTRAL',
                    'conditions': ['RS calculation error — defaulting to neutral']}

        return {'score': base_score, 'max': 15, 'status': status, 'conditions': conditions}

    # ── PRICE STRUCTURE (10 pts) ───────────────────────────────────────────────
    # Linear decay from 52-week high. >35% drawdown = 0 pts.
    def analyze_price_structure(self, df):
        price    = float(df['Close'].iloc[-1])
        high252  = float(df['Close'].tail(252).max())
        dist_pct = (price / high252 - 1) * 100
        conditions = []

        if dist_pct >= 0:
            score, status = 10, "SUPPORTIVE"
            conditions.append(f"Price in discovery (+{dist_pct:.1f}% above 52w high) — no overhead resistance, blue sky territory")
            conditions.append("Every holder from the past year is in profit — pure demand-driven price action")
        elif dist_pct >= -35:
            score  = max(0, round(10 * (1 + dist_pct / 100)))
            status = "SUPPORTIVE" if dist_pct >= -15 else "NEUTRAL" if dist_pct >= -25 else "UNSUPPORTIVE"
            conditions.append(f"{abs(dist_pct):.1f}% below 52w high — linear decay score: {score}/10")
            conditions.append(
                "Near prior highs, minimal overhead supply — strong breakout candidate" if dist_pct >= -10 else
                "Moderate overhead supply — needs sustained buying pressure to break through" if dist_pct >= -20 else
                "Significant overhead supply — recovery requires substantial institutional demand"
            )
        else:
            score, status = 0, "UNSUPPORTIVE"
            conditions.append(f"{abs(dist_pct):.1f}% below 52w high — score zeroed beyond 35% drawdown")
            conditions.append("Excessive overhead supply makes a near-term breakout unlikely from this depth")

        return {'score': score, 'max': 10, 'status': status, 'conditions': conditions}

    # ── RATE ONE STOCK ────────────────────────────────────────────────────────
    def rate_stock(self, ticker, spy_df=None):
        df = self.fetch_data(ticker)
        if df is None:
            return None
        df = self.calculate_indicators(df)

        adr_pct, dollar_volume = self.compute_adr_and_dolvol(df)

        # Hard enforce filters again on full 2y data
        if adr_pct < ADR_MIN or dollar_volume < VOL_MIN:
            return None

        trend           = self.analyze_trend(df)
        momentum        = self.analyze_momentum(df)
        volatility      = self.analyze_volatility(df)
        volume          = self.analyze_volume(df)
        rs_spy          = self.analyze_rs_spy(df, spy_df)
        price_structure = self.analyze_price_structure(df)

        total = (trend['score'] + momentum['score'] + volatility['score'] +
                 volume['score'] + rs_spy['score'] + price_structure['score'])

        if   total >= 85: grade, verdict = "A+", "PRIME SETUP — Multiple factors aligned for a potential supernova move"
        elif total >= 75: grade, verdict = "A",  "STRONG SETUP — High-probability swing candidate with strong confluence"
        elif total >= 65: grade, verdict = "B",  "DEVELOPING SETUP — Several factors supportive, watch for confirmation"
        elif total >= 50: grade, verdict = "C",  "MIXED SETUP — Some potential but lacks full factor alignment"
        elif total >= 35: grade, verdict = "D",  "WEAK SETUP — Most factors are neutral or unsupportive"
        else:             grade, verdict = "F",  "NO SETUP — Conditions are unfavorable for swing entry"

        try:
            name = yf.Ticker(ticker).info.get('shortName', ticker)
        except Exception:
            name = ticker

        return {
            'ticker':          ticker,
            'name':            name,
            'price':           round(float(df['Close'].iloc[-1]), 2),
            'total':           total,
            'grade':           grade,
            'verdict':         verdict,
            'adr_pct':         adr_pct,
            'dollar_volume':   dollar_volume,
            'trend':           trend,
            'momentum':        momentum,
            'volatility':      volatility,
            'volume':          volume,
            'rs_spy':          rs_spy,
            'price_structure': price_structure,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# INDEX
# ═══════════════════════════════════════════════════════════════════════════════

def update_index(new_date):
    if os.path.exists(INDEX_FILE):
        with open(INDEX_FILE) as f:
            idx = json.load(f)
    else:
        idx = {'dates': []}

    idx['dates']  = sorted(set(idx['dates'] + [new_date]), reverse=True)
    idx['latest'] = idx['dates'][0]

    with open(INDEX_FILE, 'w') as f:
        json.dump(idx, f)

    print(f"Index updated — {len(idx['dates'])} scan day(s) on record")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print(f"{'='*60}")
    print(f"  SwingScan  |  {TODAY}  |  ADR>{ADR_MIN}%  Vol>${VOL_MIN/1e6:.0f}M")
    print(f"  Scoring: Trend(25) + Momentum(20) + Volatility(15)")
    print(f"           + Volume(15) + RS_SPY(15) + PriceStruct(10) = 100")
    print(f"{'='*60}\n")

    # ── Step 1 ──
    print("STEP 1 — Fetching universe from Yahoo Finance screeners...")
    universe = get_yahoo_universe()
    print(f"  → {len(universe)} candidate tickers found\n")

    if not universe:
        print("No universe returned — aborting. Yahoo Finance may be rate-limiting.")
        return

    # ── Step 2 ──
    print(f"STEP 2 — Pre-filtering {len(universe)} tickers (ADR + DolVol, 20d data)...")
    candidates = pre_filter(universe, ADR_MIN, VOL_MIN, max_workers=20)
    print(f"  → {len(candidates)} tickers passed pre-filter\n")

    if not candidates:
        print("No candidates passed pre-filter.")
        return

    print(f"  Candidates: {', '.join(candidates)}\n")

    # ── Fetch SPY once — shared across all scoring threads ──
    print("Fetching SPY data for RS calculations...", end=' ', flush=True)
    try:
        spy_df = yf.Ticker('SPY').history(period='2y', auto_adjust=True)
        spy_df = spy_df if len(spy_df) >= 60 else None
        print(f"done ({len(spy_df)} sessions).\n" if spy_df is not None else "FAILED — RS will default to neutral.\n")
    except Exception:
        spy_df = None
        print("FAILED — RS will default to neutral.\n")

    # ── Step 3 ──
    print(f"STEP 3 — Full analysis on {len(candidates)} candidates...")
    analyzer = SwingTradeAnalyzer()
    results  = []

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(analyzer.rate_stock, t, spy_df): t for t in candidates}
        for i, fut in enumerate(as_completed(futures), 1):
            t = futures[fut]
            try:
                r = fut.result()
                if r:
                    results.append(r)
                    print(
                        f"  [{i:>2}/{len(candidates)}] PASS  {t:<6}  "
                        f"{r['total']:>3}/100 [{r['grade']:<2}]  "
                        f"ADR {r['adr_pct']:.1f}%  "
                        f"T:{r['trend']['score']} M:{r['momentum']['score']} "
                        f"V:{r['volatility']['score']} Vol:{r['volume']['score']} "
                        f"RS:{r['rs_spy']['score']} PS:{r['price_structure']['score']}"
                    )
                else:
                    print(f"  [{i:>2}/{len(candidates)}] skip  {t}  (filtered on 2y re-check)")
            except Exception as e:
                print(f"  [{i:>2}/{len(candidates)}] ERR   {t}  —  {e}")

    results.sort(key=lambda x: x['total'], reverse=True)

    payload = {
        'date':       TODAY,
        'count':      len(results),
        'universe':   len(universe),
        'candidates': len(candidates),
        'filters':    {'adr_min': ADR_MIN, 'vol_min': VOL_MIN},
        'results':    results,
    }

    os.makedirs(DATA_DIR, exist_ok=True)

    with open(DATED_FILE, 'w') as f:
        json.dump(payload, f)

    with open(LATEST_FILE, 'w') as f:
        json.dump(payload, f)

    update_index(TODAY)

    print(f"\n{'='*60}")
    print(f"  Done — {len(results)} stocks passed all filters")
    print(f"  Scanned {len(universe)} → pre-filtered to {len(candidates)} → scored {len(results)}")
    print(f"  Saved to {DATED_FILE}")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
