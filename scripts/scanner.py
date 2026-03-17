#!/usr/bin/env python3
"""
scripts/scanner.py
Qullamaggie Breakout Screener — dynamic universe, no hardcoded tickers.
Pipeline:
  1. Pull high-volume US equities from Yahoo Finance screener endpoints
  2. Pre-filter: ADR > 5% AND dollar volume > $20M (fast, 20d data)
  3. Full Qullamaggie 6-factor breakout analysis on passing candidates

Scoring: PriorMove(25) + Consolidation(20) + MASurf(15) + BreakoutReadiness(15)
         + VolumeSignature(15) + RelativeStrength(10) = 100
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
# STEP 1 — UNIVERSE
# ═══════════════════════════════════════════════════════════════════════════════

def get_yahoo_universe():
    base      = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
    screeners = [
        'most_actives', 'day_gainers', 'day_losers',
        'growth_technology_stocks', 'small_cap_gainers', 'aggressive_small_caps',
    ]
    tickers = set()
    for scr in screeners:
        try:
            res = requests.get(base, headers=YF_HEADERS,
                               params={'scrIds': scr, 'count': 250}, timeout=15)
            res.raise_for_status()
            quotes = res.json()['finance']['result'][0]['quotes']
            before = len(tickers)
            for q in quotes:
                sym    = q.get('symbol', '')
                q_type = q.get('quoteType', '')
                if (sym and q_type in ('EQUITY', 'ETF')
                        and '.' not in sym and '^' not in sym
                        and '-' not in sym and '+' not in sym
                        and not sym.endswith('W') and not sym.endswith('R')
                        and not sym.endswith('U') and len(sym) <= 5):
                    tickers.add(sym)
            print(f"  {scr:<35} +{len(tickers) - before:>3}  (total {len(tickers)})")
        except Exception as e:
            print(f"  {scr:<35} FAILED — {e}")
    return sorted(tickers)


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2 — PRE-FILTER
# ═══════════════════════════════════════════════════════════════════════════════

def pre_filter(tickers, adr_min, vol_min, max_workers=20):
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
            r = fut.result()
            if r:
                passing.append(r)
    return sorted(passing)


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3 — QULLAMAGGIE BREAKOUT ANALYZER
# ═══════════════════════════════════════════════════════════════════════════════

class BreakoutAnalyzer:

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
        tr             = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
        df['ATR']      = tr.rolling(14).mean()
        df['ATR_Pct']  = df['ATR'] / c * 100
        df['Vol_MA20'] = df['Volume'].rolling(20).mean()
        return df

    def compute_adr_and_dolvol(self, df):
        adr    = ((df['High'] - df['Low']) / df['Close'] * 100).tail(20).mean()
        dolvol = (df['Close'] * df['Volume']).tail(10).mean()
        return round(float(adr), 2), round(float(dolvol), 0)

    # ── 1. PRIOR MOVE / FLAGPOLE (25 pts) ────────────────────────────────────
    def analyze_prior_move(self, df):
        score, conditions = 0, []
        c = df['Close']

        # 1-month return (0-10 pts)
        ret_1m = (float(c.iloc[-1]) / float(c.iloc[-21]) - 1) * 100 if len(c) >= 21 else 0
        if ret_1m > 50:
            score += 10
            conditions.append(f"1-month return: +{ret_1m:.1f}% — explosive flagpole, top-tier momentum")
        elif ret_1m > 30:
            score += 8
            conditions.append(f"1-month return: +{ret_1m:.1f}% — strong flagpole, clear prior move")
        elif ret_1m > 20:
            score += 5
            conditions.append(f"1-month return: +{ret_1m:.1f}% — solid move, moderate flagpole")
        elif ret_1m > 10:
            score += 3
            conditions.append(f"1-month return: +{ret_1m:.1f}% — mild move, weak flagpole")
        else:
            score += 0
            conditions.append(f"1-month return: {ret_1m:.1f}% — no meaningful prior move")

        # Move sharpness: how quickly was the bulk of the move made? (0-8 pts)
        # Find the highest close in last 21 sessions and how many days it took from low
        recent     = c.tail(21)
        low_idx    = int(recent.values.argmin())
        high_idx   = int(recent.values.argmax())
        days_taken = abs(high_idx - low_idx)

        if ret_1m > 10:
            if days_taken <= 7:
                score += 8
                conditions.append(f"Move completed in {days_taken} sessions — sharp, impulsive flagpole")
            elif days_taken <= 14:
                score += 5
                conditions.append(f"Move took {days_taken} sessions — reasonably sharp flagpole")
            else:
                score += 2
                conditions.append(f"Move took {days_taken} sessions — slow grind, not ideal flagpole character")
        else:
            score += 0
            conditions.append("No significant move to measure sharpness")

        # Catalyst day detection (0-7 pts)
        # Gap up >= 4% OR single candle body > 2x ATR on volume > 1.8x avg
        gap       = (df['Open'] / df['Close'].shift(1) - 1) * 100
        max_gap   = float(gap.tail(30).max())
        body      = (df['Close'] - df['Open']).abs()
        atr_valid = df['ATR'].dropna()
        vol_ma20  = df['Vol_MA20'].dropna()

        if len(atr_valid) > 0 and len(vol_ma20) > 0:
            surge_days = (
                (body > 2 * df['ATR']) &
                (df['Volume'] > 1.8 * df['Vol_MA20']) &
                (df['Close'] > df['Open'])
            ).tail(30)
            has_surge = bool(surge_days.any())
        else:
            has_surge = False

        if max_gap >= 8 or (max_gap >= 4 and has_surge):
            score += 7
            conditions.append(f"Catalyst confirmed: gap +{max_gap:.1f}% and/or surge candle on heavy volume — institutional event fingerprint")
        elif max_gap >= 4:
            score += 4
            conditions.append(f"Gap up of {max_gap:.1f}% detected in last 30 sessions — potential catalyst event")
        elif has_surge:
            score += 3
            conditions.append("Surge candle (2× ATR body on 1.8× volume) detected — possible catalyst without gap")
        else:
            score += 0
            conditions.append("No identifiable catalyst day in last 30 sessions — organic drift, not episodic")

        pct    = score / 25
        status = "SUPPORTIVE" if pct >= 0.65 else "NEUTRAL" if pct >= 0.35 else "UNSUPPORTIVE"
        return {'score': min(25, score), 'max': 25, 'status': status, 'conditions': conditions}

    # ── 2. CONSOLIDATION QUALITY (20 pts) ─────────────────────────────────────
    def analyze_consolidation(self, df):
        score, conditions = 0, []
        c, h, l = df['Close'], df['High'], df['Low']

        # Find consolidation start: last local high in recent 30 sessions
        recent_30   = c.tail(30)
        peak_idx    = int(recent_30.values.argmax())
        days_since  = len(recent_30) - 1 - peak_idx  # sessions since peak

        price       = float(c.iloc[-1])
        peak_price  = float(recent_30.iloc[peak_idx])
        pullback    = (price / peak_price - 1) * 100  # negative = pullback

        # Range tightness: H-L of last 5 sessions as % of current price (0-10 pts)
        last5_h = float(h.tail(5).max())
        last5_l = float(l.tail(5).min())
        rng5    = (last5_h - last5_l) / price * 100

        if rng5 < 5:
            score += 10
            conditions.append(f"5-session H-L range: {rng5:.1f}% — exceptionally tight coil, spring loaded")
        elif rng5 < 8:
            score += 7
            conditions.append(f"5-session H-L range: {rng5:.1f}% — tight consolidation, very constructive")
        elif rng5 < 12:
            score += 4
            conditions.append(f"5-session H-L range: {rng5:.1f}% — moderate tightness, acceptable flag")
        else:
            score += 1
            conditions.append(f"5-session H-L range: {rng5:.1f}% — too wide, not a tight flag")

        # Pullback depth off flagpole high (0-6 pts)
        if pullback >= -10:
            score += 6
            conditions.append(f"Pullback: {pullback:.1f}% off recent high — barely breathed, shallow flag")
        elif pullback >= -18:
            score += 4
            conditions.append(f"Pullback: {pullback:.1f}% off recent high — healthy flag depth")
        elif pullback >= -28:
            score += 2
            conditions.append(f"Pullback: {pullback:.1f}% off recent high — deeper pullback, more overhead supply")
        else:
            score += 0
            conditions.append(f"Pullback: {pullback:.1f}% off recent high — excessive pullback, flag structure damaged")

        # Structure: higher lows in the consolidation window (0-4 pts)
        if days_since >= 3:
            consol_lows = l.tail(days_since + 1)
            n           = len(consol_lows)
            if n >= 3:
                hl_count = sum(
                    float(consol_lows.iloc[i]) > float(consol_lows.iloc[i - 1])
                    for i in range(1, n)
                )
                hl_ratio = hl_count / (n - 1)
                if hl_ratio >= 0.6:
                    score += 4
                    conditions.append(f"Higher lows on {hl_count}/{n-1} sessions — orderly, constructive flag structure")
                elif hl_ratio >= 0.4:
                    score += 2
                    conditions.append(f"Mixed lows pattern ({hl_count}/{n-1} higher) — consolidation neutral")
                else:
                    score += 0
                    conditions.append(f"Lower lows dominating — distribution concern within consolidation")
            else:
                score += 2
                conditions.append("Insufficient consolidation sessions for structure analysis")
        else:
            score += 2
            conditions.append("Very recent peak — consolidation just beginning")

        # Duration penalty / ideal window
        if days_since < 3:
            score -= 3
            conditions.append(f"Only {days_since} sessions since peak — flag not yet formed, too early")
        elif days_since <= 15:
            conditions.append(f"{days_since} sessions since peak — ideal consolidation window (3–15 days)")
        elif days_since <= 30:
            score -= 2
            conditions.append(f"{days_since} sessions since peak — consolidation extending, energy leaking")
        else:
            score -= 5
            conditions.append(f"{days_since} sessions since peak — move is stale, flag too extended")

        pct    = score / 20
        status = "SUPPORTIVE" if pct >= 0.65 else "NEUTRAL" if pct >= 0.35 else "UNSUPPORTIVE"
        return {'score': max(0, min(20, score)), 'max': 20, 'status': status, 'conditions': conditions}

    # ── 3. MA SURF (15 pts) ───────────────────────────────────────────────────
    def analyze_ma_surf(self, df):
        score, conditions = 0, []

        row    = df.dropna(subset=['SMA10', 'SMA20']).iloc[-1]
        price  = float(row['Close'])
        sma10  = float(row['SMA10'])
        sma20  = float(row['SMA20'])
        sma50  = float(row['SMA50']) if not pd.isna(row.get('SMA50', float('nan'))) else None

        pct10 = (price - sma10) / sma10 * 100
        pct20 = (price - sma20) / sma20 * 100

        def slope(col, n):
            s = df[col].dropna()
            return float((s.iloc[-1] - s.iloc[-n]) / s.iloc[-n] * 100) if len(s) >= n else 0.0

        s10_rising = slope('SMA10', 5) > 0
        s20_rising = slope('SMA20', 5) > 0

        # 10d SMA surf (0-8 pts)
        if -3 <= pct10 <= 3:
            pts10 = 8 if s10_rising else 5
            tag   = "rising" if s10_rising else "flat — watch slope"
            conditions.append(f"Price within 10d SMA zone ({pct10:+.1f}%), SMA {tag} — ideal surf position")
        elif pct10 > 3:
            pts10 = 3
            conditions.append(f"Price {pct10:+.1f}% extended above 10d SMA — too far from base, buyable on pullback")
        else:
            pts10 = 0
            conditions.append(f"Price {pct10:+.1f}% below 10d SMA — broken below short-term support")
        score += pts10

        # 20d SMA surf (0-7 pts)
        if -5 <= pct20 <= 5:
            pts20 = 7 if s20_rising else 4
            tag   = "rising" if s20_rising else "flat"
            conditions.append(f"Price within 20d SMA zone ({pct20:+.1f}%), SMA {tag} — holding key support zone")
        elif pct20 > 5:
            pts20 = 2
            conditions.append(f"Price {pct20:+.1f}% extended above 20d SMA — extended, needs base")
        else:
            pts20 = 0
            conditions.append(f"Price {pct20:+.1f}% below 20d SMA — below medium-term support, flag structure broken")
        score += pts20

        # Full MA stack bonus (capped at 15 total)
        if sma50 is not None:
            s50_rising = slope('SMA50', 10) > 0
            if sma10 > sma20 > sma50 and s10_rising and s20_rising and s50_rising:
                score += 3
                conditions.append("Full bullish stack: 10d > 20d > 50d, all rising — textbook Qullamaggie trend alignment")
            elif sma10 > sma20 > sma50:
                score += 1
                conditions.append("10d > 20d > 50d stacked but not all rising — structural alignment present")

        pct    = min(score, 15) / 15
        status = "SUPPORTIVE" if pct >= 0.65 else "NEUTRAL" if pct >= 0.35 else "UNSUPPORTIVE"
        return {'score': min(15, score), 'max': 15, 'status': status, 'conditions': conditions}

    # ── 4. BREAKOUT READINESS (15 pts) ────────────────────────────────────────
    def analyze_breakout_readiness(self, df):
        score, conditions = 0, []
        c, h, l = df['Close'], df['High'], df['Low']
        price   = float(c.iloc[-1])

        # Distance to consolidation high (recent 15 sessions) (0-8 pts)
        consol_high = float(h.tail(15).max())
        dist_consol = (price / consol_high - 1) * 100

        if -3 <= dist_consol <= 0:
            score += 8
            conditions.append(f"Price {abs(dist_consol):.1f}% from consolidation high — at the door, entry imminent")
        elif dist_consol >= 0:
            score += 6
            conditions.append(f"Price has broken consolidation high by {dist_consol:.1f}% — potential breakout in progress")
        elif dist_consol >= -7:
            score += 6
            conditions.append(f"Price {abs(dist_consol):.1f}% below consolidation high — approaching pivot, watch closely")
        elif dist_consol >= -12:
            score += 3
            conditions.append(f"Price {abs(dist_consol):.1f}% below consolidation high — needs more work before pivot")
        else:
            score += 0
            conditions.append(f"Price {abs(dist_consol):.1f}% below consolidation high — too far from breakout level")

        # Distance to 52-week high (0-4 pts)
        high52  = float(c.tail(252).max())
        dist52  = (price / high52 - 1) * 100

        if dist52 >= -5:
            score += 4
            conditions.append(f"Price within {abs(dist52):.1f}% of 52w high — blue sky territory, no overhead resistance")
        elif dist52 >= -15:
            score += 2
            conditions.append(f"Price {abs(dist52):.1f}% below 52w high — moderate overhead supply")
        else:
            score += 0
            conditions.append(f"Price {abs(dist52):.1f}% below 52w high — significant overhead resistance")

        # Today's range compression vs ATR14 (0-3 pts)
        today_range = float(h.iloc[-1]) - float(l.iloc[-1])
        atr14       = float(df['ATR'].dropna().iloc[-1]) if len(df['ATR'].dropna()) > 0 else today_range

        range_ratio = today_range / atr14 if atr14 > 0 else 1.0

        if range_ratio < 0.5:
            score += 3
            conditions.append(f"Today's range {range_ratio:.0%} of ATR — inside/tight day, spring fully compressed")
        elif range_ratio < 0.75:
            score += 1
            conditions.append(f"Today's range {range_ratio:.0%} of ATR — below average range, mild compression")
        else:
            score += 0
            conditions.append(f"Today's range {range_ratio:.0%} of ATR — normal or wide range day")

        pct    = score / 15
        status = "SUPPORTIVE" if pct >= 0.65 else "NEUTRAL" if pct >= 0.35 else "UNSUPPORTIVE"
        return {'score': min(15, score), 'max': 15, 'status': status, 'conditions': conditions}

    # ── 5. VOLUME SIGNATURE (15 pts) ──────────────────────────────────────────
    def analyze_volume_signature(self, df):
        score, conditions = 0, []
        clean    = df.dropna(subset=['Vol_MA20'])
        vol_ma20 = float(clean['Vol_MA20'].iloc[-1])

        # Volume dry-up during consolidation (0-6 pts)
        r5v  = float(clean['Volume'].tail(5).mean())
        p5v  = float(clean['Volume'].tail(10).head(5).mean())
        vr   = r5v / vol_ma20
        vchg = (r5v / p5v - 1) * 100 if p5v > 0 else 0

        if vr < 0.5:
            score += 6
            conditions.append(f"Volume at {vr:.2f}x avg — severe dry-up, stock being ignored: ideal pre-breakout")
        elif vr < 0.65:
            score += 5
            conditions.append(f"Volume at {vr:.2f}x avg — clear dry-up during consolidation, very constructive")
        elif vr < 0.80:
            score += 3
            conditions.append(f"Volume at {vr:.2f}x avg — mild dry-up, consolidating quietly")
        elif vr < 1.0:
            score += 1
            conditions.append(f"Volume at {vr:.2f}x avg — slightly below average, not a clear dry-up")
        else:
            score += 0
            conditions.append(f"Volume at {vr:.2f}x avg — elevated during consolidation, distribution risk")

        # Accumulation bias: up-day vol vs down-day vol (0-5 pts)
        recent = clean.tail(20)
        up     = recent[recent['Close'] > recent['Open']]
        dn     = recent[recent['Close'] <= recent['Open']]
        uv     = float(up['Volume'].mean()) if len(up) > 0 else 0
        dv     = float(dn['Volume'].mean()) if len(dn) > 0 else 1

        if uv > 0 and dv > 0:
            acc = uv / dv
            if acc > 1.6:
                score += 5
                conditions.append(f"Up-day vol {acc:.1f}x down-day vol — clear accumulation signature on the flagpole")
            elif acc > 1.25:
                score += 3
                conditions.append(f"Up-day vol {acc:.1f}x down-day vol — mild accumulation bias, constructive")
            elif acc > 0.85:
                score += 1
                conditions.append(f"Up/down vol near parity ({acc:.1f}x) — neutral, no clear directional bias")
            else:
                score += 0
                conditions.append(f"Down-day vol exceeds up-day ({acc:.1f}x) — distribution concern")

        # Flagpole volume spike in last 10 sessions (0-4 pts)
        spikes = clean['Volume'].tail(10) / vol_ma20
        mx_spk = float(spikes.max())
        spk_idx = spikes.idxmax()
        is_up   = float(clean.loc[spk_idx, 'Close']) > float(clean.loc[spk_idx, 'Open'])

        if mx_spk > 2.0 and is_up:
            score += 4
            conditions.append(f"Volume spike {mx_spk:.1f}x avg on up-day (last 10 sessions) — institutional buy program fingerprint")
        elif mx_spk > 1.5 and is_up:
            score += 2
            conditions.append(f"Volume pickup {mx_spk:.1f}x avg on up-day — buying interest present on the move")
        elif mx_spk > 2.0 and not is_up:
            score += 0
            conditions.append(f"Volume spike {mx_spk:.1f}x avg on a down-day — distribution warning")
        else:
            score += 1
            conditions.append(f"No major volume spike detected (max {mx_spk:.1f}x) — quiet base, no selling pressure either")

        pct    = score / 15
        status = "SUPPORTIVE" if pct >= 0.65 else "NEUTRAL" if pct >= 0.35 else "UNSUPPORTIVE"
        return {'score': min(15, score), 'max': 15, 'status': status, 'conditions': conditions}

    # ── 6. RELATIVE STRENGTH (10 pts) ─────────────────────────────────────────
    def analyze_relative_strength(self, df):
        score, conditions = 0, []
        c = df['Close']

        # 1-month return (0-3 pts)
        ret_1m = (float(c.iloc[-1]) / float(c.iloc[-21]) - 1) * 100 if len(c) >= 21 else 0
        if ret_1m > 20:
            score += 3
            conditions.append(f"1-month return: +{ret_1m:.1f}% — top-tier 1m performer")
        elif ret_1m > 10:
            score += 2
            conditions.append(f"1-month return: +{ret_1m:.1f}% — above-average near-term strength")
        else:
            score += 0
            conditions.append(f"1-month return: {ret_1m:.1f}% — not in the top tier on 1m basis")

        # 3-month return (0-4 pts)
        ret_3m = (float(c.iloc[-1]) / float(c.iloc[-63]) - 1) * 100 if len(c) >= 63 else 0
        if ret_3m > 40:
            score += 4
            conditions.append(f"3-month return: +{ret_3m:.1f}% — exceptional 3m strength, institutional-grade leader")
        elif ret_3m > 20:
            score += 3
            conditions.append(f"3-month return: +{ret_3m:.1f}% — solid 3m performer, above the crowd")
        else:
            score += 0
            conditions.append(f"3-month return: {ret_3m:.1f}% — 3m performance not exceptional")

        # 6-month return (0-3 pts)
        ret_6m = (float(c.iloc[-1]) / float(c.iloc[-126]) - 1) * 100 if len(c) >= 126 else 0
        if ret_6m > 60:
            score += 3
            conditions.append(f"6-month return: +{ret_6m:.1f}% — dominant 6m leader, top 1–2% territory")
        elif ret_6m > 30:
            score += 2
            conditions.append(f"6-month return: +{ret_6m:.1f}% — strong 6m performer")
        else:
            score += 0
            conditions.append(f"6-month return: {ret_6m:.1f}% — 6m performance not in leading tier")

        pct    = score / 10
        status = "SUPPORTIVE" if pct >= 0.65 else "NEUTRAL" if pct >= 0.35 else "UNSUPPORTIVE"
        return {'score': min(10, score), 'max': 10, 'status': status, 'conditions': conditions}

    # ── RATE ONE STOCK ────────────────────────────────────────────────────────
    def rate_stock(self, ticker):
        df = self.fetch_data(ticker)
        if df is None:
            return None
        df = self.calculate_indicators(df)

        adr_pct, dollar_volume = self.compute_adr_and_dolvol(df)
        if adr_pct < ADR_MIN or dollar_volume < VOL_MIN:
            return None

        prior_move   = self.analyze_prior_move(df)
        consolidation = self.analyze_consolidation(df)
        ma_surf      = self.analyze_ma_surf(df)
        br_ready     = self.analyze_breakout_readiness(df)
        vol_sig      = self.analyze_volume_signature(df)
        rel_str      = self.analyze_relative_strength(df)

        total = (prior_move['score'] + consolidation['score'] + ma_surf['score'] +
                 br_ready['score'] + vol_sig['score'] + rel_str['score'])

        if   total >= 88: grade, verdict = "A+", "PRIME SETUP — Textbook Qullamaggie flag on a leader, stalk for entry"
        elif total >= 75: grade, verdict = "A",  "STRONG SETUP — High-quality consolidation, near pivot"
        elif total >= 62: grade, verdict = "B",  "DEVELOPING — Flagpole solid, flag still forming"
        elif total >= 48: grade, verdict = "C",  "MIXED — Some elements present, not yet actionable"
        elif total >= 35: grade, verdict = "D",  "WEAK — Prior move fading or consolidation messy"
        else:             grade, verdict = "F",  "NO SETUP — Not a Qullamaggie candidate"

        try:
            name = yf.Ticker(ticker).info.get('shortName', ticker)
        except Exception:
            name = ticker

        return {
            'ticker':        ticker,
            'name':          name,
            'price':         round(float(df['Close'].iloc[-1]), 2),
            'total':         total,
            'grade':         grade,
            'verdict':       verdict,
            'adr_pct':       adr_pct,
            'dollar_volume': dollar_volume,
            'prior_move':    prior_move,
            'consolidation': consolidation,
            'ma_surf':       ma_surf,
            'br_ready':      br_ready,
            'vol_sig':       vol_sig,
            'rel_str':       rel_str,
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
    print(f"  QullaScanner  |  {TODAY}  |  ADR>{ADR_MIN}%  Vol>${VOL_MIN/1e6:.0f}M")
    print(f"  PriorMove(25) + Consolidation(20) + MASurf(15)")
    print(f"  + BreakoutReady(15) + VolumeSignature(15) + RS(10) = 100")
    print(f"{'='*60}\n")

    print("STEP 1 — Fetching universe from Yahoo Finance screeners...")
    universe = get_yahoo_universe()
    print(f"  → {len(universe)} candidate tickers found\n")
    if not universe:
        print("No universe returned — aborting.")
        return

    print(f"STEP 2 — Pre-filtering {len(universe)} tickers...")
    candidates = pre_filter(universe, ADR_MIN, VOL_MIN, max_workers=20)
    print(f"  → {len(candidates)} tickers passed pre-filter\n")
    if not candidates:
        print("No candidates passed pre-filter.")
        return

    print(f"  Candidates: {', '.join(candidates)}\n")
    print(f"STEP 3 — Full Qullamaggie analysis on {len(candidates)} candidates...")

    analyzer = BreakoutAnalyzer()
    results  = []

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(analyzer.rate_stock, t): t for t in candidates}
        for i, fut in enumerate(as_completed(futures), 1):
            t = futures[fut]
            try:
                r = fut.result()
                if r:
                    results.append(r)
                    print(
                        f"  [{i:>2}/{len(candidates)}] PASS  {t:<6}  "
                        f"{r['total']:>3}/100 [{r['grade']:<2}]  "
                        f"PM:{r['prior_move']['score']} "
                        f"C:{r['consolidation']['score']} "
                        f"MA:{r['ma_surf']['score']} "
                        f"BR:{r['br_ready']['score']} "
                        f"V:{r['vol_sig']['score']} "
                        f"RS:{r['rel_str']['score']}"
                    )
                else:
                    print(f"  [{i:>2}/{len(candidates)}] skip  {t}")
            except Exception as e:
                print(f"  [{i:>2}/{len(candidates)}] ERR   {t}  — {e}")

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
    with open(DATED_FILE,  'w') as f: json.dump(payload, f)
    with open(LATEST_FILE, 'w') as f: json.dump(payload, f)
    update_index(TODAY)

    print(f"\n{'='*60}")
    print(f"  Done — {len(results)} stocks passed all filters")
    print(f"  Scanned {len(universe)} → pre-filtered {len(candidates)} → scored {len(results)}")
    print(f"  Saved to {DATED_FILE}")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
