#!/usr/bin/env python3
"""
swing/api_server.py
Qullamaggie Breakout Rater API — deployed on Render
Start command: uvicorn api_server:app --host 0.0.0.0 --port 8000

Scoring: PriorMove(25) + Consolidation(20) + MASurf(15) + BreakoutReadiness(15)
         + VolumeSignature(15) + RelativeStrength(10) = 100
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import yfinance as yf
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

ADR_MIN = 5.0
VOL_MIN = 20_000_000


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

        gap     = (df['Open'] / df['Close'].shift(1) - 1) * 100
        max_gap = float(gap.tail(30).max())
        body    = (df['Close'] - df['Open']).abs()
        atr_v   = df['ATR'].dropna()
        vol_v   = df['Vol_MA20'].dropna()

        if len(atr_v) > 0 and len(vol_v) > 0:
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
            conditions.append(f"Catalyst confirmed: gap +{max_gap:.1f}% and/or surge candle on heavy volume")
        elif max_gap >= 4:
            score += 4
            conditions.append(f"Gap up of {max_gap:.1f}% detected — potential catalyst event")
        elif has_surge:
            score += 3
            conditions.append("Surge candle detected (2× ATR body on 1.8× volume) — possible catalyst without gap")
        else:
            score += 0
            conditions.append("No identifiable catalyst day — organic drift, not episodic")

        pct    = score / 25
        status = "SUPPORTIVE" if pct >= 0.65 else "NEUTRAL" if pct >= 0.35 else "UNSUPPORTIVE"
        return {'score': min(25, score), 'max': 25, 'status': status, 'conditions': conditions}

    # ── 2. CONSOLIDATION QUALITY (20 pts) ─────────────────────────────────────
    def analyze_consolidation(self, df):
        score, conditions = 0, []
        c, h, l = df['Close'], df['High'], df['Low']

        recent_30  = c.tail(30)
        peak_idx   = int(recent_30.values.argmax())
        days_since = len(recent_30) - 1 - peak_idx
        price      = float(c.iloc[-1])
        peak_price = float(recent_30.iloc[peak_idx])
        pullback   = (price / peak_price - 1) * 100

        last5_h = float(h.tail(5).max())
        last5_l = float(l.tail(5).min())
        rng5    = (last5_h - last5_l) / price * 100

        if rng5 < 5:
            score += 10
            conditions.append(f"5-session H-L range: {rng5:.1f}% — exceptionally tight coil")
        elif rng5 < 8:
            score += 7
            conditions.append(f"5-session H-L range: {rng5:.1f}% — tight flag, very constructive")
        elif rng5 < 12:
            score += 4
            conditions.append(f"5-session H-L range: {rng5:.1f}% — moderate tightness")
        else:
            score += 1
            conditions.append(f"5-session H-L range: {rng5:.1f}% — too wide, not a tight flag")

        if pullback >= -10:
            score += 6
            conditions.append(f"Pullback: {pullback:.1f}% — barely breathed, shallow flag")
        elif pullback >= -18:
            score += 4
            conditions.append(f"Pullback: {pullback:.1f}% — healthy flag depth")
        elif pullback >= -28:
            score += 2
            conditions.append(f"Pullback: {pullback:.1f}% — deeper pullback, more overhead supply")
        else:
            score += 0
            conditions.append(f"Pullback: {pullback:.1f}% — excessive, flag structure damaged")

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
                    conditions.append(f"Higher lows on {hl_count}/{n-1} sessions — orderly flag structure")
                elif hl_ratio >= 0.4:
                    score += 2
                    conditions.append(f"Mixed lows ({hl_count}/{n-1} higher) — neutral structure")
                else:
                    score += 0
                    conditions.append("Lower lows dominating — distribution concern")
            else:
                score += 2
                conditions.append("Too few sessions for structure analysis")
        else:
            score += 2
            conditions.append("Very recent peak — consolidation just beginning")

        if days_since < 3:
            score -= 3
            conditions.append(f"Only {days_since} sessions since peak — flag not yet formed")
        elif days_since <= 15:
            conditions.append(f"{days_since} sessions since peak — ideal window")
        elif days_since <= 30:
            score -= 2
            conditions.append(f"{days_since} sessions since peak — extending, energy leaking")
        else:
            score -= 5
            conditions.append(f"{days_since} sessions since peak — move is stale")

        pct    = score / 20
        status = "SUPPORTIVE" if pct >= 0.65 else "NEUTRAL" if pct >= 0.35 else "UNSUPPORTIVE"
        return {'score': max(0, min(20, score)), 'max': 20, 'status': status, 'conditions': conditions}

    # ── 3. MA SURF (15 pts) ───────────────────────────────────────────────────
    def analyze_ma_surf(self, df):
        score, conditions = 0, []
        row   = df.dropna(subset=['SMA10', 'SMA20']).iloc[-1]
        price = float(row['Close'])
        sma10 = float(row['SMA10'])
        sma20 = float(row['SMA20'])
        sma50 = float(row['SMA50']) if not pd.isna(row.get('SMA50', float('nan'))) else None

        pct10 = (price - sma10) / sma10 * 100
        pct20 = (price - sma20) / sma20 * 100

        def slope(col, n):
            s = df[col].dropna()
            return float((s.iloc[-1] - s.iloc[-n]) / s.iloc[-n] * 100) if len(s) >= n else 0.0

        s10_rising = slope('SMA10', 5) > 0
        s20_rising = slope('SMA20', 5) > 0

        if -3 <= pct10 <= 3:
            pts10 = 8 if s10_rising else 5
            tag   = "rising" if s10_rising else "flat"
            conditions.append(f"Price within 10d SMA zone ({pct10:+.1f}%), SMA {tag} — ideal surf position")
        elif pct10 > 3:
            pts10 = 3
            conditions.append(f"Price {pct10:+.1f}% extended above 10d SMA — too far from base")
        else:
            pts10 = 0
            conditions.append(f"Price {pct10:+.1f}% below 10d SMA — broken below short-term support")
        score += pts10

        if -5 <= pct20 <= 5:
            pts20 = 7 if s20_rising else 4
            tag   = "rising" if s20_rising else "flat"
            conditions.append(f"Price within 20d SMA zone ({pct20:+.1f}%), SMA {tag} — holding key support")
        elif pct20 > 5:
            pts20 = 2
            conditions.append(f"Price {pct20:+.1f}% extended above 20d SMA — needs base")
        else:
            pts20 = 0
            conditions.append(f"Price {pct20:+.1f}% below 20d SMA — below medium-term support")
        score += pts20

        if sma50 is not None:
            s50_rising = slope('SMA50', 10) > 0
            if sma10 > sma20 > sma50 and s10_rising and s20_rising and s50_rising:
                score += 3
                conditions.append("Full bullish stack: 10d > 20d > 50d, all rising — textbook Qullamaggie alignment")
            elif sma10 > sma20 > sma50:
                score += 1
                conditions.append("10d > 20d > 50d stacked but not all rising")

        pct    = min(score, 15) / 15
        status = "SUPPORTIVE" if pct >= 0.65 else "NEUTRAL" if pct >= 0.35 else "UNSUPPORTIVE"
        return {'score': min(15, score), 'max': 15, 'status': status, 'conditions': conditions}

    # ── 4. BREAKOUT READINESS (15 pts) ────────────────────────────────────────
    def analyze_breakout_readiness(self, df):
        score, conditions = 0, []
        c, h, l = df['Close'], df['High'], df['Low']
        price   = float(c.iloc[-1])

        consol_high = float(h.tail(15).max())
        dist_consol = (price / consol_high - 1) * 100

        if -3 <= dist_consol <= 0:
            score += 8
            conditions.append(f"Price {abs(dist_consol):.1f}% from consolidation high — at the door")
        elif dist_consol >= 0:
            score += 6
            conditions.append(f"Price has broken consolidation high by {dist_consol:.1f}% — breakout in progress")
        elif dist_consol >= -7:
            score += 6
            conditions.append(f"Price {abs(dist_consol):.1f}% below consolidation high — approaching pivot")
        elif dist_consol >= -12:
            score += 3
            conditions.append(f"Price {abs(dist_consol):.1f}% below consolidation high — needs more work")
        else:
            score += 0
            conditions.append(f"Price {abs(dist_consol):.1f}% below consolidation high — too far from pivot")

        high52  = float(c.tail(252).max())
        dist52  = (price / high52 - 1) * 100

        if dist52 >= -5:
            score += 4
            conditions.append(f"Within {abs(dist52):.1f}% of 52w high — blue sky territory")
        elif dist52 >= -15:
            score += 2
            conditions.append(f"{abs(dist52):.1f}% below 52w high — moderate overhead supply")
        else:
            score += 0
            conditions.append(f"{abs(dist52):.1f}% below 52w high — significant overhead resistance")

        today_range = float(h.iloc[-1]) - float(l.iloc[-1])
        atr14       = float(df['ATR'].dropna().iloc[-1]) if len(df['ATR'].dropna()) > 0 else today_range
        range_ratio = today_range / atr14 if atr14 > 0 else 1.0

        if range_ratio < 0.5:
            score += 3
            conditions.append(f"Today's range {range_ratio:.0%} of ATR — inside/tight day")
        elif range_ratio < 0.75:
            score += 1
            conditions.append(f"Today's range {range_ratio:.0%} of ATR — below average range")
        else:
            score += 0
            conditions.append(f"Today's range {range_ratio:.0%} of ATR — normal or wide day")

        pct    = score / 15
        status = "SUPPORTIVE" if pct >= 0.65 else "NEUTRAL" if pct >= 0.35 else "UNSUPPORTIVE"
        return {'score': min(15, score), 'max': 15, 'status': status, 'conditions': conditions}

    # ── 5. VOLUME SIGNATURE (15 pts) ──────────────────────────────────────────
    def analyze_volume_signature(self, df):
        score, conditions = 0, []
        clean    = df.dropna(subset=['Vol_MA20'])
        vol_ma20 = float(clean['Vol_MA20'].iloc[-1])

        r5v = float(clean['Volume'].tail(5).mean())
        p5v = float(clean['Volume'].tail(10).head(5).mean())
        vr  = r5v / vol_ma20

        if vr < 0.5:
            score += 6
            conditions.append(f"Volume at {vr:.2f}x avg — severe dry-up, ideal pre-breakout silence")
        elif vr < 0.65:
            score += 5
            conditions.append(f"Volume at {vr:.2f}x avg — clear dry-up, very constructive")
        elif vr < 0.80:
            score += 3
            conditions.append(f"Volume at {vr:.2f}x avg — mild dry-up, quietly consolidating")
        elif vr < 1.0:
            score += 1
            conditions.append(f"Volume at {vr:.2f}x avg — slightly below average")
        else:
            score += 0
            conditions.append(f"Volume at {vr:.2f}x avg — elevated during consolidation, distribution risk")

        recent = clean.tail(20)
        up     = recent[recent['Close'] > recent['Open']]
        dn     = recent[recent['Close'] <= recent['Open']]
        uv     = float(up['Volume'].mean()) if len(up) > 0 else 0
        dv     = float(dn['Volume'].mean()) if len(dn) > 0 else 1

        if uv > 0 and dv > 0:
            acc = uv / dv
            if acc > 1.6:
                score += 5
                conditions.append(f"Up-day vol {acc:.1f}x down-day vol — clear accumulation on flagpole")
            elif acc > 1.25:
                score += 3
                conditions.append(f"Up-day vol {acc:.1f}x down-day vol — mild accumulation bias")
            elif acc > 0.85:
                score += 1
                conditions.append(f"Up/down vol near parity ({acc:.1f}x) — neutral")
            else:
                score += 0
                conditions.append(f"Down-day vol exceeds up-day ({acc:.1f}x) — distribution concern")

        spikes  = clean['Volume'].tail(10) / vol_ma20
        mx_spk  = float(spikes.max())
        spk_idx = spikes.idxmax()
        is_up   = float(clean.loc[spk_idx, 'Close']) > float(clean.loc[spk_idx, 'Open'])

        if mx_spk > 2.0 and is_up:
            score += 4
            conditions.append(f"Volume spike {mx_spk:.1f}x avg on up-day — institutional buy program fingerprint")
        elif mx_spk > 1.5 and is_up:
            score += 2
            conditions.append(f"Volume pickup {mx_spk:.1f}x avg on up-day — buying interest on the move")
        elif mx_spk > 2.0 and not is_up:
            score += 0
            conditions.append(f"Volume spike {mx_spk:.1f}x avg on down-day — distribution warning")
        else:
            score += 1
            conditions.append(f"No major volume spike (max {mx_spk:.1f}x) — quiet base, no selling pressure")

        pct    = score / 15
        status = "SUPPORTIVE" if pct >= 0.65 else "NEUTRAL" if pct >= 0.35 else "UNSUPPORTIVE"
        return {'score': min(15, score), 'max': 15, 'status': status, 'conditions': conditions}

    # ── 6. RELATIVE STRENGTH (10 pts) ─────────────────────────────────────────
    def analyze_relative_strength(self, df):
        score, conditions = 0, []
        c = df['Close']

        ret_1m = (float(c.iloc[-1]) / float(c.iloc[-21])  - 1) * 100 if len(c) >= 21  else 0
        ret_3m = (float(c.iloc[-1]) / float(c.iloc[-63])  - 1) * 100 if len(c) >= 63  else 0
        ret_6m = (float(c.iloc[-1]) / float(c.iloc[-126]) - 1) * 100 if len(c) >= 126 else 0

        if ret_1m > 20:
            score += 3
            conditions.append(f"1-month return: +{ret_1m:.1f}% — top-tier near-term performer")
        elif ret_1m > 10:
            score += 2
            conditions.append(f"1-month return: +{ret_1m:.1f}% — above-average 1m strength")
        else:
            score += 0
            conditions.append(f"1-month return: {ret_1m:.1f}% — not in top tier on 1m basis")

        if ret_3m > 40:
            score += 4
            conditions.append(f"3-month return: +{ret_3m:.1f}% — exceptional leader, institutional-grade strength")
        elif ret_3m > 20:
            score += 3
            conditions.append(f"3-month return: +{ret_3m:.1f}% — solid 3m performer")
        else:
            score += 0
            conditions.append(f"3-month return: {ret_3m:.1f}% — 3m performance not exceptional")

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

        prior_move    = self.analyze_prior_move(df)
        consolidation = self.analyze_consolidation(df)
        ma_surf       = self.analyze_ma_surf(df)
        br_ready      = self.analyze_breakout_readiness(df)
        vol_sig       = self.analyze_volume_signature(df)
        rel_str       = self.analyze_relative_strength(df)

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


# ── ROUTES ────────────────────────────────────────────────────────────────────
_analyzer = BreakoutAnalyzer()

@app.post("/api/rate")
async def rate_stock(request: Request):
    data   = await request.json()
    ticker = data.get("ticker", "").upper().strip()
    if not ticker:
        return JSONResponse({"error": "No ticker provided"}, status_code=400)
    result = _analyzer.rate_stock(ticker)
    if not result:
        return JSONResponse({"error": f"Could not fetch data for {ticker}"}, status_code=404)
    return JSONResponse(result)

@app.get("/api/health")
async def health():
    return {"status": "ok"}
