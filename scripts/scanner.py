#!/usr/bin/env python3
"""
scripts/scanner.py
Daily swing trade screener — runs via GitHub Actions
Filters: ADR > 5%, Daily Dollar Volume > $20M
Outputs: data/screener_data.json
"""

import yfinance as yf
import pandas as pd
import numpy as np
import json
import os
import warnings
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings('ignore')

OUTPUT_FILE  = "data/screener_data.json"
ADR_MIN      = 5.0
VOL_MIN      = 20_000_000

UNIVERSE = [
    "NVDA", "AMD",  "AVGO", "SMCI", "ARM",  "TSM",  "AEHR", "MRVL",
    "MSTR", "COIN", "HOOD", "MARA", "RIOT", "CLSK", "CIFR", "HUT",
    "META", "TSLA", "PLTR", "SOFI", "UPST", "CRWD", "NET",  "DDOG", "AXON",
    "IONQ", "RGTI", "QUBT", "QBTS", "ARQQ",
    "ACHR", "JOBY", "RKLB", "LUNR", "BLNK",
    "SNOW", "BILL", "RBLX", "SHOP", "U",    "CELH",
    "BABA", "JD",   "PDD",
    "GME",  "SOUN", "BBAI", "CLOV",
]


class SwingTradeAnalyzer:

    def fetch_data(self, ticker):
        try:
            df = yf.Ticker(ticker).history(period="1y", auto_adjust=True)
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

        delta        = c.diff()
        gain         = delta.clip(lower=0).rolling(14).mean()
        loss         = (-delta.clip(upper=0)).rolling(14).mean()
        df['RSI']    = 100 - (100 / (1 + gain / loss))

        ema12             = c.ewm(span=12, adjust=False).mean()
        ema26             = c.ewm(span=26, adjust=False).mean()
        df['MACD']        = ema12 - ema26
        df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
        df['MACD_Hist']   = df['MACD'] - df['MACD_Signal']

        bb_mid         = c.rolling(20).mean()
        bb_std         = c.rolling(20).std()
        df['BB_Upper'] = bb_mid + 2 * bb_std
        df['BB_Lower'] = bb_mid - 2 * bb_std
        df['BB_Width'] = (df['BB_Upper'] - df['BB_Lower']) / bb_mid

        tr             = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
        df['ATR']      = tr.rolling(14).mean()
        df['ATR_Pct']  = df['ATR'] / c * 100
        df['Vol_MA20'] = df['Volume'].rolling(20).mean()
        return df

    def compute_adr_and_dolvol(self, df):
        adr     = ((df['High'] - df['Low']) / df['Close'] * 100).tail(20).mean()
        dol_vol = (df['Close'] * df['Volume']).tail(10).mean()
        return round(float(adr), 2), round(float(dol_vol), 0)

    def analyze_trend(self, df):
        row    = df.dropna(subset=['SMA10', 'SMA20', 'SMA50']).iloc[-1]
        price  = float(row['Close'])
        sma10  = float(row['SMA10'])
        sma20  = float(row['SMA20'])
        sma50  = float(row['SMA50'])
        sma200 = float(row['SMA200']) if not pd.isna(row['SMA200']) else None
        score, conditions, status = 0, [], "NEUTRAL"

        checks  = [price > sma10, price > sma20, price > sma50]
        if sma200:
            checks.append(price > sma200)
        n_above, total = sum(checks), len(checks)

        if n_above == total:
            score += 10; status = "SUPPORTIVE"
            conditions.append(f"Price is above all {total} key SMAs (10/20/50/200) — strong bullish positioning")
        elif n_above >= total - 1:
            score += 7; status = "SUPPORTIVE"
            conditions.append(f"Price is above {n_above}/{total} key SMAs — mostly bullish, minor lag on one average")
        elif n_above == 2:
            score += 4
            conditions.append(f"Price is above {n_above}/{total} key SMAs — mixed trend, no clear directional bias")
        elif n_above == 1:
            score += 2; status = "UNSUPPORTIVE"
            conditions.append(f"Price is above only {n_above}/{total} SMAs — below most key levels")
        else:
            score += 0; status = "UNSUPPORTIVE"
            conditions.append("Price is below all key SMAs — bearish or early-stage base building")

        stacked = sma10 > sma20 > sma50
        if sma200:
            stacked = stacked and sma50 > sma200

        def slope(col, n):
            s = df[col].dropna()
            return float((s.iloc[-1] - s.iloc[-n]) / s.iloc[-n] * 100) if len(s) >= n else 0.0

        s10       = slope('SMA10', 5)
        s20       = slope('SMA20', 5)
        s50       = slope('SMA50', 10)
        slopes_up = s10 > 0 and s20 > 0 and s50 > 0

        if stacked and slopes_up:
            score += 10; status = "SUPPORTIVE"
            conditions.append("Full bullish MA stack (10>20>50>200) with all averages sloping upward — ideal trend alignment")
        elif stacked or (sma10 > sma20 > sma50 and slopes_up):
            score += 7
            conditions.append("MAs show bullish stacking with generally rising slopes — constructive trend structure")
            if status != "SUPPORTIVE": status = "SUPPORTIVE"
        elif sma10 > sma20 > sma50:
            score += 5
            conditions.append("Short-term MAs (10>20>50) stacked bullishly but longer-term alignment is mixed — emerging trend")
        elif slopes_up:
            score += 3
            conditions.append("MAs not fully stacked but all rising — trend turning, not yet fully ordered")
        else:
            score += 1
            conditions.append("MAs are not bullishly stacked — trend structure is mixed or bearish")
            if status == "NEUTRAL": status = "UNSUPPORTIVE"

        c10 = df['Close'].tail(10)
        m20 = df['SMA20'].tail(10)
        m50 = df['SMA50'].tail(10)

        def reclaimed(pr, ma, lb=5):
            for i in range(1, min(lb, len(pr))):
                if not pd.isna(ma.iloc[i]) and pr.iloc[i] > ma.iloc[i] and pr.iloc[i-1] <= ma.iloc[i-1]:
                    return True
            return False

        high52 = float(df['Close'].tail(252).max())
        dist   = (price / high52 - 1) * 100

        if reclaimed(c10, m50):
            score += 10
            conditions.append("Price recently reclaimed the 50-day SMA — high-quality trend resumption signal")
        elif reclaimed(c10, m20):
            score += 7
            conditions.append("Price recently reclaimed the 20-day SMA — short-term trend structure improving")
        elif dist > -5:
            score += 9
            conditions.append(f"Price is within {abs(dist):.1f}% of 52-week high — trend extended and strong")
        elif dist > -15:
            score += 7
            conditions.append(f"Price is {abs(dist):.1f}% off 52-week high — trend intact with room to recover")
        elif dist > -30:
            score += 4
            conditions.append(f"Price is {abs(dist):.1f}% off 52-week high — significant recovery needed")
        else:
            score += 1
            conditions.append(f"Price is {abs(dist):.1f}% off 52-week high — trend deeply broken or base building")

        return {'score': score, 'max': 30, 'status': status, 'conditions': conditions}

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
        score, conditions, status = 0, [], "NEUTRAL"

        if 55 <= rsi <= 75:
            score += 8; status = "SUPPORTIVE"
            conditions.append(f"RSI at {rsi:.1f} — in the bullish momentum zone (55–75), ideal for swing continuation")
        elif 45 <= rsi < 55:
            score += 5
            conditions.append(f"RSI at {rsi:.1f} — approaching bullish zone from neutral, momentum building")
        elif rsi > 75:
            score += 4
            conditions.append(f"RSI at {rsi:.1f} — overbought; momentum strong but pullback risk elevated")
        elif 30 <= rsi < 45:
            score += 3; status = "UNSUPPORTIVE"
            conditions.append(f"RSI at {rsi:.1f} — weak momentum, recovery not yet confirmed")
        else:
            score += 2; status = "UNSUPPORTIVE"
            conditions.append(f"RSI at {rsi:.1f} — oversold; potential reversal but trend still bearish")

        cross50 = any(
            rsi_s.iloc[i] > 50 and rsi_s.iloc[i-1] <= 50
            for i in range(max(-6, -len(rsi_s)+1), 0)
        )
        if cross50 and rsi_d > 0:
            score += 7; status = "SUPPORTIVE"
            conditions.append(f"RSI recently crossed above 50 with upward trajectory ({rsi_d:+.1f} over 5d) — ignition signal")
        elif rsi > 50 and rsi_d > 2:
            score += 6
            conditions.append(f"RSI rising from {rsi5a:.1f} to {rsi:.1f} above midline — momentum accelerating")
            if status != "SUPPORTIVE": status = "SUPPORTIVE"
        elif rsi > 50 and rsi_d > 0:
            score += 4
            conditions.append(f"RSI above 50 and gently rising — bullish bias, not strongly accelerating yet")
        elif rsi > 50:
            score += 2
            conditions.append(f"RSI above 50 but declining — momentum fading, watch for reset to midline")
            if status == "SUPPORTIVE": status = "NEUTRAL"
        else:
            score += 1
            conditions.append(f"RSI below 50 with no recovery signal — bearish momentum bias persists")
            if status in ["NEUTRAL", "UNSUPPORTIVE"]: status = "UNSUPPORTIVE"

        mc  = df['MACD'].dropna()
        sc2 = df['MACD_Signal'].dropna()
        rbc = any(
            mc.iloc[i] > sc2.iloc[i] and mc.iloc[i-1] <= sc2.iloc[i-1]
            for i in range(max(-6, -min(len(mc), len(sc2))+1), 0)
        )
        hist_exp        = abs(hist) > abs(hist_p) and hist > 0
        hist_neg_shrink = hist < 0 and abs(hist) < abs(hist5)

        if rbc and macd > 0 and hist_exp:
            score += 10; status = "SUPPORTIVE"
            conditions.append("MACD: Fresh bullish cross above signal, expanding histogram above zero — strong momentum confirmation")
        elif rbc and hist_exp:
            score += 8
            conditions.append("MACD: Recent bullish crossover with expanding histogram — momentum turning positive")
            if status != "SUPPORTIVE": status = "SUPPORTIVE"
        elif macd > sig and hist_exp and macd > 0:
            score += 8
            conditions.append("MACD: Above signal with expanding positive histogram — momentum building")
            if status != "SUPPORTIVE": status = "SUPPORTIVE"
        elif macd > sig and macd > 0:
            score += 6
            conditions.append("MACD: Above signal in positive territory, histogram tightening — momentum stabilizing")
        elif macd > sig:
            score += 4
            conditions.append("MACD: Crossed above signal but still below zero — early-stage bullish cross, unconfirmed")
        elif hist_neg_shrink:
            score += 2
            conditions.append("MACD: Histogram negative but contracting — bearish momentum decelerating")
        else:
            score += 0
            conditions.append("MACD: Below signal with expanding negative histogram — bearish momentum dominant")
            if status in ["NEUTRAL", "UNSUPPORTIVE"]: status = "UNSUPPORTIVE"

        return {'score': score, 'max': 25, 'status': status, 'conditions': conditions}

    def analyze_volatility(self, df):
        clean    = df.dropna(subset=['BB_Width', 'ATR_Pct'])
        score, conditions, status = 0, [], "NEUTRAL"

        bb_now   = float(clean['BB_Width'].iloc[-1])
        bb_60d   = float(clean['BB_Width'].tail(60).mean())
        bb_ratio = bb_now / bb_60d if bb_60d > 0 else 1.0

        atr5  = float(clean['ATR_Pct'].tail(5).mean())
        atr60 = float(clean['ATR_Pct'].tail(60).mean())
        atr_r = atr5 / atr60 if atr60 > 0 else 1.0

        rng   = (df['High'] - df['Low']).tail(20) / df['Close'].tail(20) * 100
        r5    = float(rng.tail(5).mean())
        r20   = float(rng.mean())
        rng_r = r5 / r20 if r20 > 0 else 1.0

        if bb_ratio < 0.55:
            score += 9; status = "SUPPORTIVE"
            conditions.append(f"Bollinger Bands severely compressed ({bb_ratio:.0%} of 60d avg) — mature squeeze, high expansion potential")
        elif bb_ratio < 0.72:
            score += 7; status = "SUPPORTIVE"
            conditions.append(f"Bollinger Bands notably tighter than normal ({bb_ratio:.0%} of 60d avg) — significant volatility compression")
        elif bb_ratio < 0.88:
            score += 5
            conditions.append(f"Bollinger Bands slightly below 60d avg ({bb_ratio:.0%}) — mild compression developing")
        elif bb_ratio < 1.10:
            score += 3
            conditions.append(f"Bollinger Band width near historical average ({bb_ratio:.0%}) — no squeeze signal present")
        else:
            score += 1; status = "UNSUPPORTIVE"
            conditions.append(f"Bollinger Bands expanded ({bb_ratio:.0%} of 60d avg) — volatility already releasing")

        if rng_r < 0.58:
            score += 8; status = "SUPPORTIVE"
            conditions.append(f"Daily ranges sharply compressed (recent {r5:.1f}% vs 20d avg {r20:.1f}%) — stock is in a tight coil")
        elif rng_r < 0.75:
            score += 6
            conditions.append(f"Price ranges meaningfully narrower than recent history — tightening into a potential setup")
            if status != "SUPPORTIVE": status = "SUPPORTIVE"
        elif rng_r < 0.90:
            score += 4
            conditions.append(f"Price ranges slightly below average — modest compression, watch for further tightening")
        else:
            score += 2
            conditions.append(f"Daily ranges at or above average — no meaningful price compression in recent sessions")

        if atr_r < 0.62:
            score += 8; status = "SUPPORTIVE"
            conditions.append(f"ATR contracted sharply ({atr5:.1f}% vs 60d avg {atr60:.1f}%) — volatility at cycle low, coiling for expansion")
        elif atr_r < 0.78:
            score += 6
            conditions.append(f"ATR well below 60d average ({atr_r:.0%}) — meaningful volatility contraction in progress")
            if status != "SUPPORTIVE": status = "SUPPORTIVE"
        elif atr_r < 0.93:
            score += 4
            conditions.append(f"ATR slightly below 60d avg ({atr_r:.0%}) — mild softening, not a full contraction")
        else:
            score += 2
            conditions.append(f"ATR at or above historical average ({atr_r:.0%}) — volatility not contracting")

        return {'score': score, 'max': 25, 'status': status, 'conditions': conditions}

    def analyze_volume(self, df):
        clean    = df.dropna(subset=['Vol_MA20'])
        recent   = clean.tail(20)
        vol_ma20 = float(clean['Vol_MA20'].iloc[-1])
        score, conditions, status = 0, [], "NEUTRAL"

        up = recent[recent['Close'] > recent['Open']]
        dn = recent[recent['Close'] <= recent['Open']]
        uv = float(up['Volume'].mean()) if len(up) > 0 else 0
        dv = float(dn['Volume'].mean()) if len(dn) > 0 else 1

        if uv > 0 and dv > 0:
            acc = uv / dv
            if acc > 1.6:
                score += 8; status = "SUPPORTIVE"
                conditions.append(f"Volume is {acc:.1f}x higher on up-days vs down-days — clear institutional accumulation signature")
            elif acc > 1.25:
                score += 6; status = "SUPPORTIVE"
                conditions.append(f"Up-day volume moderately exceeds down-day volume ({acc:.1f}x) — mild accumulation bias")
            elif acc > 0.85:
                score += 3
                conditions.append(f"Up/down volume near parity ({acc:.1f}x) — neutral, no clear accumulation signal")
            else:
                score += 1; status = "UNSUPPORTIVE"
                conditions.append(f"Down-day volume exceeds up-day volume ({acc:.1f}x) — potential distribution pattern")
        else:
            score += 3
            conditions.append("Insufficient data to determine accumulation/distribution pattern")

        r5     = clean.tail(5)
        spikes = r5['Volume'] / vol_ma20
        mx_spk = float(spikes.max())

        if mx_spk > 2.5:
            idx   = spikes.idxmax()
            is_up = float(clean.loc[idx, 'Close']) > float(clean.loc[idx, 'Open'])
            if is_up:
                score += 5; status = "SUPPORTIVE"
                conditions.append(f"Volume spike of {mx_spk:.1f}x avg on an up-day in last 5 sessions — potential institutional buy program")
            else:
                score += 1
                conditions.append(f"Volume spike of {mx_spk:.1f}x avg on a down-day — possible distribution or stop-hunt")
                if status != "SUPPORTIVE": status = "UNSUPPORTIVE"
        elif mx_spk > 1.5:
            score += 3
            conditions.append(f"Moderate volume pickup ({mx_spk:.1f}x avg) in recent sessions — notable but not extreme")
        else:
            score += 1
            conditions.append(f"No unusual volume spikes in last 5 sessions — volume quiet near average levels")

        p5v  = float(clean['Volume'].tail(10).head(5).mean())
        r5v  = float(clean['Volume'].tail(5).mean())
        vr   = r5v / vol_ma20
        vchg = (r5v / p5v - 1) * 100 if p5v > 0 else 0
        prng = float(
            (clean['Close'].tail(5).max() - clean['Close'].tail(5).min())
            / clean['Close'].tail(5).mean() * 100
        )

        if prng < 3.0 and r5v < p5v * 0.80:
            score += 4; status = "SUPPORTIVE"
            conditions.append(f"Volume drying up ({vchg:.0f}% vs prior week) while price coils in {prng:.1f}% range — textbook pre-breakout setup")
        elif vr < 0.65:
            score += 3
            conditions.append(f"Volume well below 20d avg ({vr:.2f}x) — low-volume consolidation, typical before breakout")
            if status != "SUPPORTIVE": status = "SUPPORTIVE"
        elif vr < 0.85:
            score += 2
            conditions.append(f"Volume modestly below average ({vr:.2f}x) — mild drying up, constructive")
        else:
            score += 1
            conditions.append(f"Volume elevated ({vr:.2f}x avg) — active participation; watch price direction for context")

        adv = clean[clean['Close'] > clean['Open']].tail(3)
        if len(adv) > 0:
            avr = float((adv['Volume'] / vol_ma20).mean())
            if avr > 1.5:
                score += 3
                conditions.append(f"Recent bullish candles backed by {avr:.1f}x avg volume — buying demand confirmed")
                if status != "SUPPORTIVE": status = "SUPPORTIVE"
            elif avr > 1.0:
                score += 2
                conditions.append(f"Recent advances show average-to-above-average volume ({avr:.1f}x) — buying interest present")
            else:
                score += 1
                conditions.append(f"Recent advances on below-average volume ({avr:.1f}x) — buying conviction appears weak")

        return {'score': score, 'max': 20, 'status': status, 'conditions': conditions}

    def rate_stock(self, ticker):
        df = self.fetch_data(ticker)
        if df is None:
            return None
        df = self.calculate_indicators(df)

        adr_pct, dollar_volume = self.compute_adr_and_dolvol(df)

        trend      = self.analyze_trend(df)
        momentum   = self.analyze_momentum(df)
        volatility = self.analyze_volatility(df)
        volume     = self.analyze_volume(df)
        total      = trend['score'] + momentum['score'] + volatility['score'] + volume['score']

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
            'ticker':        ticker,
            'name':          name,
            'price':         round(float(df['Close'].iloc[-1]), 2),
            'total':         total,
            'grade':         grade,
            'verdict':       verdict,
            'adr_pct':       adr_pct,
            'dollar_volume': dollar_volume,
            'trend':         trend,
            'momentum':      momentum,
            'volatility':    volatility,
            'volume':        volume,
        }


def main():
    analyzer = SwingTradeAnalyzer()
    results  = []

    print(f"Scanning {len(UNIVERSE)} tickers  |  ADR > {ADR_MIN}%  |  Vol > ${VOL_MIN/1e6:.0f}M")

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(analyzer.rate_stock, t): t for t in UNIVERSE}
        for i, fut in enumerate(as_completed(futures), 1):
            t = futures[fut]
            try:
                r = fut.result()
                if r and r['adr_pct'] >= ADR_MIN and r['dollar_volume'] >= VOL_MIN:
                    results.append(r)
                    print(f"  [{i:>2}/{len(UNIVERSE)}] PASS  {t:<6}  {r['total']}/100 [{r['grade']}]  ADR {r['adr_pct']:.1f}%  Vol ${r['dollar_volume']/1e6:.0f}M")
                else:
                    print(f"  [{i:>2}/{len(UNIVERSE)}] skip  {t}")
            except Exception as e:
                print(f"  [{i:>2}/{len(UNIVERSE)}] ERR   {t}  —  {e}")

    results.sort(key=lambda x: x['total'], reverse=True)

    os.makedirs("data", exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump({
            'date':    str(date.today()),
            'count':   len(results),
            'filters': {'adr_min': ADR_MIN, 'vol_min': VOL_MIN},
            'results': results,
        }, f)

    print(f"\nDone — {len(results)} stocks passed filters → saved to {OUTPUT_FILE}")


if __name__ == '__main__':
    main()
