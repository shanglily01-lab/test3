#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MarketContextBuilder

从 Binance API 拉取多维实时数据，计算跨资产特征，
生成面向大模型推理的高密度语义上下文文档。

输出不是给人看的 —— 是为了让 LLM 发现人类未知的预测维度。
"""

import os
import time
import warnings
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
import pymysql
import requests
from dotenv import load_dotenv
from loguru import logger

warnings.filterwarnings("ignore")

load_dotenv()

# ─── Binance REST 基础 ────────────────────────────────────────────────────────
BINANCE_BASE  = "https://fapi.binance.com"
SPOT_BASE     = "https://api.binance.com"
_SESSION      = requests.Session()
_SESSION.headers.update({"X-MBX-APIKEY": os.getenv("BINANCE_API_KEY", "")})

_DB_CFG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "binance-data"),
    "charset":  "utf8mb4",
}


def _db_query(sql: str, args: tuple = ()) -> list:
    try:
        conn = pymysql.connect(**_DB_CFG)
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(sql, args)
            rows = cur.fetchall()
        conn.close()
        return rows
    except Exception as e:
        logger.warning(f"DB query failed: {e}")
        return []


def _get(url: str, params: dict = None, timeout: int = 10) -> list | dict:
    r = _SESSION.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


# ─── 数据拉取层 ───────────────────────────────────────────────────────────────

def fetch_klines(symbol: str, interval: str, limit: int = 500) -> pd.DataFrame:
    """拉取期货 K 线，返回带特征的 DataFrame。symbol 格式：BTCUSDT"""
    raw = _get(f"{BINANCE_BASE}/fapi/v1/klines",
               {"symbol": symbol, "interval": interval, "limit": limit})
    df = pd.DataFrame(raw, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_vol", "trades", "taker_buy_base",
        "taker_buy_quote", "ignore"
    ])
    for c in ["open", "high", "low", "close", "volume", "quote_vol",
              "taker_buy_base", "taker_buy_quote"]:
        df[c] = df[c].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
    return df


def fetch_funding_rate(symbol: str, limit: int = 30) -> pd.DataFrame:
    """拉取资金费率历史（每8小时一次）"""
    raw = _get(f"{BINANCE_BASE}/fapi/v1/fundingRate",
               {"symbol": symbol, "limit": limit})
    df = pd.DataFrame(raw)
    df["fundingRate"] = df["fundingRate"].astype(float)
    df["fundingTime"] = pd.to_datetime(df["fundingTime"], unit="ms")
    return df


def fetch_open_interest(symbol: str) -> dict:
    """当前持仓量（合约张数 + USDT 名义价值）"""
    return _get(f"{BINANCE_BASE}/fapi/v1/openInterest", {"symbol": symbol})


def fetch_oi_history(symbol: str, period: str = "1h", limit: int = 48) -> pd.DataFrame:
    """持仓量历史"""
    raw = _get(f"{BINANCE_BASE}/futures/data/openInterestHist",
               {"symbol": symbol, "period": period, "limit": limit})
    df = pd.DataFrame(raw)
    df["sumOpenInterest"] = df["sumOpenInterest"].astype(float)
    df["sumOpenInterestValue"] = df["sumOpenInterestValue"].astype(float)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    return df


def fetch_long_short_ratio(symbol: str, period: str = "1h", limit: int = 48) -> pd.DataFrame:
    """全局多空账户比例历史"""
    raw = _get(f"{BINANCE_BASE}/futures/data/globalLongShortAccountRatio",
               {"symbol": symbol, "period": period, "limit": limit})
    df = pd.DataFrame(raw)
    df["longAccount"]  = df["longAccount"].astype(float)
    df["shortAccount"] = df["shortAccount"].astype(float)
    df["longShortRatio"] = df["longShortRatio"].astype(float)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    return df


def fetch_liquidations(symbol: str, limit: int = 100) -> pd.DataFrame:
    """强平订单（反映极端情绪）"""
    try:
        raw = _get(f"{BINANCE_BASE}/fapi/v1/allForceOrders",
                   {"symbol": symbol, "limit": limit})
        df = pd.DataFrame(raw)
        if df.empty:
            return df
        df["price"]           = df["price"].astype(float)
        df["origQty"]         = df["origQty"].astype(float)
        df["executedQty"]     = df["executedQty"].astype(float)
        df["averagePrice"]    = df["averagePrice"].astype(float)
        df["time"]            = pd.to_datetime(df["time"], unit="ms")
        return df
    except Exception:
        return pd.DataFrame()


# ─── 特征计算层 ───────────────────────────────────────────────────────────────

def compute_rsi(series: pd.Series, period: int = 14) -> float:
    delta = series.diff().dropna()
    gain  = delta.clip(lower=0).rolling(period).mean().iloc[-1]
    loss  = (-delta.clip(upper=0)).rolling(period).mean().iloc[-1]
    if loss == 0:
        return 100.0
    rs = gain / loss
    return round(100 - 100 / (1 + rs), 2)


def compute_atr(df: pd.DataFrame, period: int = 14) -> float:
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs()
    ], axis=1).max(axis=1)
    return round(tr.rolling(period).mean().iloc[-1], 6)


def percentile_rank(series: pd.Series, value: float) -> int:
    """value 在 series 历史中的百分位（0-100）"""
    return int((series < value).mean() * 100)


def volume_ratio(df: pd.DataFrame, period: int = 20) -> float:
    avg = df["volume"].iloc[-period - 1:-1].mean()
    cur = df["volume"].iloc[-1]
    return round(cur / avg, 2) if avg > 0 else 1.0


def taker_buy_ratio(df: pd.DataFrame, n: int = 5) -> float:
    """近 n 根 K 线主动买入量占比（反映买方积极性）"""
    recent = df.tail(n)
    total  = recent["volume"].sum()
    buy    = recent["taker_buy_base"].sum()
    return round(buy / total, 3) if total > 0 else 0.5


def macd_signal(df: pd.DataFrame) -> dict:
    close = df["close"]
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd  = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    hist   = macd - signal
    return {
        "macd":    round(float(macd.iloc[-1]), 6),
        "signal":  round(float(signal.iloc[-1]), 6),
        "hist":    round(float(hist.iloc[-1]), 6),
        "trend":   "bullish" if hist.iloc[-1] > 0 else "bearish",
        "expanding": bool(abs(hist.iloc[-1]) > abs(hist.iloc[-2]))
    }


def ema_structure(df: pd.DataFrame) -> dict:
    """EMA 多头/空头排列"""
    close = df["close"]
    e20   = float(close.ewm(span=20).mean().iloc[-1])
    e50   = float(close.ewm(span=50).mean().iloc[-1])
    e200  = float(close.ewm(span=200).mean().iloc[-1])
    price = float(close.iloc[-1])
    above = sum([price > e20, price > e50, price > e200])
    return {
        "ema20": round(e20, 4), "ema50": round(e50, 4), "ema200": round(e200, 4),
        "price_above_ema_count": above,  # 0/3 = strongly below, 3/3 = strongly above
        "alignment": "bullish" if e20 > e50 > e200 else ("bearish" if e20 < e50 < e200 else "mixed")
    }


def detect_price_structure(df: pd.DataFrame, window: int = 20) -> dict:
    """识别价格结构：高高/高低/低高/低低"""
    highs  = df["high"].tail(window)
    lows   = df["low"].tail(window)
    close  = df["close"]

    hh = highs.iloc[-1] > highs.max()  # 新高
    ll = lows.iloc[-1]  < lows.min()   # 新低

    # 通道方向
    mid  = window // 2
    h_slope = (highs.iloc[-1] - highs.iloc[mid]) / mid
    l_slope = (lows.iloc[-1]  - lows.iloc[mid])  / mid

    return {
        "new_high": bool(hh),
        "new_low":  bool(ll),
        "channel":  "up" if h_slope > 0 and l_slope > 0 else
                    "down" if h_slope < 0 and l_slope < 0 else "sideways",
        "range_pct": round((highs.max() - lows.min()) / lows.min() * 100, 2)
    }


def find_pivot_levels(df_1h: pd.DataFrame, df_4h: pd.DataFrame) -> dict:
    """
    识别关键支撑/阻力位：
    - 1h 图上最近 48 根 K 线的摆动高/低点
    - 4h 图上最近 30 根 K 线的摆动高/低点
    - 24h 和 7d 的最高/最低价
    返回价格列表，用于告知 LLM 具体的价格坐标。
    """
    def swing_highs(highs: pd.Series, n: int = 3) -> list[float]:
        """找局部极大值（左右各 n 根都低于它）"""
        result = []
        for i in range(n, len(highs) - n):
            h = highs.iloc[i]
            if all(highs.iloc[i-n:i] < h) and all(highs.iloc[i+1:i+n+1] < h):
                result.append(round(float(h), 4))
        return sorted(set(result), reverse=True)[:4]  # 最近 4 个阻力

    def swing_lows(lows: pd.Series, n: int = 3) -> list[float]:
        """找局部极小值"""
        result = []
        for i in range(n, len(lows) - n):
            l = lows.iloc[i]
            if all(lows.iloc[i-n:i] > l) and all(lows.iloc[i+1:i+n+1] > l):
                result.append(round(float(l), 4))
        return sorted(set(result))[:4]  # 最近 4 个支撑

    price = float(df_1h["close"].iloc[-1])

    # 1h 摆动点（近 72 根）
    s1h = df_1h.tail(72)
    res_1h = swing_highs(s1h["high"])
    sup_1h = swing_lows(s1h["low"])

    # 4h 摆动点（近 30 根）
    s4h = df_4h.tail(30)
    res_4h = swing_highs(s4h["high"])
    sup_4h = swing_lows(s4h["low"])

    # 24h / 7d 区间
    h24 = float(df_1h["high"].tail(24).max())
    l24 = float(df_1h["low"].tail(24).min())
    h7d = float(df_1h["high"].tail(168).max())
    l7d = float(df_1h["low"].tail(168).min())

    # 合并支撑阻力，按距离当前价分层
    all_res = sorted(set(res_1h + res_4h), reverse=True)
    all_sup = sorted(set(sup_1h + sup_4h))

    nearest_res = [r for r in all_res if r > price][:3]
    nearest_sup = [s for s in all_sup if s < price][-3:]

    return {
        "price":        price,
        "resistance":   nearest_res,
        "support":      nearest_sup,
        "high_24h":     round(h24, 4),
        "low_24h":      round(l24, 4),
        "high_7d":      round(h7d, 4),
        "low_7d":       round(l7d, 4),
    }


# ─── 单币语义块生成 ───────────────────────────────────────────────────────────

def build_symbol_context(sym_slash: str) -> str:
    """
    为单个交易对生成语义上下文块。
    sym_slash: 'BTC/USDT'
    """
    symbol  = sym_slash.replace("/", "")   # BTCUSDT
    label   = sym_slash                    # BTC/USDT

    lines   = [f"\n{'='*64}", f"SYMBOL: {label}", f"{'='*64}"]

    # ── 拉取 K 线 ─────────────────────────────────────────────────
    try:
        df5m  = fetch_klines(symbol, "5m",  limit=200)
        df1h  = fetch_klines(symbol, "1h",  limit=168)
        df4h  = fetch_klines(symbol, "4h",  limit=90)
        df1d  = fetch_klines(symbol, "1d",  limit=90)
    except Exception as e:
        lines.append(f"ERROR: kline fetch failed: {e}")
        return "\n".join(lines)

    price_now = float(df1h["close"].iloc[-1])
    now_utc   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # ── 价格行情 ──────────────────────────────────────────────────
    ret1h  = (df1h["close"].iloc[-1]  / df1h["close"].iloc[-2]  - 1) * 100
    ret4h  = (df4h["close"].iloc[-1]  / df4h["close"].iloc[-4]  - 1) * 100
    ret24h = (df1h["close"].iloc[-1]  / df1h["close"].iloc[-25] - 1) * 100
    ret7d  = (df1d["close"].iloc[-1]  / df1d["close"].iloc[-8]  - 1) * 100

    pct_7d_range   = percentile_rank(df1d["close"].tail(30), price_now)
    pct_30d_range  = percentile_rank(df1d["close"].tail(90), price_now)

    lines += [
        f"\nPRICE_ACTION [as of {now_utc}]:",
        f"  Current : ${price_now:,.4f}",
        f"  Returns : 1h={ret1h:+.2f}%  4h={ret4h:+.2f}%  24h={ret24h:+.2f}%  7d={ret7d:+.2f}%",
        f"  Position: 7d_range={pct_7d_range}th_pct  30d_range={pct_30d_range}th_pct",
    ]

    # ── 关键价格坐标（支撑/阻力/区间）────────────────────────────
    try:
        atr1h_raw = compute_atr(df1h, 14)
        pivots = find_pivot_levels(df1h, df4h)
        ema1h_raw = ema_structure(df1h)
        # 24h ATR 预期波动范围
        atr24h_range_up   = round(price_now + atr1h_raw * 4, 4)
        atr24h_range_down = round(price_now - atr1h_raw * 4, 4)
        lines += [
            f"\nKEY_PRICE_LEVELS:",
            f"  Resistance: {' | '.join(f'${r:,.2f}' for r in pivots['resistance']) or 'none found'}",
            f"  Support   : {' | '.join(f'${s:,.2f}' for s in pivots['support']) or 'none found'}",
            f"  24h High/Low: ${pivots['high_24h']:,.2f} / ${pivots['low_24h']:,.2f}",
            f"  7d  High/Low: ${pivots['high_7d']:,.2f} / ${pivots['low_7d']:,.2f}",
            f"  EMA20={ema1h_raw['ema20']:,.2f}  EMA50={ema1h_raw['ema50']:,.2f}  EMA200={ema1h_raw['ema200']:,.2f}",
            f"  ATR_24h_expected_range: ${atr24h_range_down:,.2f} — ${atr24h_range_up:,.2f}",
        ]
    except Exception as e:
        lines.append(f"\nKEY_PRICE_LEVELS: partial ({e})")

    # ── 波动率 ────────────────────────────────────────────────────
    atr1h = compute_atr(df1h, 14)
    atr4h = compute_atr(df4h, 14)
    atr1h_pct = round(atr1h / price_now * 100, 3)
    atr4h_pct = round(atr4h / price_now * 100, 3)

    # ATR 压缩程度：当前 ATR vs 近 30 根的平均 ATR
    recent_atrs = []
    for i in range(30):
        sl = df1h.iloc[-(i+15):-i] if i > 0 else df1h.iloc[-15:]
        if len(sl) >= 14:
            recent_atrs.append(compute_atr(sl, 14))
    avg_atr = float(np.mean(recent_atrs)) if recent_atrs else atr1h
    atr_compression = round(atr1h / avg_atr, 2) if avg_atr > 0 else 1.0

    lines += [
        f"\nVOLATILITY:",
        f"  ATR_1h  : {atr1h_pct:.3f}% of price  "
        f"(vs 30-bar avg {round(avg_atr/price_now*100,3):.3f}%  "
        f"compression_ratio={atr_compression}x)",
        f"  ATR_4h  : {atr4h_pct:.3f}% of price",
        f"  Note    : {'COMPRESSED — spring tension building' if atr_compression < 0.75 else 'ELEVATED — trend active' if atr_compression > 1.3 else 'NORMAL'}",
    ]

    # ── 动量 ─────────────────────────────────────────────────────
    rsi1h = compute_rsi(df1h["close"], 14)
    rsi4h = compute_rsi(df4h["close"], 14)
    macd1h = macd_signal(df1h)
    ema1h  = ema_structure(df1h)
    struct1d = detect_price_structure(df1d, 20)

    lines += [
        f"\nMOMENTUM:",
        f"  RSI_1h  : {rsi1h}  ({'oversold <30' if rsi1h < 30 else 'overbought >70' if rsi1h > 70 else 'neutral'})",
        f"  RSI_4h  : {rsi4h}  ({'oversold <30' if rsi4h < 30 else 'overbought >70' if rsi4h > 70 else 'neutral'})",
        f"  MACD_1h : hist={macd1h['hist']:.6f}  trend={macd1h['trend']}  "
        f"{'EXPANDING' if macd1h['expanding'] else 'contracting'}",
        f"  EMA_1h  : alignment={ema1h['alignment']}  price_above={ema1h['price_above_ema_count']}/3 EMAs",
        f"  Structure (1d): channel={struct1d['channel']}  "
        f"new_high={struct1d['new_high']}  new_low={struct1d['new_low']}  "
        f"range={struct1d['range_pct']:.2f}%",
    ]

    # ── 成交量 ───────────────────────────────────────────────────
    vol_ratio_1h  = volume_ratio(df1h,  20)
    vol_ratio_4h  = volume_ratio(df4h,  20)
    tbr_1h        = taker_buy_ratio(df1h, 6)
    tbr_4h        = taker_buy_ratio(df4h, 6)

    lines += [
        f"\nVOLUME:",
        f"  1h vol_ratio : {vol_ratio_1h}x vs 20-bar avg  "
        f"({'HIGH' if vol_ratio_1h > 1.5 else 'LOW' if vol_ratio_1h < 0.7 else 'normal'})",
        f"  4h vol_ratio : {vol_ratio_4h}x vs 20-bar avg",
        f"  Taker_buy_1h : {tbr_1h:.1%} of volume  "
        f"({'buyers aggressive >55%' if tbr_1h > 0.55 else 'sellers aggressive <45%' if tbr_1h < 0.45 else 'balanced'})",
        f"  Taker_buy_4h : {tbr_4h:.1%} of volume",
        f"  Price_vol_divergence: {'YES — price falling with LOW volume (weak selling)' if ret1h < 0 and vol_ratio_1h < 0.8 else 'YES — price rising with LOW volume (weak buying)' if ret1h > 0 and vol_ratio_1h < 0.8 else 'none'}",
    ]

    # ── 资金费率 ──────────────────────────────────────────────────
    try:
        fr_df = fetch_funding_rate(symbol, limit=30)
        fr_now   = float(fr_df["fundingRate"].iloc[-1])
        fr_prev  = float(fr_df["fundingRate"].iloc[-2])
        fr_3ago  = float(fr_df["fundingRate"].iloc[-4])
        fr_mean  = float(fr_df["fundingRate"].mean())
        fr_pct   = percentile_rank(fr_df["fundingRate"], fr_now)
        fr_trend = "increasing_negative" if fr_now < fr_prev < fr_3ago else \
                   "decreasing_negative" if fr_now > fr_prev > fr_3ago and fr_now < 0 else \
                   "increasing_positive" if fr_now > fr_prev > fr_3ago else \
                   "decreasing_positive" if fr_now < fr_prev < fr_3ago and fr_now > 0 else "mixed"
        lines += [
            f"\nFUNDING_RATE:",
            f"  Current  : {fr_now*100:.4f}%  ({'shorts paying longs (bearish sentiment)' if fr_now < 0 else 'longs paying shorts (bullish sentiment)'})",
            f"  Trend    : {fr_prev*100:.4f}% -> {fr_now*100:.4f}%  direction={fr_trend}",
            f"  30-period mean: {fr_mean*100:.4f}%  current_percentile={fr_pct}th",
            f"  Interpretation: {'EXTREME bearish positioning — potential short squeeze fuel' if fr_pct < 10 else 'EXTREME bullish positioning — crowded longs, long squeeze risk' if fr_pct > 90 else 'moderate'}",
        ]
    except Exception as e:
        lines.append(f"\nFUNDING_RATE: unavailable ({e})")

    # ── 持仓量 ────────────────────────────────────────────────────
    try:
        oi_now   = fetch_open_interest(symbol)
        oi_hist  = fetch_oi_history(symbol, "1h", limit=48)
        oi_val   = float(oi_now["openInterest"])
        oi_usd   = float(oi_hist["sumOpenInterestValue"].iloc[-1])
        oi_1h_chg  = (oi_hist["sumOpenInterestValue"].iloc[-1] / oi_hist["sumOpenInterestValue"].iloc[-2] - 1) * 100
        oi_24h_chg = (oi_hist["sumOpenInterestValue"].iloc[-1] / oi_hist["sumOpenInterestValue"].iloc[-25] - 1) * 100
        oi_pct     = percentile_rank(oi_hist["sumOpenInterestValue"], oi_usd)

        # OI vs 价格散度
        if oi_24h_chg > 2 and ret24h > 0:
            oi_interp = "OI RISING + price rising = trend continuation (healthy buildup)"
        elif oi_24h_chg > 2 and ret24h < 0:
            oi_interp = "OI RISING + price falling = new shorts entering (bearish conviction)"
        elif oi_24h_chg < -2 and ret24h < 0:
            oi_interp = "OI FALLING + price falling = DELEVERAGING/liquidations (capitulation signal)"
        elif oi_24h_chg < -2 and ret24h > 0:
            oi_interp = "OI FALLING + price rising = short covering (potential squeeze exhaustion)"
        else:
            oi_interp = "stable OI — no significant position change"

        lines += [
            f"\nOPEN_INTEREST:",
            f"  Current   : {oi_usd/1e9:.2f}B USDT  percentile={oi_pct}th vs 48h history",
            f"  Change    : 1h={oi_1h_chg:+.2f}%  24h={oi_24h_chg:+.2f}%",
            f"  Interpretation: {oi_interp}",
        ]
    except Exception as e:
        lines.append(f"\nOPEN_INTEREST: unavailable ({e})")

    # ── 多空比 ────────────────────────────────────────────────────
    try:
        ls_df    = fetch_long_short_ratio(symbol, "1h", limit=48)
        ls_now   = float(ls_df["longShortRatio"].iloc[-1])
        ls_24ago = float(ls_df["longShortRatio"].iloc[-25]) if len(ls_df) >= 25 else ls_now
        ls_pct   = percentile_rank(ls_df["longShortRatio"], ls_now)
        long_acc = float(ls_df["longAccount"].iloc[-1])
        short_acc = float(ls_df["shortAccount"].iloc[-1])

        lines += [
            f"\nLONG_SHORT_RATIO:",
            f"  Account L/S : {ls_now:.3f}  (long_acct={long_acc:.1%}  short_acct={short_acc:.1%})",
            f"  24h change  : {ls_24ago:.3f} -> {ls_now:.3f}  ({'+' if ls_now>ls_24ago else ''}{(ls_now/ls_24ago-1)*100:.1f}%)",
            f"  Percentile  : {ls_pct}th vs 48h history",
            f"  Signal      : {'EXTREME long crowd — contrarian bearish' if ls_pct > 85 else 'EXTREME short crowd — contrarian bullish' if ls_pct < 15 else 'balanced positioning'}",
        ]
    except Exception as e:
        lines.append(f"\nLONG_SHORT_RATIO: unavailable ({e})")

    # ── 强平数据 ─────────────────────────────────────────────────
    try:
        liq_df = fetch_liquidations(symbol, limit=100)
        if not liq_df.empty:
            long_liq  = liq_df[liq_df["side"] == "SELL"]["origQty"].sum()
            short_liq = liq_df[liq_df["side"] == "BUY"]["origQty"].sum()
            total_liq_usd = (liq_df["origQty"] * liq_df["averagePrice"]).sum()
            lines += [
                f"\nRECENT_LIQUIDATIONS (last 100 events):",
                f"  Long_liq   : {long_liq:.2f} contracts ({long_liq/(long_liq+short_liq+1e-9):.0%} of total)",
                f"  Short_liq  : {short_liq:.2f} contracts",
                f"  Total_USD  : ${total_liq_usd/1e6:.1f}M",
                f"  Dominance  : {'LONGS being liquidated — selling pressure' if long_liq > short_liq*1.5 else 'SHORTS being liquidated — buying pressure' if short_liq > long_liq*1.5 else 'balanced'}",
            ]
    except Exception:
        pass

    # ── 资金费率速度（velocity）─────────────────────────────────
    try:
        sym_slash = label  # e.g. BTC/USDT
        fr_rows = _db_query(
            "SELECT funding_rate, mark_price, index_price, timestamp "
            "FROM funding_rate_data WHERE symbol=%s "
            "ORDER BY timestamp DESC LIMIT 12",
            (sym_slash,)
        )
        if len(fr_rows) >= 4:
            rates = [float(r["funding_rate"]) for r in fr_rows]
            # velocity: change per period (8h interval)
            vel_1p = rates[0] - rates[1]           # last period change
            vel_3p = (rates[0] - rates[3]) / 3     # 3-period avg velocity
            vel_6p = (rates[0] - rates[6]) / 6 if len(fr_rows) >= 7 else None
            accel  = vel_1p - (rates[1] - rates[2]) # acceleration
            # Perp premium: (mark - index) / index
            mark  = float(fr_rows[0]["mark_price"] or 0)
            index = float(fr_rows[0]["index_price"] or 0)
            premium_pct = (mark / index - 1) * 100 if index > 0 else 0
            # historical premium distribution (last 48 periods)
            premium_rows = _db_query(
                "SELECT mark_price, index_price FROM funding_rate_data "
                "WHERE symbol=%s AND mark_price IS NOT NULL ORDER BY timestamp DESC LIMIT 48",
                (sym_slash,)
            )
            premiums = [(float(r["mark_price"]) / float(r["index_price"]) - 1) * 100
                        for r in premium_rows if float(r["index_price"] or 0) > 0]
            if premiums:
                prem_pct_rank = round(sum(1 for p in premiums if p <= premium_pct) / len(premiums) * 100)
            else:
                prem_pct_rank = 50

            trend_desc = (
                "accelerating positive (shorts paying more, squeeze building)"
                if vel_1p > 0 and accel > 0
                else "decelerating positive (squeeze momentum fading)"
                if vel_1p > 0 and accel < 0
                else "accelerating negative (longs paying more, flush building)"
                if vel_1p < 0 and accel < 0
                else "decelerating negative (flush momentum fading)"
                if vel_1p < 0 and accel > 0
                else "flat"
            )
            lines += [
                f"\nFUNDING_VELOCITY:",
                f"  Current rate    : {rates[0]*100:.5f}%",
                f"  Velocity 1p     : {vel_1p*100:+.5f}% (change last 8h)",
                f"  Velocity 3p avg : {vel_3p*100:+.5f}% per period",
                f"  Acceleration    : {accel*100:+.5f}% ({trend_desc})",
                f"  Perp premium    : {premium_pct:+.5f}% (mark vs index, {prem_pct_rank}th pct of last 48 periods)",
            ]
    except Exception as e:
        logger.debug(f"Funding velocity calc failed: {e}")

    # ── Hyperliquid 聪明钱信号 ────────────────────────────────────
    try:
        sym_base = label.split("/")[0]  # BTC
        hl_rows = _db_query(
            "SELECT net_flow, long_trades, short_trades, long_short_ratio, "
            "total_volume, avg_trade_size, max_trade_size, "
            "total_pnl, win_rate, hyperliquid_score, hyperliquid_signal, sentiment, updated_at "
            "FROM hyperliquid_symbol_aggregation "
            "WHERE symbol=%s AND period='24h' "
            "ORDER BY updated_at DESC LIMIT 1",
            (sym_base,)
        )
        if hl_rows:
            h = hl_rows[0]
            net    = float(h["net_flow"] or 0)
            lsr    = float(h["long_short_ratio"] or 0)
            score  = float(h["hyperliquid_score"] or 0)
            signal = h["hyperliquid_signal"] or "NEUTRAL"
            sent   = h["sentiment"] or "neutral"
            vol    = float(h["total_volume"] or 0)
            avg_sz = float(h["avg_trade_size"] or 0)
            max_sz = float(h["max_trade_size"] or 0)
            wr     = float(h["win_rate"] or 0)
            pnl    = float(h["total_pnl"] or 0)
            lt     = int(h["long_trades"] or 0)
            st     = int(h["short_trades"] or 0)
            updated = h["updated_at"]
            lines += [
                f"\nHYPERLIQUID_SMART_MONEY (24h, as of {updated}):",
                f"  Signal        : {signal} (score={score}/100, sentiment={sent})",
                f"  Net flow      : ${net:+,.0f} ({'inflow' if net > 0 else 'outflow'})",
                f"  L/S ratio     : {lsr:.3f} ({lt} longs vs {st} shorts)",
                f"  Volume        : ${vol:,.0f}  avg_size=${avg_sz:,.0f}  max_size=${max_sz:,.0f}",
                f"  Smart P&L     : ${pnl:+,.0f}  win_rate={wr:.1f}%",
                f"  Interpretation: {'Smart money accumulating — high conviction longs' if signal in ('STRONG_BULLISH','BULLISH') and net > 0 else 'Smart money distributing — high conviction shorts' if signal in ('STRONG_BEARISH','BEARISH') and net < 0 else 'Smart money neutral or mixed signals'}",
            ]
    except Exception as e:
        logger.debug(f"Hyperliquid context failed: {e}")

    # ── 跨交易所溢价 ──────────────────────────────────────────────
    try:
        xex_rows = _db_query(
            "SELECT binance_price, okx_price, bybit_price, "
            "okx_spread_pct, bybit_spread_pct, "
            "okx_funding_rate, bybit_funding_rate, collected_at "
            "FROM cross_exchange_prices WHERE symbol=%s "
            "ORDER BY collected_at DESC LIMIT 3",
            (label,)
        )
        if xex_rows:
            x = xex_rows[0]
            okx_s  = float(x["okx_spread_pct"] or 0) if x["okx_spread_pct"] is not None else None
            byb_s  = float(x["bybit_spread_pct"] or 0) if x["bybit_spread_pct"] is not None else None
            okx_fr = float(x["okx_funding_rate"] or 0) if x["okx_funding_rate"] is not None else None
            byb_fr = float(x["bybit_funding_rate"] or 0) if x["bybit_funding_rate"] is not None else None
            spread_lines = []
            if okx_s is not None:
                spread_lines.append(f"OKX={okx_s:+.4f}%")
            if byb_s is not None:
                spread_lines.append(f"Bybit={byb_s:+.4f}%")
            if spread_lines:
                # Interpret: positive = Binance premium over others (selling pressure there)
                max_abs = max((abs(okx_s or 0), abs(byb_s or 0)))
                interp = (
                    "Binance trading at premium — arbitrageurs will sell Binance, buy elsewhere"
                    if (okx_s or 0) > 0.05 and (byb_s or 0) > 0.05
                    else "Binance trading at discount — arbitrageurs will buy Binance, sell elsewhere"
                    if (okx_s or 0) < -0.05 and (byb_s or 0) < -0.05
                    else "spreads within normal range (<0.05%)"
                )
                fr_comp = []
                if okx_fr is not None:
                    fr_comp.append(f"OKX={okx_fr*100:.5f}%")
                if byb_fr is not None:
                    fr_comp.append(f"Bybit={byb_fr*100:.5f}%")
                lines += [
                    f"\nCROSS_EXCHANGE_SPREAD:",
                    f"  Price spread  : {', '.join(spread_lines)} vs Binance",
                    f"  Funding rates : {', '.join(fr_comp)} (vs Binance current)",
                    f"  Interpretation: {interp}",
                ]
    except Exception as e:
        logger.debug(f"Cross-exchange context failed: {e}")

    return "\n".join(lines)


# ─── 跨资产分析块 ─────────────────────────────────────────────────────────────

def build_cross_asset_context(symbols: list[str]) -> str:
    """
    计算四个币种之间的相关性、领先-滞后、相对强弱。
    symbols: ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT']
    """
    lines = [f"\n{'='*64}", "CROSS_ASSET_ANALYSIS", f"{'='*64}"]

    # 拉取所有 1h 收盘价
    closes = {}
    for s in symbols:
        sym = s.replace("/", "")
        try:
            df = fetch_klines(sym, "1h", limit=168)
            closes[s] = df.set_index("open_time")["close"]
        except Exception as e:
            lines.append(f"  {s}: fetch failed ({e})")

    if len(closes) < 2:
        return "\n".join(lines)

    df_closes = pd.DataFrame(closes).dropna()
    df_ret    = df_closes.pct_change().dropna()

    # 相关矩阵（近 7 天）
    corr = df_ret.tail(168).corr()
    lines.append("\nCORRELATION_MATRIX (7d rolling 1h returns):")
    for s1 in symbols:
        row = []
        for s2 in symbols:
            if s1 == s2:
                row.append(f"{s2.split('/')[0]:6s}= 1.00")
            else:
                v = corr.loc[s1, s2] if (s1 in corr.index and s2 in corr.columns) else float('nan')
                row.append(f"{s2.split('/')[0]:6s}={v:.2f}")
        lines.append(f"  {s1.split('/')[0]:6s}: {' | '.join(row)}")

    # 相对强弱：各币 vs BTC
    btc_col = "BTC/USDT"
    if btc_col in df_ret.columns:
        lines.append("\nRELATIVE_STRENGTH vs BTC (1h / 4h / 24h):")
        for s in symbols:
            if s == btc_col or s not in df_closes.columns:
                continue
            r1h  = (df_closes[s].iloc[-1]  / df_closes[s].iloc[-2]  - 1) * 100
            r4h  = (df_closes[s].iloc[-1]  / df_closes[s].iloc[-5]  - 1) * 100
            r24h = (df_closes[s].iloc[-1]  / df_closes[s].iloc[-25] - 1) * 100
            b1h  = (df_closes[btc_col].iloc[-1] / df_closes[btc_col].iloc[-2]  - 1) * 100
            b4h  = (df_closes[btc_col].iloc[-1] / df_closes[btc_col].iloc[-5]  - 1) * 100
            b24h = (df_closes[btc_col].iloc[-1] / df_closes[btc_col].iloc[-25] - 1) * 100
            d1h  = r1h  - b1h
            d4h  = r4h  - b4h
            d24h = r24h - b24h
            signal = "OUTPERFORMING" if d4h > 1 else "UNDERPERFORMING" if d4h < -1 else "in-line"
            lines.append(
                f"  {s.split('/')[0]:6s}: 1h={d1h:+.2f}pp  4h={d4h:+.2f}pp  24h={d24h:+.2f}pp  => {signal}"
            )

    # 领先-滞后检测（BTC vs 其他，1-3 小时滞后）
    lines.append("\nLEAD_LAG_DETECTION (does BTC lead/lag others by 1-3h):")
    if btc_col in df_ret.columns:
        btc_ret = df_ret[btc_col].tail(72)
        for s in symbols:
            if s == btc_col or s not in df_ret.columns:
                continue
            s_ret = df_ret[s].tail(72)
            lags  = {}
            for lag in [1, 2, 3]:
                aligned_btc = btc_ret.iloc[:-lag]
                aligned_s   = s_ret.iloc[lag:]
                if len(aligned_btc) < 5:
                    continue
                corr_lag = aligned_btc.corr(aligned_s)
                lags[lag] = round(corr_lag, 3)
            best_lag = max(lags, key=lambda k: abs(lags[k])) if lags else 0
            lines.append(
                f"  BTC -> {s.split('/')[0]:6s}: lag1h={lags.get(1,'n/a')}  lag2h={lags.get(2,'n/a')}  "
                f"lag3h={lags.get(3,'n/a')}  best_lag={best_lag}h"
            )

    # ETH/BTC 比值趋势（市场风险偏好信号）
    if "ETH/USDT" in df_closes.columns:
        eth_btc = df_closes["ETH/USDT"] / df_closes["BTC/USDT"]
        eb_1h   = (eth_btc.iloc[-1] / eth_btc.iloc[-2]  - 1) * 100
        eb_24h  = (eth_btc.iloc[-1] / eth_btc.iloc[-25] - 1) * 100
        lines += [
            f"\nETH/BTC_RATIO (risk appetite proxy):",
            f"  1h change : {eb_1h:+.3f}%  24h change: {eb_24h:+.3f}%",
            f"  Signal    : {'RISK_ON — altcoins leading, market bullish' if eb_24h > 1 else 'RISK_OFF — BTC dominance rising, market defensice' if eb_24h < -1 else 'neutral'}",
        ]

    # ── Fear & Greed Index ────────────────────────────────────────
    try:
        fng_rows = _db_query(
            "SELECT fng_value, fng_label, recorded_date FROM market_sentiment "
            "ORDER BY recorded_date DESC LIMIT 3"
        )
        if fng_rows:
            latest = fng_rows[0]
            val   = latest["fng_value"]
            label_fng = latest["fng_label"]
            history = " | ".join(f"{r['fng_label']}({r['fng_value']})" for r in fng_rows)
            lines += [
                f"\nFEAR_AND_GREED_INDEX:",
                f"  Today         : {val} — {label_fng}",
                f"  Last 3 days   : {history}",
                f"  Interpretation: {'extreme fear often precedes bounces — contrarian buy signal' if val <= 25 else 'extreme greed often precedes corrections — contrarian sell signal' if val >= 75 else 'neutral zone, no strong contrarian signal'}",
            ]
    except Exception as e:
        logger.debug(f"Fear & Greed context failed: {e}")

    # ── 全网强平热力图（最近1h）────────────────────────────────────
    try:
        liq_rows = _db_query(
            "SELECT symbol, side, SUM(usd_value) as total_usd, COUNT(*) as cnt "
            "FROM global_liquidations "
            "WHERE event_time >= UTC_TIMESTAMP() - INTERVAL 1 HOUR "
            "GROUP BY symbol, side ORDER BY total_usd DESC LIMIT 20"
        )
        if liq_rows:
            total_long_liq = sum(float(r["total_usd"] or 0) for r in liq_rows if r["side"] == "SELL")
            total_short_liq = sum(float(r["total_usd"] or 0) for r in liq_rows if r["side"] == "BUY")
            total = total_long_liq + total_short_liq
            top = sorted(liq_rows, key=lambda r: float(r["total_usd"] or 0), reverse=True)[:5]
            top_str = " | ".join(
                f"{r['symbol']} {'long' if r['side']=='SELL' else 'short'} ${float(r['total_usd']or 0)/1e6:.2f}M"
                for r in top
            )
            lines += [
                f"\nGLOBAL_LIQUIDATION_HEATMAP (last 1h):",
                f"  Long liquidated : ${total_long_liq/1e6:.2f}M ({total_long_liq/total*100:.0f}% of total)" if total > 0 else "  Long liquidated : $0",
                f"  Short liquidated: ${total_short_liq/1e6:.2f}M ({total_short_liq/total*100:.0f}% of total)" if total > 0 else "  Short liquidated: $0",
                f"  Top events      : {top_str}",
                f"  Market stress   : {'HIGH — cascading long liquidations, potential capitulation' if total_long_liq > total_short_liq * 2 else 'HIGH — cascading short liquidations, potential short squeeze' if total_short_liq > total_long_liq * 2 else 'LOW — balanced liquidations'}",
            ]
    except Exception as e:
        logger.debug(f"Global liquidation heatmap failed: {e}")

    return "\n".join(lines)


# ─── 发现查询模板 ─────────────────────────────────────────────────────────────

_SEP = "=" * 64

DISCOVERY_PROMPT_TEMPLATE = """
{sep}
TRADING_SIGNAL_ANALYSIS — {timestamp}
Symbols: {symbols}
{sep}

You are a professional crypto quantitative trader. Your job is to convert multi-dimensional market data into precise, actionable trade plans for the next 24 hours.

Below is real-time multi-dimensional data for {symbols}, including:
- Exact price coordinates (support/resistance/EMA/24h range)
- Momentum structure (RSI/MACD/EMA alignment)
- Volume characteristics (taker buy ratio / price-volume divergence)
- Positioning sentiment (funding rate / long-short ratio / liquidations)
- FUNDING_VELOCITY: rate of change and acceleration of funding rates (novel signal)
- HYPERLIQUID_SMART_MONEY: professional trader net flow, win rate, L/S ratio from Hyperliquid (leading indicator)
- CROSS_EXCHANGE_SPREAD: Binance vs OKX vs Bybit price premium (arbitrage pressure signal)
- GLOBAL_LIQUIDATION_HEATMAP: real-time forced liquidation volume by direction (cascade risk signal)
- FEAR_AND_GREED_INDEX: market-wide sentiment for contrarian signals
- Cross-asset correlations and lead-lag relationships

{context_body}

{sep}
TASK: Generate one actionable trade plan per symbol for the next 24 hours.

Rules:
1. Each plan MUST include exact entry zone, stop loss, and two profit targets derived from KEY_PRICE_LEVELS
2. Win rate must be estimated from signal confluence statistics (NOT gut feeling):
   - 1 signal (e.g. RSI oversold only): base rate ~52-55%
   - 2 signals aligned: 55-62%
   - 3 signals aligned: 62-70%
   - 4+ signals aligned: 68-75%
   - Extreme sentiment reversal (funding rate <5th pct + price at multiple supports): up to 70-78%
   - Conflicting signals: reduce win rate
3. If signals are contradictory or unclear, output direction "SKIP" and explain why
4. Stop loss must be placed just beyond the nearest support/resistance level (avoid noise stops)
5. confidence field: 1-10 scale (not a percentage)
6. ALL text fields must be in ENGLISH

Respond with ONLY valid JSON, no extra text, no markdown:
{{
  "trade_plans": [
    {{
      "symbol": "BTC",
      "direction": "LONG or SHORT or SKIP",
      "entry_zone": {{"low": 0.0, "high": 0.0}},
      "stop_loss": 0.0,
      "target1": 0.0,
      "target2": 0.0,
      "win_rate_pct": 0,
      "risk_reward": 0.0,
      "confidence": 0,
      "time_window": "e.g. enter within 4h, hold 12-24h",
      "entry_trigger": "specific condition to trigger entry, e.g. 1h close above EMA20",
      "invalidation": "condition that cancels this plan",
      "confluence_signals": ["signal 1", "signal 2", "signal 3"],
      "win_rate_basis": "which signals contribute to win rate estimate and why they combine to this level"
    }}
  ],
  "market_regime": "trending_up or trending_down or ranging or volatile_reversal",
  "cross_asset_insight": "single most important cross-asset observation for trading, max 60 words",
  "highest_conviction_trade": "symbol with highest win rate"
}}
"""


# ─── 主入口 ───────────────────────────────────────────────────────────────────

class MarketContextBuilder:
    """组装完整的多维上下文文档，供 PatternDiscoveryAgent 调用"""

    DEFAULT_SYMBOLS = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]

    def __init__(self, symbols: list[str] = None):
        self.symbols = symbols or self.DEFAULT_SYMBOLS

    def build(self) -> str:
        """
        返回完整的 LLM-ready 上下文字符串（不含 DISCOVERY_QUERY）。
        PatternDiscoveryAgent 会在此基础上附加查询模板。
        """
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        header = [
            "MARKET_INTELLIGENCE_CONTEXT",
            f"Generated: {ts}",
            f"Symbols: {', '.join(self.symbols)}",
            f"Purpose: LLM pattern discovery — NOT for human display",
            "=" * 64,
        ]
        parts = ["\n".join(header)]

        for sym in self.symbols:
            logger.info(f"Building context for {sym}...")
            try:
                parts.append(build_symbol_context(sym))
            except Exception as e:
                logger.error(f"{sym} context failed: {e}")
                parts.append(f"\n{sym}: BUILD_FAILED — {e}")
            time.sleep(0.3)  # Binance rate-limit buffer

        logger.info("Building cross-asset context...")
        try:
            parts.append(build_cross_asset_context(self.symbols))
        except Exception as e:
            logger.error(f"Cross-asset context failed: {e}")

        return "\n".join(parts)

    def build_with_query(self) -> str:
        """返回完整上下文 + 发现查询，直接送给 LLM"""
        body = self.build()
        ts   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        query = DISCOVERY_PROMPT_TEMPLATE.format(
            symbols=", ".join(self.symbols),
            timestamp=ts,
            context_body=body,
            sep=_SEP,
        )
        return query
