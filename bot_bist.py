#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Entegre BIST Scanner + TeknikAnaliz sınıfı + Grafik gönderimi (trend çizgisi + fiyat etiketleri)
- Orijinal bot_bist (2).py taban alındı, TeknikAnaliz sınıfı entegre edildi.
- Telegram mesajlarına grafik (mum + indikatörler + trend + etiketler + RSI alt paneli) eklenir.
"""

import io
import os
import asyncio
import aiohttp
from aiohttp import web
import datetime as dt
import logging
import json
from io import BytesIO
import time
from datetime import timezone, timedelta
from typing import List, Tuple, Dict, Optional, Set, Any
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import pandas as pd
import math
import yfinance as yf
import pytz
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from scipy.signal import find_peaks
from dotenv import load_dotenv

# Load .env
load_dotenv()

# --- AYARLAR VE GÜVENLİK ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
COLLECTAPI_KEY = os.getenv("COLLECTAPI_KEY")  # (mevcutsa)

if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
    raise ValueError("Ortam değişkenleri (TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID) ayarlanmalı!")

LOG_FILE = os.getenv('LOG_FILE', 'trading_bot.log')
DATA_CACHE_DIR = "data_cache"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
HEALTH_CHECK_PORT = int(os.getenv("PORT", 8080))
IST_TZ = pytz.timezone('Europe/Istanbul')

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
logger = logging.getLogger("bist_scanner")

# ----------------------- TARAMA AYARLARI -----------------------
MARKET_OPEN_HOUR = 9
MARKET_CLOSE_HOUR = 18
MARKET_CLOSE_MINUTE = 30
DAILY_REPORT_HOUR = 18
DAILY_REPORT_MINUTE = 30

def is_market_hours() -> bool:
    now_ist = dt.datetime.now(IST_TZ)
    if now_ist.weekday() >= 5: return False
    market_open = dt.time(MARKET_OPEN_HOUR, 0)
    market_close = dt.time(MARKET_CLOSE_HOUR, MARKET_CLOSE_MINUTE)
    return market_open <= now_ist.time() <= market_close

# Hisse listesi (orijinal dosyadan alındı; uzun olduğu için burada sabit bırakıldı)
ALL_BIST_STOCKS = [
    # (kısaltılmış gösterim — tam listeyi orijinal dosyandan al)
    "THYAO","SAHOL","ASTOR","AKBNK","SISE","BIMAS","EREGL","KCHOL","PETKM","VAKBN",
    # ... (orijinal listeni buraya koy)
]
SCAN_MODE = os.getenv("SCAN_MODE", "ALL")
CUSTOM_TICKERS_STR = os.getenv("CUSTOM_TICKERS", "")
if SCAN_MODE == "CUSTOM" and CUSTOM_TICKERS_STR:
    TICKERS = [t.strip() for t in CUSTOM_TICKERS_STR.split(',')]
else:
    TICKERS = ALL_BIST_STOCKS

CHECK_EVERY_MIN = int(os.getenv("CHECK_EVERY_MIN", "15"))
TIMEFRAMES = [t.strip() for t in os.getenv("TIMEFRAMES", "").split(',') if t.strip()] or ["15m", "1h", "4h", "1d"]
MIN_PRICE = float(os.getenv("MIN_PRICE", "1.0"))
MIN_VOLUME_TRY = float(os.getenv("MIN_VOLUME_TRY", "5000000"))
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "2.0"))
RSI_LEN = int(os.getenv("RSI_LEN", "30"))
RSI_EMA_LEN = int(os.getenv("RSI_EMA", "99"))
PIVOT_PERIOD = int(os.getenv("PIVOT_PERIOD", "15"))
MACD_FAST = int(os.getenv("MACD_FAST", "12"))
MACD_SLOW = int(os.getenv("MACD_SLOW", "26"))
MACD_SIGNAL = int(os.getenv("MACD_SIGNAL", "9"))
BB_PERIOD = int(os.getenv("BB_PERIOD", "20"))
BB_MULT = float(os.getenv("BB_MULT", "2.0"))
BB_NEAR_PCT = float(os.getenv("BB_NEAR_PCT", "0.03"))
STOCHRSI_PERIOD = int(os.getenv("STOCHRSI_PERIOD", "14"))
STOCHRSI_K = int(os.getenv("STOCHRSI_K", "3"))
STOCHRSI_D = int(os.getenv("STOCHRSI_D", "3"))
MA_SHORT = int(os.getenv("MA_SHORT", "50"))
MA_LONG = int(os.getenv("MA_LONG", "200"))
MIN_SIGNAL_SCORE = float(os.getenv("MIN_SIGNAL_SCORE", "3.0"))

LAST_SCAN_TIME: Optional[dt.datetime] = None
START_TIME = time.time()
DAILY_SIGNALS: Dict[str, Dict] = {}

@dataclass
class SignalInfo:
    symbol: str
    timeframe: str
    direction: str
    price: float
    rsi: float
    rsi_ema: float
    volume_ratio: float
    volume_try: float
    macd_signal: str
    bb_position: str
    strength_score: float
    timestamp: str
    breakout_angle: Optional[float] = None
    candle_formation: Optional[str] = None
    multi_tf_score: Optional[float] = None
    stochrsi_k: Optional[float] = None
    stochrsi_d: Optional[float] = None
    ma_cross: Optional[str] = None
    tsi_value: Optional[float] = None
    sar_status: Optional[str] = None
    breakout_coords: Optional[Tuple[int, float, int, float]] = None

# ---------------- TEKNİK ANALİZ SINIFI (ENTEGRASYON) ----------------
class TeknikAnaliz:
    @staticmethod
    def _to_df(klines: List[List[Any]]) -> pd.DataFrame:
        if len(klines) < 50:
            raise ValueError(f"Yetersiz veri: {len(klines)} < 50")
        cols = [
            "open_time","open","high","low","close","volume",
            "close_time","quote_asset_volume","number_of_trades",
            "taker_buy_base_vol","taker_buy_quote_vol","ignore",
        ]
        df = pd.DataFrame(klines, columns=cols)
        for c in ["open","high","low","close","volume"]:
            df[c] = pd.to_numeric(df[c], errors='coerce')
        df = df.dropna(subset=["open","high","low","close","volume"])
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
        if len(df) < 50:
            raise ValueError(f"Temizleme sonrası yetersiz veri: {len(df)} < 50")
        return df

    @staticmethod
    def rsi(series: pd.Series, period: int = 14) -> pd.Series:
        if len(series) < period + 5:
            return pd.Series([50] * len(series), index=series.index)
        delta = series.diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        for i in range(period, len(series)):
            avg_gain.iloc[i] = (avg_gain.iloc[i-1] * (period - 1) + gain.iloc[i]) / period
            avg_loss.iloc[i] = (avg_loss.iloc[i-1] * (period - 1) + loss.iloc[i]) / period
        rs = avg_gain / avg_loss.replace(0, 1e-10)
        rsi = 100 - (100 / (1 + rs))
        return rsi.fillna(50)

    @staticmethod
    def bollinger_bands(close: pd.Series, period: int = 20, mult: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
        if len(close) < period:
            mid = pd.Series([close.mean()] * len(close), index=close.index)
            std_val = close.std() if len(close) > 1 else close.iloc[0] * 0.02
            upper = mid + mult * std_val
            lower = mid - mult * std_val
            return lower, mid, upper
        mid = close.rolling(window=period, min_periods=1).mean()
        std = close.rolling(window=period, min_periods=1).std()
        std = std.fillna(close.std() if len(close) > 1 else close.iloc[0] * 0.02)
        upper = mid + mult * std
        lower = mid - mult * std
        return lower, mid, upper

    @staticmethod
    def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        if len(high) < 2:
            return pd.Series([high.iloc[0] - low.iloc[0] if len(high) > 0 else 0.01] * len(high), index=high.index)
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.ewm(span=period, adjust=False).mean()
        return atr.fillna(tr.mean())

    @staticmethod
    def stoch_rsi(close: pd.Series, period: int = 14, k: int = 3, d: int = 3) -> Tuple[pd.Series, pd.Series]:
        rsi = TeknikAnaliz.rsi(close, period)
        if len(rsi) < period:
            k_line = pd.Series([50] * len(rsi), index=rsi.index)
            d_line = pd.Series([50] * len(rsi), index=rsi.index)
            return k_line, d_line
        min_rsi = rsi.rolling(window=period, min_periods=1).min()
        max_rsi = rsi.rolling(window=period, min_periods=1).max()
        stoch = (rsi - min_rsi) / (max_rsi - min_rsi).replace(0, 1e-10)
        stoch = stoch.fillna(0.5)
        k_line = stoch.rolling(window=k, min_periods=1).mean() * 100
        d_line = k_line.rolling(window=d, min_periods=1).mean()
        return k_line.fillna(50), d_line.fillna(50)

    @staticmethod
    def obv(close: pd.Series, volume: pd.Series) -> pd.Series:
        if len(close) < 2:
            return volume.cumsum()
        direction = np.sign(close.diff().fillna(0))
        obv = (direction * volume).cumsum()
        return obv

    @staticmethod
    def vwap(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series) -> pd.Series:
        typical = (high + low + close) / 3
        vwap = (typical * volume).cumsum() / volume.cumsum()
        return vwap.fillna(typical)

    @staticmethod
    def simple_trend(close: pd.Series, short_period: int = 10, long_period: int = 30) -> pd.Series:
        if len(close) < long_period:
            return pd.Series([0] * len(close), index=close.index)
        short_ma = close.rolling(window=short_period, min_periods=1).mean()
        long_ma = close.rolling(window=long_period, min_periods=1).mean()
        trend = np.where(short_ma > long_ma, 1, -1)
        return pd.Series(trend, index=close.index)

    @staticmethod
    def rsi_divergence(df: pd.DataFrame, lookback: int = 15) -> float:
        close = df["close"]
        rsi = TeknikAnaliz.rsi(close)
        if len(close) < lookback + 5:
            return 0.0
        close_change = close.iloc[-1] - close.iloc[-lookback]
        rsi_change = rsi.iloc[-1] - rsi.iloc[-lookback]
        if close_change < 0 and rsi_change > 5:
            return 1.0
        elif close_change > 0 and rsi_change < -5:
            return -1.0
        return 0.0

    @staticmethod
    def detect_patterns(df: pd.DataFrame) -> float:
        close = df["close"].values
        if len(close) < 10:
            return 0.0
        recent = close[-10:]
        if len(recent) >= 5:
            min_idx = np.argmin(recent)
            if min_idx > 0 and min_idx < len(recent) - 1:
                if recent[-1] > recent[min_idx] and recent[0] > recent[min_idx]:
                    return 0.8
        if recent[-1] > recent[0]:
            if recent[-3:].min() > recent[:3].min():
                return 0.5
        else:
            if recent[-3:].max() < recent[:3].max():
                return -0.5
        return 0.0

    @staticmethod
    def detect_downtrend_breakout(df: pd.DataFrame, lookback: int = 60):
        if len(df) < 5:
            return 0.0, None
        highs = df['high'].iloc[-lookback:]
        slice_len = len(highs)
        if slice_len < 6:
            return 0.0, None
        peaks, _ = find_peaks(highs.values, distance=5, prominence=np.nanstd(highs.values) * 0.5)
        if len(peaks) < 2:
            return 0.0, None
        peak1_pos, peak2_pos = peaks[-2], peaks[-1]
        slice_start = len(df) - slice_len
        idx1, idx2 = slice_start + int(peak1_pos), slice_start + int(peak2_pos)
        y1, y2 = float(df['high'].iloc[idx1]), float(df['high'].iloc[idx2])
        if y1 > y2:
            m = (y2 - y1) / (peak2_pos - peak1_pos) if (peak2_pos - peak1_pos) != 0 else 0.0
            c = y1 - m * peak1_pos
            last_trendline_price = m * (slice_len - 1) + c
            last_price = float(df['close'].iloc[-1])
            if last_price > last_trendline_price:
                return 1.8, (idx1, y1, idx2, y2)
        return 0.0, None

    @staticmethod
    def analyze_df(df: pd.DataFrame) -> Dict[str, Any]:
        try:
            close = df["close"]
            high, low, vol = df["high"], df["low"], df["volume"]
            rsi_val = TeknikAnaliz.rsi(close)
            bb_low, bb_mid, bb_up = TeknikAnaliz.bollinger_bands(close)
            atr_val = TeknikAnaliz.atr(high, low, close)
            st_k, st_d = TeknikAnaliz.stoch_rsi(close)
            obv_val = TeknikAnaliz.obv(close, vol)
            vwap_val = TeknikAnaliz.vwap(high, low, close, vol)
            trend_val = TeknikAnaliz.simple_trend(close)
            rsi_div = TeknikAnaliz.rsi_divergence(df)
            pattern_score = TeknikAnaliz.detect_patterns(df)
            breakout_score, breakout_coords = TeknikAnaliz.detect_downtrend_breakout(df)
            return {
                "rsi": rsi_val,
                "bb_low": bb_low,
                "bb_mid": bb_mid,
                "bb_up": bb_up,
                "atr": atr_val,
                "stoch_k": st_k,
                "stoch_d": st_d,
                "obv": obv_val,
                "vwap": vwap_val,
                "trend": trend_val,
                "divergence": rsi_div,
                "pattern": pattern_score,
                "breakout_score": breakout_score,
                "breakout_coords": breakout_coords
            }
        except Exception as e:
            logger.error(f"Analiz hatası: {e}")
            return {
                "rsi": pd.Series([50] * len(df)),
                "bb_low": df["close"] * 0.98,
                "bb_mid": df["close"],
                "bb_up": df["close"] * 1.02,
                "atr": df["close"] * 0.02,
                "stoch_k": pd.Series([50] * len(df)),
                "stoch_d": pd.Series([50] * len(df)),
                "obv": df["volume"].cumsum(),
                "vwap": df["close"],
                "trend": pd.Series([0] * len(df)),
                "divergence": 0.0,
                "pattern": 0.0,
                "breakout_score": 0.0,
                "breakout_coords": None
            }

    @staticmethod
    def signals_from_indicators(df: pd.DataFrame, ind: Dict[str, Any]) -> Dict[str, float]:
        try:
            close = df["close"]
            last_rsi = float(ind["rsi"].iloc[-1])
            if last_rsi < 25:
                score_rsi = 1.5
            elif last_rsi < 35:
                score_rsi = 1.0
            elif last_rsi > 75:
                score_rsi = -1.5
            elif last_rsi > 65:
                score_rsi = -1.0
            else:
                score_rsi = 0.0
            last_close = close.iloc[-1]
            last_bb_low = ind["bb_low"].iloc[-1]
            last_bb_up = ind["bb_up"].iloc[-1]
            last_bb_mid = ind["bb_mid"].iloc[-1]
            bb_position = (last_close - last_bb_low) / (last_bb_up - last_bb_low) if (last_bb_up - last_bb_low) != 0 else 0.5
            if bb_position < 0.1:
                score_bb = 1.2
            elif bb_position < 0.2:
                score_bb = 0.8
            elif bb_position > 0.9:
                score_bb = -1.2
            elif bb_position > 0.8:
                score_bb = -0.8
            else:
                score_bb = 0.0
            k_val = float(ind["stoch_k"].iloc[-1]) if hasattr(ind["stoch_k"], "iloc") else float(ind["stoch_k"])
            d_val = float(ind["stoch_d"].iloc[-1]) if hasattr(ind["stoch_d"], "iloc") else float(ind["stoch_d"])
            if k_val < 15 and d_val < 15:
                score_stoch = 1.0
            elif k_val > 85 and d_val > 85:
                score_stoch = -1.0
            elif k_val > d_val and k_val < 30:
                score_stoch = 0.8
            elif k_val < d_val and k_val > 70:
                score_stoch = -0.8
            else:
                score_stoch = 0.0
            trend_val = float(ind["trend"].iloc[-1]) if hasattr(ind["trend"], "iloc") else float(ind["trend"])
            score_trend = trend_val * 0.5
            score_obv = 0.0
            if len(ind["obv"]) >= 10:
                recent_obv = ind["obv"].iloc[-10:].values
                if len(recent_obv) >= 2:
                    obv_slope = np.polyfit(range(len(recent_obv)), recent_obv, 1)[0]
                    if obv_slope > 0:
                        score_obv = 0.4
                    elif obv_slope < 0:
                        score_obv = -0.4
            score_vwap = 0.3 if last_close > ind["vwap"].iloc[-1] else -0.3
            divergence_score = float(ind.get("divergence", 0.0))
            pattern_score = float(ind.get("pattern", 0.0))
            breakout_score = float(ind.get("breakout_score", 0.0))
            return {
                "rsi": score_rsi,
                "bb": score_bb,
                "stoch": score_stoch,
                "trend": score_trend,
                "obv": score_obv,
                "vwap": score_vwap,
                "divergence": divergence_score,
                "pattern": pattern_score,
                "breakout": breakout_score
            }
        except Exception as e:
            logger.error(f"Sinyal hesaplama hatası: {e}")
            return {
                "rsi": 0.0, "bb": 0.0, "stoch": 0.0, "trend": 0.0,
                "obv": 0.0, "vwap": 0.0, "divergence": 0.0, "pattern": 0.0, "breakout": 0.0
            }

# ------------------ Yardımcı TA fonksiyonları (orijinal) ------------------
def rma(series: pd.Series, length: int) -> pd.Series:
    alpha = 1.0 / length
    return series.ewm(alpha=alpha, adjust=False).mean()

def rsi_from_close(close: pd.Series, length: int) -> pd.Series:
    delta = close.diff()
    up = np.where(delta > 0, delta, 0.0)
    down = np.where(delta < 0, -delta, 0.0)
    up_rma = rma(pd.Series(up, index=close.index), length)
    down_rma = rma(pd.Series(down, index=close.index), length)
    rs = up_rma / down_rma.replace(0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.fillna(0.0)

def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()

def sma(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(window=length).mean()

def calculate_macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    ema_fast = ema(close, fast)
    ema_slow = ema(close, slow)
    macd_line = ema_fast - ema_slow
    signal_line = ema(macd_line, signal)
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def calculate_bollinger_bands(close: pd.Series, period: int = 20, mult: float = 2.0):
    middle = sma(close, period)
    std = close.rolling(window=period).std()
    upper = middle + (std * mult)
    lower = middle - (std * mult)
    return upper, middle, lower

def calculate_stoch_rsi(rsi_series: pd.Series, length: int = 14, k: int = 3, d: int = 3):
    min_rsi = rsi_series.rolling(window=length).min()
    max_rsi = rsi_series.rolling(window=length).max()
    stoch_rsi = (rsi_series - min_rsi) / (max_rsi - min_rsi)
    stoch_rsi = stoch_rsi.fillna(0) * 100
    stoch_rsi_k = stoch_rsi.rolling(window=k).mean()
    stoch_rsi_d = stoch_rsi_k.rolling(window=d).mean()
    if stoch_rsi_k.empty or stoch_rsi_d.empty:
        return None, None
    return float(stoch_rsi_k.iloc[-1]), float(stoch_rsi_d.iloc[-1])

def detect_ma_cross(close_series: pd.Series, short_period: int, long_period: int) -> Optional[str]:
    if len(close_series) < max(short_period, long_period) + 1:
        return None
    short_ma = sma(close_series, short_period)
    long_ma = sma(close_series, long_period)
    if short_ma.iloc[-1] > long_ma.iloc[-1] and short_ma.iloc[-2] <= long_ma.iloc[-2]:
        return "GOLDEN_CROSS"
    elif short_ma.iloc[-1] < long_ma.iloc[-1] and short_ma.iloc[-2] >= long_ma.iloc[-2]:
        return "DEATH_CROSS"
    return None

def find_pivots(arr: pd.Series, left: int, right: int) -> Tuple[List[int], List[int]]:
    highs, lows = [], []
    n = len(arr)
    for i in range(left, n - right):
        window = arr.iloc[i-left:i+right+1]
        if arr.iloc[i] == window.max():
            highs.append(i)
        if arr.iloc[i] == window.min():
            lows.append(i)
    return highs, lows

def trendline_from_pivots(idx1: int, val1: float, idx2: int, val2: float) -> Tuple[float, float]:
    if idx2 == idx1:
        return 0.0, val2
    m = (val2 - val1) / (idx2 - idx1)
    b = val2 - m * idx2
    return m, b

def value_on_line(m: float, b: float, x: float) -> float:
    return m * x + b

def detect_breakouts(rsi: pd.Series, highs_idx: List[int], lows_idx: List[int], now_i: int) -> Tuple[bool, bool, Optional[float]]:
    bull_break, bear_break, breakout_angle = False, False, None
    if len(highs_idx) >= 2:
        i2, i1 = highs_idx[-1], highs_idx[-2]
        v2, v1 = rsi.iloc[i2], rsi.iloc[i1]
        if v2 < v1:
            m, b = trendline_from_pivots(i1, v1, i2, v2)
            if rsi.iloc[now_i] > value_on_line(m, b, now_i):
                bull_break = True
                breakout_angle = math.degrees(math.atan(m))
    if len(lows_idx) >= 2:
        j2, j1 = lows_idx[-1], lows_idx[-2]
        w2, w1 = rsi.iloc[j2], rsi.iloc[j1]
        if w2 > w1:
            m2, b2 = trendline_from_pivots(j1, w1, j2, w2)
            if rsi.iloc[now_i] < value_on_line(m2, b2, now_i):
                bear_break = True
                breakout_angle = math.degrees(math.atan(m2))
    return bull_break, bear_break, breakout_angle

# --- Mum formasyon tespit fonksiyonları (kopyalandı) ---
def is_doji(open_p, close_p, high_p, low_p, body_size_threshold=0.1):
    body = abs(close_p - open_p)
    full_range = high_p - low_p
    return full_range > 0 and body < full_range * body_size_threshold

def is_hammer(open_p, close_p, low_p, high_p):
    body = abs(close_p - open_p)
    lower_shadow = min(open_p, close_p) - low_p
    upper_shadow = high_p - max(open_p, close_p)
    return lower_shadow > 2 * body and upper_shadow < body

def is_inverted_hammer(open_p, close_p, low_p, high_p):
    body = abs(close_p - open_p)
    lower_shadow = min(open_p, close_p) - low_p
    upper_shadow = high_p - max(open_p, close_p)
    return upper_shadow > 2 * body and lower_shadow < body

def is_hanging_man(open_p, close_p, low_p, high_p):
    body = abs(close_p - open_p)
    lower_shadow = min(open_p, close_p) - low_p
    upper_shadow = high_p - max(open_p, close_p)
    return lower_shadow > 2 * body and upper_shadow < body

def is_shooting_star(open_p, close_p, low_p, high_p):
    body = abs(close_p - open_p)
    lower_shadow = min(open_p, close_p) - low_p
    upper_shadow = high_p - max(open_p, close_p)
    return upper_shadow > 2 * body and lower_shadow < body

def is_bullish_engulfing(prev_c, last_c):
    return (
        last_c['close'] > last_c['open'] and
        prev_c['close'] < prev_c['open'] and
        last_c['close'] > prev_c['open'] and
        last_c['open'] < prev_c['close']
    )

def is_bearish_engulfing(prev_c, last_c):
    return (
        last_c['close'] < last_c['open'] and
        prev_c['close'] > prev_c['open'] and
        last_c['close'] < prev_c['open'] and
        last_c['open'] > prev_c['close']
    )

def is_piercing_pattern(df):
    if len(df) < 2: return False
    prev_c, last_c = df.iloc[-2], df.iloc[-1]
    return (
        prev_c['close'] < prev_c['open'] and
        last_c['close'] > last_c['open'] and
        last_c['open'] < prev_c['close'] and
        last_c['close'] > (prev_c['open'] + prev_c['close']) / 2
    )

def is_dark_cloud_cover(df):
    if len(df) < 2: return False
    prev_c, last_c = df.iloc[-2], df.iloc[-1]
    return (
        prev_c['close'] > prev_c['open'] and
        last_c['close'] < last_c['open'] and
        last_c['open'] > prev_c['close'] and
        last_c['close'] < (prev_c['open'] + prev_c['close']) / 2
    )

def is_bullish_harami(prev_c, last_c):
    return (
        prev_c['close'] < prev_c['open'] and
        last_c['close'] > last_c['open'] and
        last_c['low'] > prev_c['low'] and
        last_c['high'] < prev_c['high']
    )

def is_bearish_harami(prev_c, last_c):
    return (
        prev_c['close'] > prev_c['open'] and
        last_c['close'] < last_c['open'] and
        last_c['low'] > prev_c['low'] and
        last_c['high'] < prev_c['high']
    )

def is_tweezer_bottoms(df):
    if len(df) < 2: return False
    prev_c, last_c = df.iloc[-2], df.iloc[-1]
    return (
        abs(prev_c['low'] - last_c['low']) / last_c['low'] < 0.001 and
        prev_c['close'] < prev_c['open'] and
        last_c['close'] > last_c['open']
    )

def is_tweezer_tops(df):
    if len(df) < 2: return False
    prev_c, last_c = df.iloc[-2], df.iloc[-1]
    return (
        abs(prev_c['high'] - last_c['high']) / last_c['high'] < 0.001 and
        prev_c['close'] > prev_c['open'] and
        last_c['close'] < prev_c['close']
    )

def is_three_white_soldiers(df):
    if len(df) < 3: return False
    c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    return (
        c1['close'] > c1['open'] and c2['close'] > c2['open'] and c3['close'] > c3['open'] and
        c2['open'] >= c1['open'] and c2['close'] >= c1['close'] and
        c3['open'] >= c2['open'] and c3['close'] >= c2['close']
    )

def is_three_black_crows(df):
    if len(df) < 3: return False
    c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    return (
        c1['close'] < c1['open'] and c2['close'] < c2['open'] and c3['close'] < c3['open'] and
        c2['open'] <= c1['open'] and c2['close'] <= c1['close'] and
        c3['open'] <= c2['open'] and c3['close'] <= c2['close']
    )

def is_morning_star(df):
    if len(df) < 3: return False
    c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    return (
        c1['close'] < c1['open'] and c1['open'] - c1['close'] > abs(c1['high'] - c1['low']) * 0.6 and
        abs(c2['open'] - c2['close']) < abs(c2['high'] - c2['low']) * 0.4 and
        c3['close'] > c3['open'] and c3['close'] > (c1['open'] + c1['close']) / 2
    )

def is_evening_star(df):
    if len(df) < 3: return False
    c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    return (
        c1['close'] > c1['open'] and c1['close'] - c1['open'] > abs(c1['high'] - c1['low']) * 0.6 and
        abs(c2['open'] - c2['close']) < abs(c2['high'] - c2['low']) * 0.4 and
        c3['close'] < c3['open'] and c3['close'] < (c1['open'] + c1['close']) / 2
    )

def detect_candle_formation(df: pd.DataFrame) -> Optional[str]:
    if len(df) < 3:
        return None
    last_candle = df.iloc[-1]
    prev_candle = df.iloc[-2]
    open_p, high_p, low_p, close_p = last_candle['open'], last_candle['high'], last_candle['low'], last_candle['close']
    if is_three_white_soldiers(df): return "Three White Soldiers"
    if is_three_black_crows(df): return "Three Black Crows"
    if is_morning_star(df): return "Morning Star"
    if is_evening_star(df): return "Evening Star"
    if is_bullish_engulfing(prev_candle, last_candle): return "Bullish Engulfing"
    if is_bearish_engulfing(prev_candle, last_candle): return "Bearish Engulfing"
    if is_piercing_pattern(df): return "Piercing Pattern"
    if is_dark_cloud_cover(df): return "Dark Cloud Cover"
    if is_bullish_harami(prev_candle, last_candle): return "Bullish Harami"
    if is_bearish_harami(prev_candle, last_candle): return "Bearish Harami"
    if is_tweezer_bottoms(df): return "Tweezer Bottoms"
    if is_tweezer_tops(df): return "Tweezer Tops"
    if is_doji(open_p, close_p, high_p, low_p): return "Doji"
    if is_hammer(open_p, close_p, low_p, high_p): return "Hammer"
    if is_inverted_hammer(open_p, close_p, low_p, high_p): return "Inverted Hammer"
    if is_hanging_man(open_p, close_p, low_p, high_p): return "Hanging Man"
    if is_shooting_star(open_p, close_p, low_p, high_p): return "Shooting Star"
    return None

def calculate_tsi(close: pd.Series, long_period: int = 25, short_period: int = 13, signal_period: int = 13) -> pd.Series:
    diff = close.diff()
    abs_diff = abs(diff)
    ema_diff_long = ema(diff, long_period)
    ema_abs_diff_long = ema(abs_diff, long_period)
    ema_diff_short = ema(ema_diff_long, short_period)
    ema_abs_diff_short = ema(ema_abs_diff_long, short_period)
    tsi = 100 * (ema_diff_short / ema_abs_diff_short.replace(0, np.nan))
    return tsi.fillna(0.0)

def calculate_sar(high: pd.Series, low: pd.Series, acceleration: float = 0.02, maximum: float = 0.2) -> pd.Series:
    sar = [0] * len(high)
    trend = "BULLISH"
    ep = high.iloc[0]
    af = acceleration
    for i in range(1, len(high)):
        prev_sar = sar[i-1] if i > 1 else high.iloc[0]
        if trend == "BULLISH":
            sar[i] = prev_sar + af * (ep - prev_sar)
            if sar[i] > low.iloc[i]:
                sar[i] = high.iloc[i-1]
            if low.iloc[i] < sar[i]:
                trend = "BEARISH"
                af = acceleration
                sar[i] = ep
                ep = low.iloc[i]
            else:
                if high.iloc[i] > ep:
                    ep = high.iloc[i]
                    af = min(af + acceleration, maximum)
        else:
            sar[i] = prev_sar - af * (prev_sar - ep)
            if sar[i] < high.iloc[i]:
                sar[i] = low.iloc[i-1]
            if high.iloc[i] > sar[i]:
                trend = "BULLISH"
                af = acceleration
                sar[i] = ep
                ep = high.iloc[i]
            else:
                if low.iloc[i] < ep:
                    ep = low.iloc[i]
                    af = min(af + acceleration, maximum)
    return pd.Series(sar, index=high.index)

def calculate_signal_strength(signal: SignalInfo) -> float:
    score = 5.0
    if signal.timeframe == '15m':
        score += 2.5
    elif signal.timeframe == '1h':
        score += 1.5
    elif signal.timeframe == '4h':
        score += 0.5
    if signal.direction == "BULLISH":
        if signal.rsi < 30: score += 3.0
        elif signal.rsi < 50: score += 1.0
    else:
        if signal.rsi > 70: score += 3.0
        elif signal.rsi > 50: score += 1.0
    if signal.volume_ratio > 4.0: score += 4.0
    elif signal.volume_ratio > 3.0: score += 3.0
    elif signal.volume_ratio > 2.0: score += 2.0
    elif signal.volume_ratio > 1.5: score += 1.0
    if (signal.direction == "BULLISH" and signal.macd_signal == "BULLISH") or \
       (signal.direction == "BEARISH" and signal.macd_signal == "BEARISH"):
        score += 2.5
    if (signal.direction == "BULLISH" and signal.rsi > signal.rsi_ema) or \
       (signal.direction == "BEARISH" and signal.rsi < signal.rsi_ema):
        score += 1.0
    if signal.bb_position in ("NEAR_LOWER", "LOWER") and signal.direction == "BULLISH":
        score += 0.5
    if signal.bb_position in ("NEAR_UPPER", "UPPER") and signal.direction == "BEARISH":
        score += 0.5
    if signal.price > 10: score += 0.5
    if signal.breakout_angle is not None:
        angle_abs = abs(signal.breakout_angle)
        if angle_abs > 30: score += 3.0
        elif angle_abs > 15: score += 2.0
        elif angle_abs > 5: score += 1.0
    if signal.candle_formation:
        if signal.candle_formation in ["Morning Star", "Three White Soldiers", "Bullish Engulfing"]:
            score += 4.0
        elif signal.candle_formation in ["Hammer", "Piercing Pattern", "Bullish Harami", "Tweezer Bottoms"]:
            score += 3.0
        elif signal.candle_formation in ["Inverted Hammer"]:
            score += 2.0
        if signal.candle_formation in ["Evening Star", "Three Black Crows", "Bearish Engulfing"]:
            score += 4.0
        elif signal.candle_formation in ["Shooting Star", "Dark Cloud Cover", "Bearish Harami", "Tweezer Tops"]:
            score += 3.0
        elif signal.candle_formation in ["Hanging Man"]:
            score += 2.0
    if signal.stochrsi_k is not None and signal.stochrsi_d is not None:
        if signal.direction == "BULLISH" and signal.stochrsi_k > signal.stochrsi_d and signal.stochrsi_k < 20:
            score += 2.0
        elif signal.direction == "BEARISH" and signal.stochrsi_k < signal.stochrsi_d and signal.stochrsi_k > 80:
            score += 2.0
    if signal.ma_cross:
        if signal.direction == "BULLISH" and signal.ma_cross == "GOLDEN_CROSS":
            score += 4.0
        elif signal.direction == "BEARISH" and signal.ma_cross == "DEATH_CROSS":
            score += 4.0
    if signal.tsi_value is not None:
        if signal.direction == "BULLISH" and signal.tsi_value > 0:
            score += 2.0
        elif signal.direction == "BEARISH" and signal.tsi_value < 0:
            score += 2.0
    if signal.sar_status == 'BULLISH' and signal.direction == 'BULLISH':
        score += 2.5
    elif signal.sar_status == 'BEARISH' and signal.direction == 'BEARISH':
        score += 2.5
    return float(max(0.0, min(10.0, score)))



# ------------------ Veri çekme ve analiz ------------------
def _resample_ohlcv(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    cols = {"Open":"open","High":"high","Low":"low","Close":"close","Volume":"volume"}
    df.rename(columns=cols, inplace=True)
    agg = {'open': 'first','high': 'max','low': 'min','close': 'last','volume': 'sum'}
    out = df.resample(rule.replace('H', 'h')).agg(agg).dropna()
    return out

def _standardize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.rename(columns={'Open':'open','High':'high','Low':'low','Close':'close','Volume':'volume'}, inplace=True)
    for c in ['Dividends','Stock Splits']:
        if c in df.columns:
            df.drop(columns=[c], inplace=True)
    return df

def fetch_history(symbol: str, yf_interval: str, period: str) -> pd.DataFrame:
    stock = yf.Ticker(symbol)
    df = stock.history(interval=yf_interval, period=period, auto_adjust=False)
    return df

def classify_bb_position(price: float, upper: float, lower: float, near_pct: float) -> str:
    if price >= upper: return "UPPER"
    if price <= lower: return "LOWER"
    band_range = upper - lower if upper > lower else 0
    if band_range > 0:
        if price >= upper - band_range * near_pct:
            return "NEAR_UPPER"
        if price <= lower + band_range * near_pct:
            return "NEAR_LOWER"
    return "MIDDLE"

# --- GRAFİK OLUŞTUR VE TELEGRAM'A GÖNDER (MUM + TREND + ETİKET) ---

async def send_chart_to_telegram(token: str, chat_id: str, title: str, df: pd.DataFrame, ind: Dict[str, Any]):
    try:
        # matplotlib backend'ini ayarla
        matplotlib.use("Agg")
        
        # Grafik alanını ve panelleri oluştur
        fig, (ax_price, ax_rsi) = plt.subplots(2, 1, figsize=(12, 8), gridspec_kw={'height_ratios': [3, 1]})

        # Veri hazırlığı
        xs = np.arange(len(df))
        opens = df['open'].astype(float).values
        highs = df['high'].astype(float).values
        lows = df['low'].astype(float).values
        closes = df['close'].astype(float).values
        width = 0.6
        half_width = width / 2.0

        # Mum grafiği çizimi
        for i in range(len(df)):
            o, h, l, c = opens[i], highs[i], lows[i], closes[i]
            # Çizgi gölgeyi temsil eder
            ax_price.vlines(xs[i], l, h, linewidth=1, color='black')
            # Rektörtgen gövdeyi temsil eder
            color = '#26a69a' if c >= o else '#ef5350'
            rect = Rectangle((xs[i] - half_width, min(o, c)), width, abs(c - o), edgecolor='black', facecolor=color)
            ax_price.add_patch(rect)

        # İndikatörleri Fiyat Paneline Ekle
        if 'bb_low' in ind:
            ax_price.plot(xs, ind['bb_low'].values, label='BB Alt', color='blue', alpha=0.6)
            ax_price.plot(xs, ind['bb_mid'].values, label='BB Orta', color='orange', alpha=0.6)
            ax_price.plot(xs, ind['bb_up'].values, label='BB Üst', color='blue', alpha=0.6)
        
        if 'vwap' in ind:
            ax_price.plot(xs, ind['vwap'].values, label='VWAP', color='purple', linestyle='--', alpha=0.8)
        
        ax_price.plot(xs, closes, linewidth=1.0, label='Kapanış Fiyatı', color='black', alpha=0.8)

        # Trend çizgisi ve etiketler
        breakout_coords = ind.get('breakout_coords')
        if breakout_coords:
            idx1, y1, idx2, y2 = breakout_coords
            ax_price.plot([idx1, idx2], [y1, y2], linestyle='--', linewidth=2, color='red', label='Düşen Trend', alpha=0.9)
            ax_price.text(idx1, y1, f"{y1:.2f}", fontsize=9, color="red")
            ax_price.text(idx2, y2, f"{y2:.2f}", fontsize=9, color="red")

        # Son fiyat etiketi
        ax_price.text(xs[-1], closes[-1], f"{closes[-1]:.2f}", fontsize=10, color="green", weight='bold')
        ax_price.axvline(x=xs[-1], color='green', linestyle=':', linewidth=1.8, alpha=0.8)

        ax_price.legend(loc='upper left')
        ax_price.grid(True, alpha=0.3)
        ax_price.set_title(f"{title} - Mum Grafiği", fontsize=16)
        ax_price.set_ylabel("Fiyat")

        # RSI paneli
        if 'rsi' in ind:
            rsi_vals = ind['rsi'].values
            ax_rsi.plot(xs, rsi_vals, linewidth=1.5, color='orange')
            ax_rsi.axhline(y=70, linestyle='--', color='red', alpha=0.6, label='Aşırı Alım')
            ax_rsi.axhline(y=30, linestyle='--', color='green', alpha=0.6, label='Aşırı Satım')
            ax_rsi.set_ylim(0, 100)
            ax_rsi.grid(True, alpha=0.3)
            ax_rsi.set_title("RSI", fontsize=12)
            ax_rsi.set_ylabel("RSI")

        plt.tight_layout()

        # Grafiği hafızaya kaydet
        import io
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)

        # Telegram'a fotoğraf olarak gönder
        url = f"https://api.telegram.org/bot{token}/sendPhoto"
        async with aiohttp.ClientSession() as session:
            form = aiohttp.FormData()
            form.add_field('chat_id', chat_id)
            form.add_field('photo', buf, filename='chart.png', content_type='image/png')
            await session.post(url, data=form)

    except Exception as e:
        logger.error(f"Grafik çizme veya gönderme hatası: {e}")
# --- Ana analiz ve sinyal üretim fonksiyonu ---
def fetch_and_analyze_data(symbol: str, timeframe: str) -> Tuple[Optional["SignalInfo"], Optional[pd.DataFrame], Optional[Dict[str, Any]]]:
    """
    Belirtilen sembol ve zaman dilimi için veri çeker, teknik analiz yapar ve
    sinyal, veri çerçevesi ve indikatörleri döndürür.
    """
    try:
        yf_symbol = f"{symbol}.IS"
        df = None

        # Zaman dilimine göre veri çekme ve işleme
        if timeframe == '1d':
            df = fetch_history(yf_symbol, '1d', '730d')
        elif timeframe == '4h':
            raw = fetch_history(yf_symbol, '1h', '180d')
            if raw is None or raw.empty: return None, None, None
            df = _resample_ohlcv(raw, '4h')
        elif timeframe == '1h':
            df = fetch_history(yf_symbol, '1h', '60d')
        elif timeframe == '15m':
            df = fetch_history(yf_symbol, '15m', '30d')
        else:
            return None, None, None

        if df is None or df.empty or len(df) < 50:
            logger.warning(f"{symbol} için yetersiz veri ({len(df)})")
            return None, None, None

        # Veriyi standartlaştırma ve temel kontroller
        df = _standardize_df(df)
        last_close = float(df['close'].iloc[-1])
        last_volume = float(df['volume'].iloc[-1])

        if last_close < MIN_PRICE or last_close * last_volume < MIN_VOLUME_TRY:
            return None, None, None

        # Tüm teknik indikatörleri hesapla
        ind = TeknikAnaliz.analyze_df(df)

        # Sinyal parametrelerini topla
        rsi_val = float(ind['rsi'].iloc[-1])
        stoch_k = float(ind['stoch_k'].iloc[-1])
        stoch_d = float(ind['stoch_d'].iloc[-1])
        candle_form = detect_candle_formation(df)
        ma_cross = detect_ma_cross(df['close'], MA_SHORT, MA_LONG)
        tsi_val = float(calculate_tsi(df['close']).iloc[-1])
        sar_val = calculate_sar(df['high'], df['low'])
        sar_status = 'BULLISH' if last_close > sar_val.iloc[-1] else 'BEARISH'
        bb_pos = classify_bb_position(last_close, float(ind['bb_up'].iloc[-1]), float(ind['bb_low'].iloc[-1]), BB_NEAR_PCT)
        volume_ratio = float(last_volume / max(df['volume'].rolling(20).mean().iloc[-1], 1))
        
        # Sinyal yönünü belirleme
        direction = None
        if rsi_val < 35 or stoch_k < 20 or bb_pos in ["LOWER", "NEAR_LOWER"]:
            direction = "BULLISH"
        elif rsi_val > 65 or stoch_k > 80 or bb_pos in ["UPPER", "NEAR_UPPER"]:
            direction = "BEARISH"

        if direction is None:
            return None, df, ind

        # Sinyal nesnesini oluştur
        signal = SignalInfo(
            symbol=symbol,
            timeframe=timeframe,
            direction=direction,
            price=last_close,
            rsi=rsi_val,
            rsi_ema=float(ema(ind['rsi'], RSI_EMA_LEN).iloc[-1]),
            volume_ratio=volume_ratio,
            volume_try=last_close * last_volume,
            macd_signal="BULLISH" if calculate_macd(df['close'], MACD_FAST, MACD_SLOW, MACD_SIGNAL)[0].iloc[-1] > calculate_macd(df['close'], MACD_FAST, MACD_SLOW, MACD_SIGNAL)[1].iloc[-1] else "BEARISH",
            bb_position=bb_pos,
            strength_score=0.0,
            timestamp=str(dt.datetime.now(IST_TZ)),
            breakout_angle=ind.get('breakout_score', 0.0),
            candle_formation=candle_form,
            stochrsi_k=stoch_k,
            stochrsi_d=stoch_d,
            ma_cross=ma_cross,
            tsi_value=tsi_val,
            sar_status=sar_status,
            breakout_coords=ind.get('breakout_coords')
        )

        # Sinyal gücü puanlamasını hesapla
        signal.strength_score = calculate_signal_strength(signal)
        
        if signal.strength_score < MIN_SIGNAL_SCORE:
            logger.info(f"{signal.symbol} için sinyal gücü düşük ({signal.strength_score:.2f})")
            return None, df, ind

        return signal, df, ind
    
    except Exception as e:
        logger.error(f"{symbol} analiz hatası: {e}")
        return None, None, None

# --- Tarama ve raporlama döngüsü ---
async def send_telegram(text: str):
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{TELEGRAM_API_URL}/sendMessage", json=payload, timeout=15) as r:
                if r.status != 200:
                    logger.warning(f"Telegram failed: {r.status} - {await r.text()}")
    except Exception as e:
        logger.warning(f"Telegram error: {e}")

# --- SINYAL MESAJI OLUŞTUR ---
# --- Mesaj ve grafik oluşturma fonksiyonu ---
def generate_signal_message(sig: SignalInfo, df: pd.DataFrame, ind: Dict[str, Any]) -> Tuple[str, BytesIO]:
   
    # Mesaj metni
    message = ""
    is_trend_reversal = False
    if sig.breakout_angle and sig.breakout_angle > 0:
        is_trend_reversal = True
    if sig.candle_formation in ["Morning Star", "Bullish Engulfing", "Piercing Pattern", "Tweezer Bottoms"]:
        is_trend_reversal = True
    if is_trend_reversal:
        message += f"<b>🚨 TREND DÖNÜŞ SİNYALİ - {sig.timeframe}</b>\n"
    else:
        message += f"<b>🟢 AL SİNYALİ - {sig.timeframe}</b>\n"

    message += f"\n<b>{sig.symbol}.IS</b>\n"
    message += f"<b>Güç:</b> {sig.strength_score:.1f}/10\n"
    message += f"<b>Fiyat:</b> {sig.price:.2f} TL\n---"
    message += f"\n\n<b>Detaylı Analiz:</b>"
    message += f"\n• <b>RSI:</b> {sig.rsi:.2f}"
    message += f"\n• <b>MACD Sinyali:</b> {sig.macd_signal}"
    message += f"\n• <b>Hacim Oranı:</b> {sig.volume_ratio:.2f}x"
    if sig.tsi_value is not None:
        message += f"\n• <b>TSI:</b> {sig.tsi_value:.2f}"
    message += f"\n• <b>SAR:</b> {sig.sar_status}"
    if sig.candle_formation:
        message += f"\n• <b>Mum Formasyonu:</b> {sig.candle_formation}"

    # Grafik oluştur
    matplotlib.use("Agg")
    fig, (ax_price, ax_rsi) = plt.subplots(2, 1, figsize=(12, 8), gridspec_kw={'height_ratios': [3, 1]})
    xs = np.arange(len(df))
    closes = df['close'].astype(float).values
    ax_price.plot(xs, closes, color='black', label='Kapanış')
    ax_price.set_title(f"{sig.symbol} Fiyat Grafiği")
    ax_price.grid(True)

    if 'rsi' in ind:
        rsi_vals = ind['rsi'].values
        ax_rsi.plot(xs, rsi_vals, color='orange')
        ax_rsi.axhline(70, color='red', linestyle='--')
        ax_rsi.axhline(30, color='green', linestyle='--')
        ax_rsi.set_ylim(0, 100)
        ax_rsi.set_title("RSI")

    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)

    return message, buf

async def send_signal_with_chart(sig: SignalInfo, df: pd.DataFrame, ind: Dict[str, Any]):
    # Text message
    message = ""
    is_trend_reversal = False
    if sig.breakout_angle is not None and sig.breakout_angle > 0:
        is_trend_reversal = True
    if sig.candle_formation in ["Morning Star", "Bullish Engulfing", "Piercing Pattern", "Tweezer Bottoms"]:
        is_trend_reversal = True
    if is_trend_reversal:
        message += f"<b>🚨 TREND DÖNÜŞ SİNYALİ - {sig.timeframe}</b>\n"
    else:
        message += f"<b>🟢 AL SİNYALİ - {sig.timeframe}</b>\n"
    message += f"\n<b>{sig.symbol}.IS</b>\n"
    message += f"<b>Güç:</b> {sig.strength_score:.1f}/10\n"
    message += f"<b>Fiyat:</b> {sig.price:.2f} TL\n"
    message += "---"
    # Zaman dilimi özeti
    message += f"\n\n<b>Detaylı Analiz:</b>"
    message += f"\n• <b>RSI:</b> {sig.rsi:.2f}"
    message += f"\n• <b>MACD Sinyali:</b> {sig.macd_signal}"
    message += f"\n• <b>Hacim Oranı:</b> {sig.volume_ratio:.2f}x"
    if sig.tsi_value is not None:
        message += f"\n• <b>TSI:</b> {sig.tsi_value:.2f}"
    message += f"\n• <b>SAR:</b> {sig.sar_status}"
    if sig.candle_formation:
        message += f"\n• <b>Mum Formasyonu:</b> {sig.candle_formation}"
    await send_telegram(message)
    
async def send_signal_with_chart(sig: SignalInfo, df: pd.DataFrame, ind: Dict[str, Any]):
    # Mesaj ve grafik oluştur
    message, chart_buf = generate_signal_message(sig, df, ind)
    
    # Telegram'a mesaj gönder
    await send_telegram(message)
    
    # Grafiği Telegram'a gönder
    await send_chart_to_telegram(
        TELEGRAM_BOT_TOKEN,
        TELEGRAM_CHAT_ID,
        f"{sig.symbol}.IS - {sig.timeframe} - {sig.direction} - Güç {sig.strength_score:.1f}/10",
        df.tail(200),
        ind
    )


async def scan_and_report():
    global LAST_SCAN_TIME, DAILY_SIGNALS
    logger.info("⏳ BIST taraması başlıyor...")
    LAST_SCAN_TIME = dt.datetime.now(IST_TZ)
    if is_market_hours():
        found_signals = []
        with ThreadPoolExecutor(max_workers=max(4, (os.cpu_count() or 2)//1)) as executor:
            loop = asyncio.get_running_loop()
            tasks = []
            for symbol in TICKERS:
                tasks.append(loop.run_in_executor(executor, fetch_history, f"{symbol}.IS", "1d", "1y"))
            daily_dfs = await asyncio.gather(*tasks, return_exceptions=True)
            for i, symbol in enumerate(TICKERS):
                try:
                    daily_df = daily_dfs[i]
                    if isinstance(daily_df, Exception) or daily_df is None or daily_df.empty:
                        continue
                    multi_tf_results = {}
                    for tf in TIMEFRAMES:
                        result = await loop.run_in_executor(executor, fetch_and_analyze_data, symbol, tf)
                        if result and result[0] is not None:
                            multi_tf_results[tf] = result

                    if multi_tf_results:
                        highest_signal_tuple = max(multi_tf_results.values(), key=lambda s: s[0].strength_score)
                        
                        highest_signal = highest_signal_tuple[0]
                        
                        # Grafik için veri ve indikatörleri al
                        tf_df = highest_signal_tuple[1]
                        tf_ind = highest_signal_tuple[2]

                        # Sadece son 200 veri noktasını kullanmak için DataFrame'i kes
                        plot_df = tf_df.tail(200).copy()
                        
                        # İndikatörleri de DataFrame ile aynı boyuta getir
                        plot_ind = {}
                        for key, value in tf_ind.items():
                            if isinstance(value, pd.Series):
                                plot_ind[key] = value.iloc[-200:]
                            else:
                                plot_ind[key] = value

                        if highest_signal.direction == "BULLISH" and highest_signal.strength_score >= MIN_SIGNAL_SCORE:
                            today_date_str = dt.datetime.now(IST_TZ).strftime('%Y-%m-%d')
                            if symbol in DAILY_SIGNALS and DAILY_SIGNALS[symbol].get('direction') == "BULLISH":
                                logger.info(f"🚫 {symbol}.IS için bugün zaten AL sinyali gönderildi, tekrar gönderilmiyor.")
                                continue
                            
                            found_signals.append(highest_signal)
                            DAILY_SIGNALS[symbol] = asdict(highest_signal)
                            
                            # send text + chart
                            await send_signal_with_chart(highest_signal, plot_df, plot_ind)
                            logger.info(f"🎉 Sinyal Bulundu: {symbol} - {highest_signal.timeframe} - Güç: {highest_signal.strength_score:.1f}")
                except Exception as e:
                    logger.warning(f"{symbol} analiz hatası: {e}")
                    continue
        logger.info(f"✅ BIST taraması tamamlandı. {len(found_signals)} sinyal bulundu.")
    else:
        logger.info("❌ Borsa kapalı, tarama yapılmadı.")
        now_ist = dt.datetime.now(IST_TZ)
        if now_ist.time() >= dt.time(DAILY_REPORT_HOUR, DAILY_REPORT_MINUTE) and \
           (now_ist.time() - dt.timedelta(minutes=CHECK_EVERY_MIN)) < dt.time(DAILY_REPORT_HOUR, DAILY_REPORT_MINUTE):
            await send_daily_report()
            DAILY_SIGNALS.clear()

async def run_scanner_periodically():
    while True:
        try:
            now_ist = dt.datetime.now(IST_TZ)
            target_minute = (now_ist.minute // CHECK_EVERY_MIN) * CHECK_EVERY_MIN
            target_time = now_ist.replace(minute=target_minute, second=0, microsecond=0) + dt.timedelta(minutes=CHECK_EVERY_MIN)
            if is_market_hours():
                await scan_and_report()
            sleep_duration = (target_time - dt.datetime.now(IST_TZ)).total_seconds()
            if sleep_duration < 0:
                sleep_duration = CHECK_EVERY_MIN * 60 + sleep_duration
            logger.info(f"💤 Sonraki tarama için {int(sleep_duration)} saniye bekleniyor...")
            await asyncio.sleep(max(5, sleep_duration))
        except Exception as e:
            logger.error(f"Main loop error: {e}")
            await asyncio.sleep(60)

# Health handler & daily report
class HealthHandler(web.View):
    async def get(self):
        market_status = "AÇIK" if is_market_hours() else "KAPALI"
        response = {
            "status": "healthy",
            "service": "bist-scanner",
            "market_status": market_status,
            "total_stocks_to_scan": len(TICKERS),
            "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
            "ist_time": dt.datetime.now(IST_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            "uptime_seconds": time.time() - START_TIME,
            "last_scan": LAST_SCAN_TIME.isoformat() if LAST_SCAN_TIME else None,
            "daily_signals_count": len(DAILY_SIGNALS)
        }
        return web.json_response(response)

async def send_daily_report():
    if not DAILY_SIGNALS:
        report_msg = "📉 Bugün sinyal oluşturacak koşul oluşmadı."
        await send_telegram(f"<b>📊 GÜNLÜK PERFORMANS RAPORU</b>\n\n{report_msg}")
        return
    report_msg = f"<b>📊 GÜNLÜK PERFORMANS RAPORU</b>\n"
    report_msg += f"🗓️ <b>Tarih:</b> {dt.datetime.now(IST_TZ).strftime('%Y-%m-%d')}\n"
    report_msg += f"📈 <b>Toplam Sinyal Sayısı:</b> {len(DAILY_SIGNALS)}\n"
    report_msg += "---"
    sorted_signals = sorted(DAILY_SIGNALS.values(), key=lambda s: s.get('strength_score', 0), reverse=True)
    for signal in sorted_signals:
        emoji = "🟢" if signal.get('direction', 'BULLISH') == "BULLISH" else "🔴"
        report_msg += f"\n\n{emoji} <b>{signal['symbol']}.IS</b>\n"
        report_msg += f"• Güç: {signal.get('strength_score', 0):.1f}/10\n"
        report_msg += f"• Zaman: {dt.datetime.fromisoformat(signal['timestamp']).strftime('%H:%M') if signal.get('timestamp') else '-'}"
    await send_telegram(report_msg)
    logger.info(f"📊 Günlük rapor gönderildi: {len(DAILY_SIGNALS)} sinyal")

async def start_server():
    app = web.Application()
    app.router.add_get('/health', HealthHandler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', HEALTH_CHECK_PORT)
    await site.start()
    logger.info(f"✅ Sağlık kontrol sunucusu {HEALTH_CHECK_PORT} portunda başlatıldı.")

async def main():
    await asyncio.gather(start_server(), run_scanner_periodically())

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot kapatılıyor.")
