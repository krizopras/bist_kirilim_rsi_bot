#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tüm BIST Hisselerini Paralel Tarayan Çoklu Zaman Dilimi ve Mum Formasyonu Analiz Botu
- Asenkron yapı ile hızlı tarama.
- yfinance üzerinden veri çeker.
- Çoklu zaman diliminde (15m, 1h, 4h, 1d) sinyal uyumunu inceler.
- 4H verisi Yahoo'da yerel desteklenmediği için 1H veriden 4H'ye yeniden örnekleme (resample) yapılır.
- RSI trendline kırılımı, hacim, Bollinger, MACD, genişletilmiş mum formasyonları,
  Stokastik RSI ve Hareketli Ortalama Kesişimleri puanlamaya dahil edilir.
- Çoklu zaman dilimi puanı her zaman dilimi için ayrı skor hesaplanarak ağırlıklı ortalamaya katılır.
- Bollinger "NEAR_UPPER/NEAR_LOWER" mantığı eklendi.
- Minimum fiyat ve TL hacmi filtreleri etkinleştirildi.
- Sinyal tekrarını disk tabanlı cooldown ile engeller, günlük rapor Istanbul saatine göre 18:00'de gönderilir.
"""

import os
import asyncio
import aiohttp
from aiohttp import web
import datetime as dt
import logging
import json
import time
from datetime import timezone, timedelta
from typing import List, Tuple, Dict, Optional, Set
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import pandas as pd
import math
import yfinance as yf

# --- Ayarlar ve Güvenlik ---
LOG_FILE = os.getenv('LOG_FILE', 'trading_bot.log')
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
HEALTH_CHECK_PORT = int(os.getenv("PORT", 8080))

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("TELEGRAM_BOT_TOKEN ve TELEGRAM_CHAT_ID environment variable'ları gerekli!")

TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
logger = logging.getLogger("bist_scanner")
logger.setLevel(logging.INFO)

IST_TZ = dt.timezone(dt.timedelta(hours=3))

# ----------------------- BIST Hisse Listeleri (kısaltıldı/temizlendi) -----------------------
BIST_30_STOCKS = [
    "THYAO","SAHOL","ASTOR","AKBNK","SISE","BIMAS","EREGL","KCHOL","PETKM","VAKBN",
    "TCELL","TUPRS","TTKOM","PGSUS","DOHOL","KOZAA","MGROS","ARCLK","VESTL","KRDMD",
    "FROTO","HALKB","ISCTR","GARAN","AKSA","ALARK","AYGAZ","CCOLA","EKGYO","GUBRF"
]

ALL_BIST_STOCKS = list(set(BIST_30_STOCKS))

# ----------------------- Config -----------------------
def getenv_list(key: str, default: str) -> list:
    return [s.strip() for s in os.getenv(key, default).split(",") if s.strip()]

SCAN_MODE = os.getenv("SCAN_MODE", "BIST_30")
CUSTOM_TICKERS = getenv_list("TICKERS", "")
TIMEFRAMES = getenv_list("TIMEFRAMES", "1d,4h,1h,15m")
CHECK_EVERY_MIN = int(os.getenv("CHECK_EVERY_MIN", "15"))
MIN_PRICE = float(os.getenv("MIN_PRICE", "1.0"))
MIN_VOLUME_TRY = float(os.getenv("MIN_VOLUME_TRY", "1000000"))
RSI_LEN = int(os.getenv("RSI_LEN", "22"))
RSI_EMA_LEN = int(os.getenv("RSI_EMA", "66"))
PIVOT_PERIOD = int(os.getenv("PIVOT_PERIOD", "10"))
ALERT_COOLDOWN_MIN = int(os.getenv("ALERT_COOLDOWN_MIN", "60"))
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "1.5"))
MACD_FAST = int(os.getenv("MACD_FAST", "12"))
MACD_SLOW = int(os.getenv("MACD_SLOW", "26"))
MACD_SIGNAL = int(os.getenv("MACD_SIGNAL", "9"))
BB_PERIOD = int(os.getenv("BB_PERIOD", "20"))
BB_MULT = float(os.getenv("BB_MULT", "2.0"))
BB_NEAR_PCT = float(os.getenv("BB_NEAR_PCT", "0.03"))
# Yeni indikatör ayarları
STOCHRSI_PERIOD = int(os.getenv("STOCHRSI_PERIOD", "14"))
STOCHRSI_K = int(os.getenv("STOCHRSI_K", "3"))
STOCHRSI_D = int(os.getenv("STOCHRSI_D", "3"))
MA_SHORT = int(os.getenv("MA_SHORT", "50"))
MA_LONG = int(os.getenv("MA_LONG", "200"))


if SCAN_MODE == "BIST_30":
    TICKERS = BIST_30_STOCKS
elif SCAN_MODE == "ALL":
    TICKERS = ALL_BIST_STOCKS
elif SCAN_MODE == "CUSTOM" and CUSTOM_TICKERS:
    TICKERS = CUSTOM_TICKERS
else:
    TICKERS = BIST_30_STOCKS

TICKERS = sorted(list(set(TICKERS)))

# Global variables
LAST_SCAN_TIME: Optional[dt.datetime] = None
START_TIME = time.time()

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

# ----------------------- Sağlık Kontrolü -----------------------
class HealthHandler(web.View):
    async def get(self):
        response = {
            "status": "healthy",
            "service": "bist-scanner",
            "mode": SCAN_MODE,
            "total_stocks_to_scan": len(TICKERS),
            "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
            "uptime_seconds": time.time() - START_TIME,
            "last_scan": LAST_SCAN_TIME.isoformat() if LAST_SCAN_TIME else None
        }
        return web.json_response(response)

async def start_health_server(loop, stop_event):
    app = web.Application()
    app.router.add_get('/health', HealthHandler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', HEALTH_CHECK_PORT)
    await site.start()
    logger.info(f"✅ Sağlık kontrolü http://0.0.0.0:{HEALTH_CHECK_PORT} üzerinde.")
    await stop_event.wait()
    await runner.cleanup()

# ----------------------- Utility Functions -----------------------
async def send_telegram(text: str):
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(TELEGRAM_API_URL, json=payload, timeout=15) as r:
                if r.status != 200:
                    logger.warning(f"Telegram failed: {r.status} - {await r.text()}")
    except Exception as e:
        logger.warning(f"Telegram error: {e}")

# TA helpers

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

# **YENİ EKLEME** - Stokastik RSI hesaplaması
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

# **YENİ EKLEME** - Hareketli ortalama kesişim tespiti
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

def is_doji(open_p, close_p, high_p, low_p, body_size_threshold=0.1):
    body = abs(close_p - open_p)
    full_range = high_p - low_p
    return full_range > 0 and body < full_range * body_size_threshold

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

def detect_candle_formation(df: pd.DataFrame) -> Optional[str]:
    if len(df) < 3:
        return None
    last_candle = df.iloc[-1]
    prev_candle = df.iloc[-2]
    open_p, high_p, low_p, close_p = last_candle['open'], last_candle['high'], last_candle['low'], last_candle['close']
    if is_three_white_soldiers(df): return "Three White Soldiers"
    if is_three_black_crows(df): return "Three Black Crows"
    if is_bullish_engulfing(prev_candle, last_candle): return "Bullish Engulfing"
    if is_bearish_engulfing(prev_candle, last_candle): return "Bearish Engulfing"
    if is_piercing_pattern(df): return "Piercing Pattern"
    if is_dark_cloud_cover(df): return "Dark Cloud Cover"
    if is_hammer(open_p, close_p, low_p, high_p): return "Hammer"
    if is_inverted_hammer(open_p, close_p, low_p, high_p): return "Inverted Hammer"
    if is_doji(open_p, close_p, high_p, low_p): return "Doji"
    return None

# ----------------------- Skorlama -----------------------

def calculate_signal_strength(signal: SignalInfo) -> float:
    score = 5.0
    # RSI konumu
    if signal.direction == "BULLISH":
        if signal.rsi < 30: score += 3.0
        elif signal.rsi < 50: score += 1.0
    else:
        if signal.rsi > 70: score += 3.0
        elif signal.rsi > 50: score += 1.0
    # Hacim oranı
    if signal.volume_ratio > 3.0: score += 2.5
    elif signal.volume_ratio > 2.0: score += 2.0
    elif signal.volume_ratio > 1.5: score += 1.0
    # MACD uyumu
    if (signal.direction == "BULLISH" and signal.macd_signal == "BULLISH") or \
       (signal.direction == "BEARISH" and signal.macd_signal == "BEARISH"):
        score += 1.5
    # RSI-EMA trend yönü
    if (signal.direction == "BULLISH" and signal.rsi > signal.rsi_ema) or \
       (signal.direction == "BEARISH" and signal.rsi < signal.rsi_ema):
        score += 1.0
    # Bollinger yakınlık/konum
    if signal.bb_position in ("NEAR_LOWER", "LOWER") and signal.direction == "BULLISH":
        score += 0.5
    if signal.bb_position in ("NEAR_UPPER", "UPPER") and signal.direction == "BEARISH":
        score += 0.5
    # Fiyat filtresi
    if signal.price > 10: score += 0.5
    # RSI trendline kırılım açısı
    if signal.breakout_angle is not None:
        angle_abs = abs(signal.breakout_angle)
        if angle_abs > 30: score += 3.0
        elif angle_abs > 15: score += 2.0
        elif angle_abs > 5: score += 1.0
    # Mum formasyonları
    if signal.candle_formation:
        if signal.candle_formation in [
            "Hammer", "Bullish Engulfing", "Piercing Pattern", "Three White Soldiers"
        ]:
            score += 2.5
        elif signal.candle_formation in [
            "Inverted Hammer", "Bearish Engulfing", "Dark Cloud Cover", "Three Black Crows"
        ]:
            score += 2.5
        elif signal.candle_formation == "Doji":
            score += 1.0
    # Çoklu zaman dilimi katkısı
    if signal.multi_tf_score:
        score += signal.multi_tf_score
    # **YENİ EKLEME** - Stokastik RSI puanlaması
    if signal.stochrsi_k is not None and signal.stochrsi_d is not None:
        if signal.direction == "BULLISH" and signal.stochrsi_k > signal.stochrsi_d and signal.stochrsi_k < 20:
            score += 2.0
        elif signal.direction == "BEARISH" and signal.stochrsi_k < signal.stochrsi_d and signal.stochrsi_k > 80:
            score += 2.0
    # **YENİ EKLEME** - Hareketli Ortalama kesişim puanlaması
    if signal.ma_cross:
        if signal.direction == "BULLISH" and signal.ma_cross == "GOLDEN_CROSS":
            score += 3.0
        elif signal.direction == "BEARISH" and signal.ma_cross == "DEATH_CROSS":
            score += 3.0
    return float(max(0.0, min(10.0, score)))

# ----------------------- Veri Çekme & Analiz -----------------------

def _resample_ohlcv(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """OHLCV yeniden örnekleme."""
    if df is None or df.empty:
        return df
    df = df.copy()
    cols = {"Open":"open","High":"high","Low":"low","Close":"close","Volume":"volume"}
    df.rename(columns=cols, inplace=True)
    agg = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }
    out = df.resample(rule).agg(agg).dropna()
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

def fetch_and_analyze_data(symbol: str, timeframe: str) -> Optional[SignalInfo]:
    try:
        yf_symbol = f"{symbol}.IS"
        if timeframe == '1d':
            base_interval, period = '1d', '730d'
            df = fetch_history(yf_symbol, base_interval, period)
            df = _standardize_df(df)
        elif timeframe == '1h':
            base_interval, period = '1h', '180d'
            df = fetch_history(yf_symbol, base_interval, period)
            df = _standardize_df(df)
        elif timeframe == '4h':
            base_interval, period = '1h', '180d'
            raw = fetch_history(yf_symbol, base_interval, period)
            if raw is None or raw.empty: return None
            raw = _standardize_df(raw)
            df = _resample_ohlcv(raw, '4H')
        elif timeframe == '15m':
            base_interval, period = '15m', '60d'
            df = fetch_history(yf_symbol, base_interval, period)
            df = _standardize_df(df)
        else:
            return None

        if df is None or df.empty or len(df) < max(200, MA_LONG, RSI_LEN + STOCHRSI_PERIOD):
            return None

        df = df.dropna().tail(max(300, MA_LONG + 10))

        last_close = float(df['close'].iloc[-1])
        last_vol = float(df['volume'].iloc[-1])
        if last_close < MIN_PRICE or last_close * last_vol < MIN_VOLUME_TRY:
            return None

        close, volume = df["close"], df["volume"]
        rsi_series = rsi_from_close(close, RSI_LEN)
        rsi_ema = ema(rsi_series, RSI_EMA_LEN)
        macd_line, macd_sig, macd_hist = calculate_macd(close, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        bb_upper, bb_middle, bb_lower = calculate_bollinger_bands(close, BB_PERIOD, BB_MULT)

        volume_sma = sma(volume, 20)
        current_volume = volume.iloc[-1]
        avg_volume = volume_sma.iloc[-1]
        volume_ratio = float(current_volume / avg_volume) if (avg_volume and avg_volume > 0) else 1.0

        highs_idx, lows_idx = find_pivots(rsi_series, PIVOT_PERIOD, PIVOT_PERIOD)
        now_i = len(rsi_series) - 1
        bull_break, bear_break, breakout_angle = detect_breakouts(rsi_series, highs_idx, lows_idx, now_i)

        current_rsi, current_rsi_ema = float(rsi_series.iloc[now_i]), float(rsi_ema.iloc[now_i])
        current_price = float(close.iloc[-1])

        macd_signal_str = "NEUTRAL"
        if len(macd_hist) > 1:
            if macd_hist.iloc[-1] > 0 and macd_hist.iloc[-2] <= 0: macd_signal_str = "BULLISH"
            elif macd_hist.iloc[-1] < 0 and macd_hist.iloc[-2] >= 0: macd_signal_str = "BEARISH"

        is_bull_signal = bull_break or (macd_signal_str == "BULLISH" and current_rsi > 50)
        is_bear_signal = bear_break or (macd_signal_str == "BEARISH" and current_rsi < 50)
        if not (is_bull_signal or is_bear_signal): return None
        if volume_ratio < MIN_VOLUME_RATIO: return None

        bb_pos = classify_bb_position(
            current_price,
            float(bb_upper.iloc[-1]),
            float(bb_lower.iloc[-1]),
            BB_NEAR_PCT
        )
        
        direction = "BULLISH" if is_bull_signal else "BEARISH"
        volume_try = current_price * current_volume
        candle_form = detect_candle_formation(df)
        stochrsi_k, stochrsi_d = calculate_stoch_rsi(rsi_series, STOCHRSI_PERIOD, STOCHRSI_K, STOCHRSI_D)
        ma_cross_type = detect_ma_cross(close, MA_SHORT, MA_LONG)

        base_signal = SignalInfo(
            symbol=symbol, timeframe=timeframe, direction=direction, price=current_price,
            rsi=current_rsi, rsi_ema=current_rsi_ema, volume_ratio=volume_ratio,
            volume_try=volume_try, macd_signal=macd_signal_str, bb_position=bb_pos,
            strength_score=0.0, timestamp=df.index[-1].strftime('%Y-%m-%d %H:%M'),
            breakout_angle=breakout_angle, candle_formation=candle_form,
            multi_tf_score=0.0, stochrsi_k=stochrsi_k, stochrsi_d=stochrsi_d, ma_cross=ma_cross_type
        )
        base_signal.strength_score = calculate_signal_strength(base_signal)
        return base_signal
    except Exception as e:
        logger.error(f"Analysis error {symbol} {timeframe}: {e}")
        return None

# ----------------------- Sinyal DB -----------------------

def get_signal_db():
    db_file = "signals_db.json"
    if not os.path.exists(db_file) or os.stat(db_file).st_size == 0:
        return {}
    with open(db_file, "r") as f:
        return json.load(f)

def save_signal_db(data):
    with open("signals_db.json", "w") as f:
        json.dump(data, f, indent=2)

def save_signal_to_db(signal: SignalInfo):
    data = get_signal_db()
    today_str = dt.datetime.now(IST_TZ).strftime("%Y-%m-%d")
    if today_str not in data:
        data[today_str] = {}
    data[today_str][signal.symbol] = {
        "timeframe": signal.timeframe,
        "direction": signal.direction,
        "price": signal.price,
        "strength_score": signal.strength_score,
        "timestamp": signal.timestamp
    }
    save_signal_db(data)

# ----------------------- Uyarı Gönder -----------------------

def save_last_alert(symbol: str, direction: str):
    try:
        data = {}
        if os.path.exists("last_alerts.json") and os.stat("last_alerts.json").st_size > 0:
            with open("last_alerts.json", "r") as f:
                data = json.load(f)
        data[f"{symbol}_{direction}"] = dt.datetime.now(dt.timezone.utc).isoformat()
        with open("last_alerts.json", "w") as f:
            json.dump(data, f)
    except Exception as e:
        logger.error(f"Sinyal kaydetme hatası: {e}")

def get_last_alert(symbol: str, direction: str) -> Optional[dt.datetime]:
    try:
        if os.path.exists("last_alerts.json") and os.stat("last_alerts.json").st_size > 0:
            with open("last_alerts.json", "r") as f:
                data = json.load(f)
            ts = data.get(f"{symbol}_{direction}")
            if ts:
                return dt.datetime.fromisoformat(ts)
    except Exception as e:
        logger.error(f"Son sinyal okuma hatası: {e}")
    return None

async def send_enhanced_alert(signal: SignalInfo):
    side_key = "AL" if signal.direction == "BULLISH" else "SAT"
    last_alert_time = get_last_alert(signal.symbol, side_key)
    if last_alert_time and (dt.datetime.now(dt.timezone.utc) - last_alert_time).total_seconds() < ALERT_COOLDOWN_MIN * 60:
        logger.info(f"⏳ {signal.symbol} için {side_key} sinyali cooldown'da.")
        return

    save_signal_to_db(signal)

    emoji = "🟢📈" if signal.direction == "BULLISH" else "🔴📉"
    direction_text = "AL" if signal.direction == "BULLISH" else "SAT"
    strength_stars = "⭐" * min(5, int(signal.strength_score / 2))

    if signal.volume_ratio > 5.0: volume_emoji = "🚀"
    elif signal.volume_ratio > 3.0: volume_emoji = "🔥"
    else: volume_emoji = "📈"

    if signal.volume_try >= 1_000_000: volume_str = f"{signal.volume_try/1_000_000:.1f}M TL"
    elif signal.volume_try >= 1_000: volume_str = f"{signal.volume_try/1_000:.0f}K TL"
    else: volume_str = f"{signal.volume_try:.0f} TL"

    angle_str = f"({signal.breakout_angle:.1f}°)" if signal.breakout_angle is not None else ""
    candle_form_str = f"\n🔥 <b>Mum Formasyonu:</b> {signal.candle_formation}" if signal.candle_formation else ""
    stochrsi_str = f"\n• StochRSI({STOCHRSI_K}): K:{signal.stochrsi_k:.1f} | D:{signal.stochrsi_d:.1f}" if signal.stochrsi_k is not None else ""
    ma_cross_str = f"\n• MA Kesişimi: <b>{signal.ma_cross.replace('_', ' ').title()}</b>" if signal.ma_cross else ""


    msg = f"""<b>{emoji} SİNYAL TESPİT EDİLDİ!</b>

📊 <b>Hisse:</b> {signal.symbol}.IS
⏰ <b>Zaman:</b> {signal.timeframe}
🎯 <b>Yön:</b> {direction_text}
💰 <b>Fiyat:</b> ₺{signal.price:.2f}
{candle_form_str}

📈 <b>Teknik Durum:</b>
• RSI({RSI_LEN}): {signal.rsi:.1f}
• RSI EMA({RSI_EMA_LEN}): {signal.rsi_ema:.1f}
• MACD: {signal.macd_signal}
• BB Pozisyon: {signal.bb_position}{stochrsi_str}{ma_cross_str}

{volume_emoji} <b>Hacim:</b> {signal.volume_ratio:.1f}x ({volume_str})
⚡ <b>Güç:</b> {signal.strength_score:.1f}/10 {strength_stars} {angle_str}

🕐 <b>Zaman:</b> {signal.timestamp} (bar kapanış)
🔍 <b>Tarama:</b> {SCAN_MODE}

#BIST #{signal.symbol} #{signal.timeframe} #{SCAN_MODE}"""

    await send_telegram(msg)
    save_last_alert(signal.symbol, side_key)

# ----------------------- Tarama Döngüsü -----------------------

async def run_scan_async():
    global LAST_SCAN_TIME
    logger.info("🔍 Tam BIST taraması başladı - %s modu", SCAN_MODE)
    loop = asyncio.get_running_loop()
    signals_by_symbol: Dict[str, List[SignalInfo]] = {}

    with ThreadPoolExecutor(max_workers=min(10, len(TICKERS) * len(TIMEFRAMES))) as executor:
        tasks = [loop.run_in_executor(executor, fetch_and_analyze_data, symbol, tf)
                 for symbol in TICKERS for tf in TIMEFRAMES]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, Exception) or result is None:
            continue
        signals_by_symbol.setdefault(result.symbol, []).append(result)

    weights = {'1d': 4, '4h': 3, '1h': 2, '15m': 1}
    final_signals: List[SignalInfo] = []

    for symbol, sig_list in signals_by_symbol.items():
        if not sig_list: continue
        wsum = sum(weights.get(s.timeframe, 1) for s in sig_list)
        wscore = sum(s.strength_score * weights.get(s.timeframe, 1) for s in sig_list) / max(wsum, 1)
        main = max(sig_list, key=lambda s: s.strength_score)
        main.multi_tf_score = wscore
        main.strength_score = calculate_signal_strength(main)
        final_signals.append(main)

    if final_signals:
        final_signals.sort(key=lambda s: s.strength_score, reverse=True)
        for s in final_signals:
            await send_enhanced_alert(s)
    else:
        logger.info("🤷 Herhangi bir sinyal bulunamadı.")

    LAST_SCAN_TIME = dt.datetime.now(dt.timezone.utc)
    logger.info("✅ Tarama tamamlandı. Bir sonraki tarama %s dakika sonra.", CHECK_EVERY_MIN)

# ----------------------- Günlük Rapor -----------------------

async def send_daily_report(loop):
    data = get_signal_db()
    today_str = dt.datetime.now(IST_TZ).strftime("%Y-%m-%d")
    if today_str not in data:
        logger.info("Günlük rapor için sinyal verisi bulunamadı.")
        return

    bullish, bearish = [], []
    for symbol, info in data[today_str].items():
        try:
            hist = await loop.run_in_executor(None, lambda: yf.Ticker(f"{symbol}.IS").history(period="1d", interval="1d"))
            if hist is None or hist.empty: continue
            end_price = float(hist['Close'].iloc[-1])
            change_pct = ((end_price - info['price']) / info['price']) * 100.0
            enriched = dict(info)
            enriched['end_price'] = end_price
            enriched['performance_pct'] = change_pct
            if info['direction'] == "BULLISH": bullish.append(enriched)
            else: bearish.append(enriched)
        except Exception as e:
            logger.error(f"Gün sonu raporu veri hatası: {symbol} - {e}")

    total = len(bullish) + len(bearish)
    if total == 0: return

    bull_success = sum(1 for s in bullish if s['performance_pct'] > 0)
    bear_success = sum(1 for s in bearish if s['performance_pct'] < 0)

    bull_rate = (bull_success / len(bullish) * 100) if bullish else 0
    bear_rate = (bear_success / len(bearish) * 100) if bearish else 0
    overall_rate = ((bull_success + bear_success) / total * 100) if total else 0

    bull_avg_gain = (sum(s['performance_pct'] for s in bullish if s['performance_pct'] > 0) / bull_success) if bull_success else 0
    bull_avg_loss = (sum(s['performance_pct'] for s in bullish if s['performance_pct'] < 0) / (len(bullish) - bull_success)) if (len(bullish) - bull_success) else 0

    bear_avg_gain = (sum(s['performance_pct'] for s in bearish if s['performance_pct'] > 0) / (len(bearish) - bear_success)) if (len(bearish) - bear_success) else 0
    bear_avg_loss = (sum(s['performance_pct'] for s in bearish if s['performance_pct'] < 0) / bear_success) if bear_success else 0

    msg = f"""📊📈📉 <b>Günlük Sinyal Raporu</b> ({today_str}, Istanbul)

✅ <b>Genel İstatistikler:</b>
- Toplam Sinyal Sayısı: {total}
- Genel Başarı Oranı: %{overall_rate:.1f}

🟢 <b>Boğa Sinyalleri</b>:
- Sinyal Sayısı: {len(bullish)}
- Başarı Oranı: %{bull_rate:.1f}
- Ortalama Kazanç: +%{bull_avg_gain:.2f}
- Ortalama Kayıp: %{bull_avg_loss:.2f}

🔴 <b>Ayı Sinyalleri</b>:
- Sinyal Sayısı: {len(bearish)}
- Başarı Oranı: %{bear_rate:.1f}
- Ortalama Kazanç: +%{bear_avg_gain:.2f}
- Ortalama Kayıp: %{bear_avg_loss:.2f}
"""
    await send_telegram(msg)

# ----------------------- Main -----------------------

async def main():
    logger.info("🚀 Gelişmiş BIST Sinyal Botu başlatılıyor...")
    stop_event = asyncio.Event()
    health_task = asyncio.create_task(start_health_server(asyncio.get_running_loop(), stop_event))
    await run_scan_async()
    last_report_date = dt.datetime.now(IST_TZ).date()
    try:
        while True:
            now_ist = dt.datetime.now(IST_TZ)
            if now_ist.hour == 18 and now_ist.date() != last_report_date:
                await send_daily_report(asyncio.get_running_loop())
                last_report_date = now_ist.date()
            await asyncio.sleep(CHECK_EVERY_MIN * 60)
            await run_scan_async()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot durduruluyor...")
        stop_event.set()
    finally:
        await health_task

if __name__ == "__main__":
    asyncio.run(main())
