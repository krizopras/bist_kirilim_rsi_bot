#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tüm BIST Hisselerini Paralel Tarayan Çoklu Zaman Dilimi ve Mum Formasyonu Analiz Botu
- Asenkron yapı ile hızlı tarama.
- Investpy'den BIST hisse verisi çeker.
- Çoklu zaman diliminde (15m, 1h, 4h, 1d) sinyal uyumunu inceler.
- RSI trendline kırılımı, hacim ve genişletilmiş mum formasyonlarını puanlamaya dahil eder.
"""

import os
import asyncio
import aiohttp
from aiohttp import web
import datetime
import logging
import signal
import json
import time
from datetime import timezone, timedelta
from typing import List, Tuple, Dict, Optional, Set, Callable
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import pandas as pd
import investpy
import math
import yfinance as yf # yfinance kütüphanesi eklendi

# --- Ayarlar ve Güvenlik ---
LOG_FILE = os.getenv('LOG_FILE', 'trading_bot.log')
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
IS_RENDER = os.getenv("RENDER", "false").lower() == "true"
HEALTH_CHECK_PORT = int(os.getenv("PORT", 8080))
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("TELEGRAM_BOT_TOKEN ve TELEGRAM_CHAT_ID environment variable'ları gerekli!")

TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("bist_scanner")
logger.setLevel(logging.INFO)

# ----------------------- BIST Hisse Listeleri -----------------------
BIST_100_STOCKS = [
    "THYAO", "SAHOL", "ASTOR", "AKBNK", "SISE", "BIMAS", "EREGL", "KCHOL", "PETKM", "VAKBN",
    "TCELL", "TUPRS", "TTKOM", "TOASO", "PGSUS", "ULKER", "DOHOL", "KOZAA", "KOZAL", "MGROS",
    "ARCLK", "VESTL", "KRDMD", "FROTO", "ECILC", "HALKB", "ISCTR", "GARAN", "AKSA", "ALARK",
    "AYGAZ", "BRSAN", "CCOLA", "CIMSA", "DOAS", "EGEEN", "EKGYO", "ENKAI", "ENJSA", "ERBOS",
    "GUBRF", "HEKTS", "IPEKE", "KARSN", "KLMSN", "KONYA", "KORDS", "LOGO", "MAVI", "ODAS",
    "OTKAR", "OYAKC", "PARSN", "PENGD", "PETKM", "PRKME", "QUAGR", "RAYSG", "SARKY", "SELEC",
    "SKBNK", "SOKM", "TATGD", "TAVHL", "TKFEN", "TLMAN", "TMSN", "TRGYO", "TSKB", "TURSG",
    "ULKER", "UMPAS", "VAKKO", "VERUS", "YATAS", "YKBNK", "ZOREN", "AEFES", "AKSEN", "ALCAR",
    "ANACM", "ASUZU", "AVGYO", "BAGFS", "BANVT", "BARMA", "BJKAS", "BRISA", "BRYAT", "BUCIM",
    "CEMTS", "CMENT", "CWENE", "DARDL", "DERHL", "DESA", "DGKLB", "DIRIT", "DURDO", "DYOBY",
    "EGGUB", "EKDGS", "EMKEL", "EMNIS", "ENERY", "ETYAT", "EUHOL", "EUKYO", "EUREN", "EUYO",
    "EYGYO", "FADE", "FERRY", "FFORT", "FIDEO", "FINBN", "FLAP", "FMIZP", "FONET", "FORMT",
    "FORTE", "FRIGO", "FZLGY", "GARAN", "GEDIK", "GEDZA", "GENIL", "GENTS", "GEREL", "GESAN",
    "GIPTA", "GLYHO", "GMTAS", "GOKNR", "GOODY", "GOZDE", "GRSEL", "GSDDE", "GSDHO", "GSRAY",
    "GWIND", "GYODER", "HALKB"
]

BIST_30_STOCKS = [
    "THYAO", "SAHOL", "ASTOR", "AKBNK", "SISE", "BIMAS", "EREGL", "KCHOL", "PETKM", "VAKBN",
    "TCELL", "TUPRS", "TTKOM", "PGSUS", "DOHOL", "KOZAA", "MGROS", "ARCLK", "VESTL", "KRDMD",
    "FROTO", "HALKB", "ISCTR", "GARAN", "AKSA", "ALARK", "AYGAZ", "CCOLA", "EKGYO", "GUBRF"
]

ADDITIONAL_BIST_STOCKS = [
    "AGHOL", "AGROT", "AHGAZ", "AIDCP", "AKCNS", "AKGRT", "AKMGY", "AKSGY", "AKSUN", "ALBRK",
    "ALCTL", "ALFAS", "ALGYO", "ALMAD", "ALTIN", "ANELE", "ANHYT", "ANSGR", "ANTUR", "ARDYZ",
    "ARENA", "ARMDA", "ARSAN", "ARTMS", "ARZUM", "ASGYO", "ASLAN", "ASSET", "ATAKP", "ATATP",
    "ATEKS", "ATLAS", "ATSYH", "AVHOL", "AVTUR", "AYCES", "AYDEM", "BFREN", "BIGCH", "BIZIM",
    "BLCYT", "BMSCH", "BMSTL", "BNTAS", "BOBET", "BOSSA", "BRDGS", "BRKO", "BRKVY", "BRMEN",
    "BURCE", "BURVA", "BTCIM", "BUCIM", "CARSI", "CELHA", "CEMTS", "CEOEM", "CRDFA", "CRFSA",
    "CUSAN", "CVKMD", "DAGI", "DAPGM", "DCTTR", "DENGE", "DERIM", "DESPC", "DEVA", "DGATE",
    "DGGYO", "DGNMO", "DIASA", "DITAS", "DMRGD", "DNISI", "DOKTA", "DURDO", "DYOBY", "DZGYO",
    "EASTR", "EDATA", "EFORC", "EGDGL", "EGGUB", "EGPRO", "EGSER", "EIMSK", "EKIZ", "EKSUN",
    "ELITE", "EMKEL", "EMNIS", "ENSRI", "EPLAS", "ERBOY", "ERBOS", "ERGLI", "ERSU", "ESCAR",
    "ESCOM", "ETILR", "ETYAT", "EUHOL", "EUKYO", "EUREN", "EUYO", "EYGYO", "FADE", "FERRY",
    "FFORT", "FIDEO", "FINBN", "FLAP", "FMIZP", "FONET", "FORMT", "FORTE", "FRIGO", "FZLGY",
    "GARAN", "GEDIK", "GEDZA", "GENIL", "GENTS", "GEREL", "GESAN", "GIPTA", "GLYHO", "GMTAS",
    "GOKNR", "GOODY", "GOZDE", "GRSEL", "GSDDE", "GSDHO", "GSRAY", "GWIND", "GYODER", "HALKB"
]

ALL_BIST_STOCKS = list(set(BIST_100_STOCKS + BIST_30_STOCKS + ADDITIONAL_BIST_STOCKS))

# ----------------------- Config -----------------------
def getenv_list(key: str, default: str) -> list:
    return [s.strip() for s in os.getenv(key, default).split(",") if s.strip()]

SCAN_MODE = os.getenv("SCAN_MODE", "BIST_100")
CUSTOM_TICKERS = getenv_list("TICKERS", "")
TIMEFRAMES = getenv_list("TIMEFRAMES", "1d,4h,1h,15m")
CHECK_EVERY_MIN = int(os.getenv("CHECK_EVERY_MIN", "15"))
MIN_PRICE = float(os.getenv("MIN_PRICE", "1.0"))
MIN_VOLUME_TRY = float(os.getenv("MIN_VOLUME_TRY", "1000000"))
RSI_LEN = int(os.getenv("RSI_LEN", "22"))
RSI_EMA_LEN = int(os.getenv("RSI_EMA_LEN", "66"))
PIVOT_PERIOD = int(os.getenv("PIVOT_PERIOD", "10"))
ALERT_COOLDOWN_MIN = int(os.getenv("ALERT_COOLDOWN_MIN", "60"))
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "1.5"))
MACD_FAST = int(os.getenv("MACD_FAST", "12"))
MACD_SLOW = int(os.getenv("MACD_SLOW", "26"))
MACD_SIGNAL = int(os.getenv("MACD_SIGNAL", "9"))
BB_PERIOD = int(os.getenv("BB_PERIOD", "20"))
BB_MULT = float(os.getenv("BB_MULT", "2.0"))
CANDLE_FORM_THRESHOLD = float(os.getenv("CANDLE_FORM_THRESHOLD", "0.7"))

if SCAN_MODE == "BIST_30":
    TICKERS = BIST_30_STOCKS
elif SCAN_MODE == "BIST_100":
    TICKERS = BIST_100_STOCKS
elif SCAN_MODE == "ALL":
    TICKERS = ALL_BIST_STOCKS
elif SCAN_MODE == "CUSTOM" and CUSTOM_TICKERS:
    TICKERS = CUSTOM_TICKERS
else:
    TICKERS = BIST_30_STOCKS

# Global variables
LAST_ALERT: Dict[Tuple[str, str, str], float] = {}
LAST_SCAN_TIME = None
START_TIME = time.time()
ACTIVE_STOCKS: Set[str] = set()

# Puanlamayı saklamak için bir veri yapısı
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

# ----------------------- Sağlık Kontrolü -----------------------
class HealthHandler(web.View):
    async def get(self):
        response = {
            "status": "healthy",
            "service": "bist-scanner",
            "mode": SCAN_MODE,
            "total_stocks_to_scan": len(TICKERS),
            "active_stocks": len(ACTIVE_STOCKS),
            "timestamp": datetime.datetime.now(timezone.utc).isoformat(),
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
    logger.info(f"✅ Sağlık kontrolü sunucusu http://0.0.0.0:{HEALTH_CHECK_PORT} adresinde başlatıldı.")
    await stop_event.wait()
    await runner.cleanup()

# ----------------------- Utility Functions -----------------------
async def send_telegram(text: str):
    """Telegram mesajı gönderir, aiohttp ile asenkron."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram not configured, printing:")
        print(text)
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as r:
                if r.status == 200:
                    return True
                else:
                    logger.warning(f"Telegram failed: {r.status} - {await r.text()}")
    except Exception as e:
        logger.warning(f"Telegram error: {e}")
    return False

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
    bull_break = False
    bear_break = False
    breakout_angle = None
    
    if len(highs_idx) >= 2:
        i2, i1 = highs_idx[-1], highs_idx[-2]
        v2, v1 = rsi.iloc[i2], rsi.iloc[i1]
        if v2 < v1:
            m, b = trendline_from_pivots(i1, v1, i2, v2)
            y_now = value_on_line(m, b, now_i)
            if rsi.iloc[now_i] > y_now:
                bull_break = True
                breakout_angle = math.degrees(math.atan(m))
    
    if len(lows_idx) >= 2:
        j2, j1 = lows_idx[-1], lows_idx[-2]
        w2, w1 = rsi.iloc[j2], rsi.iloc[j1]
        if w2 > w1:
            m2, b2 = trendline_from_pivots(j1, w1, j2, w2)
            y_now2 = value_on_line(m2, b2, now_i)
            if rsi.iloc[now_i] < y_now2:
                bear_break = True
                breakout_angle = math.degrees(math.atan(m2))
    
    return bull_break, bear_break, breakout_angle

def is_doji(open_p, close_p, high_p, low_p, body_size_threshold=0.1):
    body_size = abs(close_p - open_p)
    full_range = high_p - low_p
    return body_size < full_range * body_size_threshold

def is_bullish_engulfing(prev_candle, last_candle):
    return (last_candle['close'] > last_candle['open'] and  # last is bullish
            prev_candle['close'] < prev_candle['open'] and  # prev is bearish
            last_candle['close'] > prev_candle['open'] and
            last_candle['open'] < prev_candle['close'])

def is_bearish_engulfing(prev_candle, last_candle):
    return (last_candle['close'] < last_candle['open'] and  # last is bearish
            prev_candle['close'] > prev_candle['open'] and  # prev is bullish
            last_candle['close'] < prev_candle['open'] and
            last_candle['open'] > prev_candle['close'])

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
    return (prev_c['close'] < prev_c['open'] and  # prev is bearish
            last_c['close'] > last_c['open'] and  # last is bullish
            last_c['open'] < prev_c['close'] and
            last_c['close'] > (prev_c['open'] + prev_c['close']) / 2)

def is_dark_cloud_cover(df):
    if len(df) < 2: return False
    prev_c, last_c = df.iloc[-2], df.iloc[-1]
    return (prev_c['close'] > prev_c['open'] and  # prev is bullish
            last_c['close'] < last_c['open'] and  # last is bearish
            last_c['open'] > prev_c['close'] and
            last_c['close'] < (prev_c['open'] + prev_c['close']) / 2)

def is_three_white_soldiers(df):
    if len(df) < 3: return False
    c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    return (c1['close'] > c1['open'] and c2['close'] > c2['open'] and c3['close'] > c3['open'] and
            c2['open'] > c1['open'] and c2['close'] > c1['close'] and
            c3['open'] > c2['open'] and c3['close'] > c2['close'])

def is_three_black_crows(df):
    if len(df) < 3: return False
    c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    return (c1['close'] < c1['open'] and c2['close'] < c2['open'] and c3['close'] < c3['open'] and
            c2['open'] < c1['open'] and c2['close'] < c1['close'] and
            c3['open'] < c2['open'] and c3['close'] < c2['close'])

def detect_candle_formation(df: pd.DataFrame) -> Optional[str]:
    """
    Genişletilmiş mum formasyonlarını tespit eder.
    """
    if len(df) < 3:
        return None

    last_candle = df.iloc[-1]
    prev_candle = df.iloc[-2]
    
    open_p, high_p, low_p, close_p = last_candle['open'], last_candle['high'], last_candle['low'], last_candle['close']
    
    # 3-Mum Formasyonları
    if is_three_white_soldiers(df): return "Three White Soldiers"
    if is_three_black_crows(df): return "Three Black Crows"
    
    # 2-Mum Formasyonları
    if is_bullish_engulfing(prev_candle, last_candle): return "Bullish Engulfing"
    if is_bearish_engulfing(prev_candle, last_candle): return "Bearish Engulfing"
    if is_piercing_pattern(df): return "Piercing Pattern"
    if is_dark_cloud_cover(df): return "Dark Cloud Cover"

    # 1-Mum Formasyonları
    if is_hammer(open_p, close_p, low_p, high_p): return "Hammer"
    if is_inverted_hammer(open_p, close_p, low_p, high_p): return "Inverted Hammer"
    if is_doji(open_p, close_p, high_p, low_p): return "Doji"

    return None

def calculate_signal_strength(signal: SignalInfo) -> float:
    score = 5.0
    
    if signal.direction == "BULLISH":
        if signal.rsi < 30: score += 3.0
        elif signal.rsi >= 30 and signal.rsi < 50: score += 1.0
    elif signal.direction == "BEARISH":
        if signal.rsi > 70: score += 3.0
        elif signal.rsi <= 70 and signal.rsi > 50: score += 1.0
            
    if signal.volume_ratio > 3.0: score += 2.5
    elif signal.volume_ratio > 2.0: score += 2.0
    elif signal.volume_ratio > 1.5: score += 1.0
    
    if signal.direction == "BULLISH" and signal.macd_signal == "BULLISH": score += 1.5
    elif signal.direction == "BEARISH" and signal.macd_signal == "BEARISH": score += 1.5
    
    if signal.direction == "BULLISH" and signal.rsi > signal.rsi_ema: score += 1.0
    elif signal.direction == "BEARISH" and signal.rsi < signal.rsi_ema: score += 1.0
    
    if signal.direction == "BULLISH" and signal.bb_position == "NEAR_LOWER": score += 0.5
    elif signal.direction == "BEARISH" and signal.bb_position == "NEAR_UPPER": score += 0.5
    
    if signal.price > 10: score += 0.5
    
    if signal.breakout_angle is not None:
        angle_abs = abs(signal.breakout_angle)
        if angle_abs > 30: score += 3.0
        elif angle_abs > 15: score += 2.0
        elif angle_abs > 5: score += 1.0
    
    # Mum Formasyonu Puanı (YENİ)
    if signal.candle_formation:
        if signal.candle_formation in ["Hammer", "Bullish Engulfing", "Piercing Pattern", "Three White Soldiers"]:
            score += 2.5 # Güçlü yükseliş sinyalleri
        elif signal.candle_formation in ["Inverted Hammer", "Bearish Engulfing", "Dark Cloud Cover", "Three Black Crows"]:
            score += 2.5 # Güçlü düşüş sinyalleri
        elif signal.candle_formation == "Doji":
            score += 1.0 # Kararsızlık, potansiyel dönüş
            
    # Çoklu Zaman Dilimi Puanı (YENİ)
    if signal.multi_tf_score:
        score += signal.multi_tf_score
    
    return min(10.0, max(0.0, score))

# ----------------------- Data Fetching & Analysis -----------------------
def fetch_and_analyze_data(symbol: str, timeframe: str) -> Optional[SignalInfo]:
    try:
        # Hisse senedi uzantısını (ör. .IS) yfinance için ayarla
        yf_symbol = f"{symbol}.IS"
        
        # Zaman dilimlerini yfinance formatına dönüştür
        yf_interval = ""
        period = ""
        if timeframe == '1d': 
            yf_interval = '1d'
            period = '730d' # 2 yıl
        elif timeframe == '4h': 
            yf_interval = '4h'
            period = '180d' # 6 ay
        elif timeframe == '1h': 
            yf_interval = '1h'
            period = '60d' # 2 ay
        elif timeframe == '15m': 
            yf_interval = '15m'
            period = '30d' # 1 ay
        
        if not yf_interval or not period: return None

        stock = yf.Ticker(yf_symbol)
        df = stock.history(interval=yf_interval, period=period)
        
        # Sütun isimlerini standartlaştır
        df.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'}, inplace=True)
        # Ek sütunları kaldır (varsa)
        if 'Dividends' in df.columns: df = df.drop(columns=['Dividends'])
        if 'Stock Splits' in df.columns: df = df.drop(columns=['Stock Splits'])


        if df is None or df.empty or len(df) < 50:
            raise RuntimeError(f"Insufficient data for {symbol} ({timeframe})")
        
        df = df.dropna().tail(300)

        close, volume = df["close"], df["volume"]
        rsi_series = rsi_from_close(close, RSI_LEN)
        rsi_ema = ema(rsi_series, RSI_EMA_LEN)
        macd_line, macd_signal, macd_hist = calculate_macd(close, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        bb_upper, bb_middle, bb_lower = calculate_bollinger_bands(close, BB_PERIOD, BB_MULT)
        
        volume_sma = sma(volume, 20)
        current_volume = volume.iloc[-1]
        avg_volume = volume_sma.iloc[-1]
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1.0

        highs_idx, lows_idx = find_pivots(rsi_series, PIVOT_PERIOD, PIVOT_PERIOD)
        now_i = len(rsi_series) - 1

        bull_break, bear_break, breakout_angle = detect_breakouts(rsi_series, highs_idx, lows_idx, now_i)
        
        current_rsi, current_rsi_ema = rsi_series.iloc[now_i], rsi_ema.iloc[now_i]
        current_price = close.iloc[-1]
        
        macd_signal_str = "NEUTRAL"
        if len(macd_hist) > 1:
            if macd_hist.iloc[-1] > 0 and macd_hist.iloc[-2] <= 0: macd_signal_str = "BULLISH"
            elif macd_hist.iloc[-1] < 0 and macd_hist.iloc[-2] >= 0: macd_signal_str = "BEARISH"

        is_bull_signal = bull_break or (macd_signal_str == "BULLISH" and current_rsi > 50)
        is_bear_signal = bear_break or (macd_signal_str == "BEARISH" and current_rsi < 50)

        if not (is_bull_signal or is_bear_signal): return None
        if volume_ratio < MIN_VOLUME_RATIO: return None

        bb_position = "MIDDLE"
        if current_price > bb_upper.iloc[-1]: bb_position = "UPPER"
        elif current_price < bb_lower.iloc[-1]: bb_position = "LOWER"
        
        direction = "BULLISH" if is_bull_signal else "BEARISH"
        volume_try = current_price * current_volume
        candle_form = detect_candle_formation(df)
        
        signal = SignalInfo(
            symbol=symbol, timeframe=timeframe, direction=direction, price=current_price,
            rsi=current_rsi, rsi_ema=current_rsi_ema, volume_ratio=volume_ratio,
            volume_try=volume_try, macd_signal=macd_signal_str, bb_position=bb_position,
            strength_score=0.0, timestamp=df.index[-1].strftime('%Y-%m-%d %H:%M'),
            breakout_angle=breakout_angle, candle_formation=candle_form
        )
        return signal

    except Exception as e:
        logger.error(f"Analysis error {symbol} {timeframe}: {e}")
        return None

def save_last_alert(symbol: str, direction: str):
    """
    Gönderilen sinyalleri diske kaydeder.
    """
    try:
        if os.path.exists("last_alerts.json"):
            with open("last_alerts.json", "r") as f:
                data = json.load(f)
        else:
            data = {}
        
        data[f"{symbol}_{direction}"] = datetime.datetime.now(timezone.utc).isoformat()
        
        with open("last_alerts.json", "w") as f:
            json.dump(data, f)
    except Exception as e:
        logger.error(f"Sinyal kaydetme hatası: {e}")

def get_last_alert(symbol: str, direction: str) -> Optional[datetime.datetime]:
    """
    Son sinyal gönderim tarihini diskten okur.
    """
    try:
        if os.path.exists("last_alerts.json"):
            with open("last_alerts.json", "r") as f:
                data = json.load(f)
            ts = data.get(f"{symbol}_{direction}")
            if ts:
                return datetime.datetime.fromisoformat(ts)
    except Exception as e:
        logger.error(f"Son sinyal okuma hatası: {e}")
    return None

async def run_scan_async():
    global LAST_SCAN_TIME, ACTIVE_STOCKS
    
    logger.info("🔍 Tam BIST taraması başladı - %s modu", SCAN_MODE)
    
    multi_tf_signals: Dict[str, List[SignalInfo]] = {}

    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {loop.run_in_executor(executor, fetch_and_analyze_data, symbol, timeframe): (symbol, timeframe)
                   for symbol in TICKERS for timeframe in TIMEFRAMES}
        
        for future in asyncio.as_completed(futures):
            symbol, timeframe = futures[future]
            try:
                result = await future
                if result:
                    if symbol not in multi_tf_signals:
                        multi_tf_signals[symbol] = []
                    multi_tf_signals[symbol].append(result)
            except Exception as e:
                logger.error(f"Tarama görevi başarısız: {symbol} - {e}")
    
    all_final_signals = []
    
    for symbol, signals in multi_tf_signals.items():
        if len(signals) > 0:
            total_score = 0
            # Zaman dilimi uyumuna göre puanlama (1d -> 4h -> 1h -> 15m)
            weights = {'1d': 4, '4h': 3, '1h': 2, '15m': 1}
            for signal in signals:
                total_score += signal.strength_score * weights.get(signal.timeframe, 1)

            # En yüksek puanlı sinyali ana sinyal olarak seç
            main_signal = max(signals, key=lambda s: s.strength_score)
            
            # Çoklu zaman dilimi puanını ekle
            main_signal.multi_tf_score = total_score / sum(weights.values())
            main_signal.strength_score = calculate_signal_strength(main_signal)
            all_final_signals.append(main_signal)

    if all_final_signals:
        all_final_signals.sort(key=lambda s: s.strength_score, reverse=True)
        for signal in all_final_signals:
            await send_enhanced_alert(signal)
    else:
        logger.info("🤷 Herhangi bir sinyal bulunamadı.")
    
    LAST_SCAN_TIME = datetime.datetime.now(timezone.utc)
    logger.info("✅ Tarama tamamlandı. Bir sonraki tarama %s dakika sonra.", CHECK_EVERY_MIN)

async def send_enhanced_alert(signal: SignalInfo):
    side_key = "AL" if signal.direction == "BULLISH" else "SAT"
    key = (signal.symbol, signal.timeframe, side_key)
    
    last_alert_time = get_last_alert(signal.symbol, side_key)
    if last_alert_time and (datetime.datetime.now(timezone.utc) - last_alert_time).total_seconds() < ALERT_COOLDOWN_MIN * 60:
        logger.info(f"⏳ {signal.symbol} için {side_key} sinyali cooldown'da.")
        return

    emoji = "🟢📈" if signal.direction == "BULLISH" else "🔴📉"
    direction_text = "AL" if signal.direction == "BULLISH" else "SAT"
    strength_stars = "⭐" * min(5, int(signal.strength_score / 2))
    
    if signal.volume_ratio > 5.0: volume_emoji = "🚀"
    elif signal.volume_ratio > 3.0: volume_emoji = "🔥"
    else: volume_emoji = "📈"

    if signal.volume_try >= 1000000: volume_str = f"{signal.volume_try/1000000:.1f}M TL"
    elif signal.volume_try >= 1000: volume_str = f"{signal.volume_try/1000:.0f}K TL"
    else: volume_str = f"{signal.volume_try:.0f} TL"

    angle_str = f"({signal.breakout_angle:.1f}°)" if signal.breakout_angle is not None else ""
    
    candle_form_str = f"\n🔥 **Mum Formasyonu:** {signal.candle_formation}" if signal.candle_formation else ""
    
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
• BB Pozisyon: {signal.bb_position}

{volume_emoji} <b>Hacim:</b> {signal.volume_ratio:.1f}x ({volume_str})
⚡ <b>Güç:</b> {signal.strength_score:.1f}/10 {strength_stars} {angle_str}

🕐 <b>Zaman:</b> {signal.timestamp} UTC
🔍 <b>Tarama:</b> {SCAN_MODE}

#BIST #{signal.symbol} #{signal.timeframe} #{SCAN_MODE}"""

    await send_telegram(msg)
    save_last_alert(signal.symbol, side_key)

async def main():
    logger.info("🚀 Gelişmiş BIST Sinyal Botu başlatılıyor...")
    stop_event = asyncio.Event()

    health_task = asyncio.create_task(start_health_server(asyncio.get_running_loop(), stop_event))
    
    await run_scan_async()
    
    while not stop_event.is_set():
        await asyncio.sleep(CHECK_EVERY_MIN * 60)
        await run_scan_async()

    await health_task

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot durduruldu.")
        pass
