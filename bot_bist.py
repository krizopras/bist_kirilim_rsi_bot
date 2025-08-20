#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tüm BIST Hisselerini Tarayan RSI Trendline Breakout Bot
- Tüm BIST hisselerini otomatik tarar (anlık alım satıma açık olanları)
- Yahoo Finance'dan veri çeker
- RSI trendline kırılımlarını tespit eder
- Volume ve teknik analiz filtresi uygular
"""

import os
import time
import logging
import threading
import gc
import json
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Dict, Optional, Set
from dataclasses import dataclass
from http.server import HTTPServer, BaseHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
import yfinance as yf

# Render.com için timezone ayarı
os.environ['TZ'] = 'UTC'

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
    "EGGUB", "EKDGS", "EMKEL", "EMNIS", "ENERY", "ETYAT", "EUHOL", "FENER", "FMIZP", "FORMT",
    "GENTS", "GEREL", "GESAN", "GLYHO", "GOODY", "GOZDE", "GSDHO", "GSRAY", "HATEK", "HDFGS",
    "HOROZ", "HUBVC", "IEYHO", "INFO", "INTEM", "ISGYO", "JANTS", "KAPLM", "KAREL", "KATMR",
    "KENT", "KERVT", "KLSER", "KNFRT", "KRONT", "KUTPO", "LMKDC", "LUKSK", "MACKO", "MARTI"
]

BIST_30_STOCKS = [
    "THYAO", "SAHOL", "ASTOR", "AKBNK", "SISE", "BIMAS", "EREGL", "KCHOL", "PETKM", "VAKBN",
    "TCELL", "TUPRS", "TTKOM", "PGSUS", "DOHOL", "KOZAA", "MGROS", "ARCLK", "VESTL", "KRDMD",
    "FROTO", "HALKB", "ISCTR", "GARAN", "AKSA", "ALARK", "AYGAZ", "CCOLA", "EKGYO", "GUBRF"
]

# Ek popüler BIST hisseleri
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

# Tüm BIST hisselerini birleştir
ALL_BIST_STOCKS = list(set(BIST_100_STOCKS + BIST_30_STOCKS + ADDITIONAL_BIST_STOCKS))

# ----------------------- Config -----------------------
def getenv_list(key: str, default: str) -> list:
    return [s.strip() for s in os.getenv(key, default).split(",") if s.strip()]

# Tarama modu
SCAN_MODE = os.getenv("SCAN_MODE", "BIST_100")  # BIST_30, BIST_100, ALL, CUSTOM
CUSTOM_TICKERS = getenv_list("TICKERS", "")
TIMEFRAMES = getenv_list("TIMEFRAMES", "1h,4h,1d")
CHECK_EVERY_MIN = int(os.getenv("CHECK_EVERY_MIN", "15"))

# Tarama moduna göre hisse listesini seç
if SCAN_MODE == "BIST_30":
    TICKERS = BIST_30_STOCKS
elif SCAN_MODE == "BIST_100":
    TICKERS = BIST_100_STOCKS
elif SCAN_MODE == "ALL":
    TICKERS = ALL_BIST_STOCKS
elif SCAN_MODE == "CUSTOM" and CUSTOM_TICKERS:
    TICKERS = CUSTOM_TICKERS
else:
    TICKERS = BIST_30_STOCKS  # Default

# Filtreleme parametreleri
MIN_PRICE = float(os.getenv("MIN_PRICE", "1.0"))  # Minimum hisse fiyatı
MAX_PRICE = float(os.getenv("MAX_PRICE", "1000.0"))  # Maximum hisse fiyatı
MIN_VOLUME_TRY = float(os.getenv("MIN_VOLUME_TRY", "1000000"))  # Minimum günlük hacim (TL)
MIN_MARKET_CAP_MILLION = float(os.getenv("MIN_MARKET_CAP_MILLION", "100"))  # Minimum market cap (milyon TL)

# RSI ve teknik parametreler
RSI_LEN = int(os.getenv("RSI_LEN", "22"))
RSI_EMA_LEN = int(os.getenv("RSI_EMA_LEN", "66"))
PIVOT_PERIOD = int(os.getenv("PIVOT_PERIOD", "10"))
ALERT_COOLDOWN_MIN = int(os.getenv("ALERT_COOLDOWN_MIN", "60"))

# Gelişmiş filtreler
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "1.5"))
MACD_FAST = int(os.getenv("MACD_FAST", "12"))
MACD_SLOW = int(os.getenv("MACD_SLOW", "26"))
MACD_SIGNAL = int(os.getenv("MACD_SIGNAL", "9"))
BB_PERIOD = int(os.getenv("BB_PERIOD", "20"))
BB_MULT = float(os.getenv("BB_MULT", "2.0"))

# Performance ayarları
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))  # Paralel işlem sayısı
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))
MAX_DATA_POINTS = int(os.getenv("MAX_DATA_POINTS", "300"))

YF_INTRADAY_PERIOD = os.getenv("YF_INTRADAY_PERIOD", "60d")
YF_DAILY_PERIOD = os.getenv("YF_DAILY_PERIOD", "2y")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Render ayarları
RENDER_PORT = int(os.getenv("PORT", "8000"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("full_bist_scanner")

# Global variables
LAST_ALERT: Dict[Tuple[str, str, str], float] = {}
START_TIME = time.time()
LAST_SCAN_TIME = None
ACTIVE_STOCKS: Set[str] = set()
BLACKLISTED_STOCKS: Set[str] = set()

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
    market_cap: Optional[float] = None

@dataclass
class StockInfo:
    symbol: str
    price: float
    volume: float
    market_cap: Optional[float]
    is_active: bool
    last_update: datetime

# ----------------------- Stock Discovery -----------------------
def discover_active_stocks() -> Set[str]:
    """Yahoo Finance'dan aktif BIST hisselerini keşfet"""
    logger.info("🔍 Aktif BIST hisselerini keşfediyor...")
    active_stocks = set()
    failed_stocks = set()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for ticker in TICKERS:
            future = executor.submit(check_stock_activity, ticker)
            futures[future] = ticker
        
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                is_active, info = future.result()
                if is_active:
                    active_stocks.add(ticker)
                    logger.debug(f"✅ {ticker}: Aktif (₺{info.get('price', 0):.2f})")
                else:
                    failed_stocks.add(ticker)
                    logger.debug(f"❌ {ticker}: İnaktif/Veri yok")
            except Exception as e:
                failed_stocks.add(ticker)
                logger.warning(f"⚠️ {ticker} kontrol hatası: {e}")
    
    logger.info(f"✅ {len(active_stocks)} aktif hisse bulundu, {len(failed_stocks)} hisse filtrelendi")
    return active_stocks

def check_stock_activity(ticker: str) -> Tuple[bool, Dict]:
    """Tek bir hissenin aktif olup olmadığını kontrol et"""
    try:
        symbol = ensure_is_suffix(ticker)
        
        # Son 5 günlük veri al
        df = yf.download(symbol, period="5d", interval="1d", 
                        auto_adjust=False, progress=False, timeout=REQUEST_TIMEOUT)
        
        if df is None or df.empty or len(df) < 2:
            return False, {}
        
        # Son fiyat ve hacim bilgileri
        df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
        df = df.rename(columns={"Open":"open","High":"high","Low":"low","Close":"close","Volume":"volume"})
        
        last_close = df["close"].iloc[-1]
        last_volume = df["volume"].iloc[-1]
        
        # Filtreler
        if last_close < MIN_PRICE or last_close > MAX_PRICE:
            return False, {"reason": "price_range"}
        
        # Hacim kontrolü (TL cinsinden)
        volume_try = last_close * last_volume
        if volume_try < MIN_VOLUME_TRY:
            return False, {"reason": "low_volume", "volume_try": volume_try}
        
        # Son 2 günde işlem var mı kontrol et
        recent_volumes = df["volume"].tail(2)
        if all(vol == 0 for vol in recent_volumes):
            return False, {"reason": "no_recent_activity"}
        
        return True, {
            "price": last_close,
            "volume": last_volume,
            "volume_try": volume_try,
            "last_update": datetime.now(timezone.utc)
        }
        
    except Exception as e:
        logger.debug(f"Stock check error {ticker}: {e}")
        return False, {"error": str(e)}

# ----------------------- Health Check -----------------------
class HealthHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass
    
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {
                "status": "healthy",
                "service": "full-bist-scanner",
                "mode": SCAN_MODE,
                "total_stocks": len(TICKERS),
                "active_stocks": len(ACTIVE_STOCKS),
                "blacklisted": len(BLACKLISTED_STOCKS),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "uptime_seconds": time.time() - START_TIME,
                "last_scan": LAST_SCAN_TIME.isoformat() if LAST_SCAN_TIME else None
            }
            self.wfile.write(json.dumps(response, indent=2).encode())
        elif self.path == '/stocks':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {
                "active_stocks": list(ACTIVE_STOCKS),
                "blacklisted_stocks": list(BLACKLISTED_STOCKS),
                "total_monitored": len(ACTIVE_STOCKS)
            }
            self.wfile.write(json.dumps(response, indent=2).encode())
        else:
            self.send_response(404)
            self.end_headers()

def start_health_server():
    try:
        server = HTTPServer(('0.0.0.0', RENDER_PORT), HealthHandler)
        logger.info(f"🌐 Health server started on port {RENDER_PORT}")
        server.serve_forever()
    except Exception as e:
        logger.error(f"Health server error: {e}")

# ----------------------- Utility Functions -----------------------
def send_telegram(text: str):
    """Telegram mesajı gönder"""
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
    
    max_retries = 2
    for attempt in range(max_retries):
        try:
            r = requests.post(url, json=payload, timeout=10)
            if r.status_code == 200:
                return True
            else:
                logger.warning(f"Telegram attempt {attempt + 1} failed: {r.status_code}")
        except Exception as e:
            logger.warning(f"Telegram error attempt {attempt + 1}: {e}")
        
        if attempt < max_retries - 1:
            time.sleep(1)
    
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

def trendline_from_pivots(idx1: int, val1: float, idx2: int, val2: float):
    if idx2 == idx1:
        return 0.0, val2
    m = (val2 - val1) / (idx2 - idx1)
    b = val2 - m * idx2
    return m, b

def value_on_line(m: float, b: float, x: float) -> float:
    return m * x + b

def detect_breakouts(rsi: pd.Series, highs_idx: List[int], lows_idx: List[int], now_i: int):
    bull_break = False
    bear_break = False
    
    if len(highs_idx) >= 2:
        i2, i1 = highs_idx[-1], highs_idx[-2]
        v2, v1 = rsi.iloc[i2], rsi.iloc[i1]
        if v2 < v1:
            m, b = trendline_from_pivots(i1, v1, i2, v2)
            y_now = value_on_line(m, b, now_i)
            bull_break = rsi.iloc[now_i] > y_now
    
    if len(lows_idx) >= 2:
        j2, j1 = lows_idx[-1], lows_idx[-2]
        w2, w1 = rsi.iloc[j2], rsi.iloc[j1]
        if w2 > w1:
            m2, b2 = trendline_from_pivots(j1, w1, j2, w2)
            y_now2 = value_on_line(m2, b2, now_i)
            bear_break = rsi.iloc[now_i] < y_now2
    
    return bull_break, bear_break

def calculate_signal_strength(signal: SignalInfo) -> float:
    score = 5.0
    
    # Hacim puanı
    if signal.volume_ratio > 3.0:
        score += 2.5
    elif signal.volume_ratio > 2.0:
        score += 2.0
    elif signal.volume_ratio > 1.5:
        score += 1.0
    
    # MACD onayı
    if signal.direction == "BULLISH" and signal.macd_signal == "BULLISH":
        score += 1.5
    elif signal.direction == "BEARISH" and signal.macd_signal == "BEARISH":
        score += 1.5
    
    # RSI-EMA onayı
    if signal.direction == "BULLISH" and signal.rsi > signal.rsi_ema:
        score += 1.0
    elif signal.direction == "BEARISH" and signal.rsi < signal.rsi_ema:
        score += 1.0
    
    # Bollinger Bands pozisyonu
    if signal.direction == "BULLISH" and signal.bb_position == "NEAR_LOWER":
        score += 0.5
    elif signal.direction == "BEARISH" and signal.bb_position == "NEAR_UPPER":
        score += 0.5
    
    # Fiyat seviyesi bonusu (düşük fiyatlı hisseler daha riskli)
    if signal.price > 10:
        score += 0.5
    elif signal.price < 2:
        score -= 0.5
    
    return min(10.0, max(0.0, score))

def ensure_is_suffix(ticker: str) -> str:
    return ticker if ticker.upper().endswith(".IS") else (ticker.upper() + ".IS")

# ----------------------- Data Fetching -----------------------
def fetch_yf_df(symbol: str, timeframe: str) -> pd.DataFrame:
    """Yahoo Finance'dan veri çek"""
    yf_symbol = ensure_is_suffix(symbol)
    
    try:
        if timeframe in ("1h", "4h"):
            df = yf.download(tickers=yf_symbol, interval="60m", period=YF_INTRADAY_PERIOD, 
                           auto_adjust=False, progress=False, timeout=REQUEST_TIMEOUT)
        elif timeframe == "1d":
            df = yf.download(tickers=yf_symbol, interval="1d", period=YF_DAILY_PERIOD, 
                           auto_adjust=False, progress=False, timeout=REQUEST_TIMEOUT)
        else:
            raise ValueError(f"Unsupported timeframe: {timeframe}")
        
        if df is None or df.empty:
            raise RuntimeError(f"No data for {yf_symbol}")
        
        # Sütun standardizasyonu
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
        
        df = df.rename(columns={"Open":"open","High":"high","Low":"low","Close":"close","Volume":"volume"})
        df = df.dropna(subset=["close"])
        
        if len(df) < 50:
            raise RuntimeError(f"Insufficient data: {len(df)} bars")
        
        # Memory optimization
        if len(df) > MAX_DATA_POINTS:
            df = df.tail(MAX_DATA_POINTS)
        
        # Timezone handling
        if df.index.tz is not None:
            df.index = df.index.tz_convert("UTC").tz_localize(None)
        
        # 4h resampling
        if timeframe == "4h":
            o = df["open"].resample("4H", label="right", closed="right").first()
            h = df["high"].resample("4H", label="right", closed="right").max()
            l = df["low"].resample("4H", label="right", closed="right").min()
            c = df["close"].resample("4H", label="right", closed="right").last()
            v = df["volume"].resample("4H", label="right", closed="right").sum()
            df = pd.concat([o,h,l,c,v], axis=1).dropna()
            df.columns = ["open","high","low","close","volume"]
        
        return df
        
    except Exception as e:
        raise RuntimeError(f"Fetch error for {yf_symbol}: {e}")

# ----------------------- Analysis -----------------------
def analyze_symbol_timeframe(symbol: str, timeframe: str) -> Optional[SignalInfo]:
    """Tekil hisse analizi"""
    try:
        df = fetch_yf_df(symbol, timeframe)
    except Exception as e:
        # Başarısız hisseleri blacklist'e ekle
        BLACKLISTED_STOCKS.add(symbol)
        if symbol in ACTIVE_STOCKS:
            ACTIVE_STOCKS.remove(symbol)
        logger.debug(f"❌ {symbol} blacklisted: {e}")
        return None

    try:
        close = df["close"]
        volume = df["volume"]
        
        # İndikatörler
        rsi_series = rsi_from_close(close, RSI_LEN)
        rsi_ema = ema(rsi_series, RSI_EMA_LEN)
        
        macd_line, macd_signal, macd_hist = calculate_macd(close, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        bb_upper, bb_middle, bb_lower = calculate_bollinger_bands(close, BB_PERIOD, BB_MULT)
        
        # Hacim analizi
        volume_sma = sma(volume, 20)
        current_volume = volume.iloc[-1]
        avg_volume = volume_sma.iloc[-1]
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1.0

        # Pivots ve breakout tespiti
        highs_idx, lows_idx = find_pivots(rsi_series, PIVOT_PERIOD, PIVOT_PERIOD)
        now_i = len(rsi_series) - 1

        bull_break, bear_break = detect_breakouts(rsi_series, highs_idx, lows_idx, now_i)

        current_rsi = rsi_series.iloc[now_i]
        current_rsi_ema = rsi_ema.iloc[now_i]
        current_price = close.iloc[-1]

        # Gelişmiş filtreleme
        if bull_break:
            if current_rsi < 30:
                bull_break = bull_break and current_rsi > current_rsi_ema
            elif current_rsi > 70:
                bull_break = False
            else:
                bull_break = bull_break and current_rsi > current_rsi_ema

        if bear_break:
            if current_rsi > 70:
                bear_break = bear_break and current_rsi < current_rsi_ema
            elif current_rsi < 30:
                bear_break = False
            else:
                bear_break = bear_break and current_rsi < current_rsi_ema

        # Hacim filtresi
        if (bull_break or bear_break) and volume_ratio < MIN_VOLUME_RATIO:
            bull_break = bear_break = False

        if not (bull_break or bear_break):
            return None

        # MACD sinyali
        macd_signal_str = "NEUTRAL"
        if len(macd_hist) > 1:
            if macd_hist.iloc[-1] > 0 and macd_hist.iloc[-2] <= 0:
                macd_signal_str = "BULLISH"
            elif macd_hist.iloc[-1] < 0 and macd_hist.iloc[-2] >= 0:
                macd_signal_str = "BEARISH"
            elif macd_hist.iloc[-1] > macd_hist.iloc[-2]:
                macd_signal_str = "BULLISH"
            elif macd_hist.iloc[-1] < macd_hist.iloc[-2]:
                macd_signal_str = "BEARISH"

        # Bollinger Bands pozisyonu
        bb_position = "MIDDLE"
        current_bb_upper = bb_upper.iloc[-1]
        current_bb_lower = bb_lower.iloc[-1]
        bb_range = current_bb_upper - current_bb_lower
        
        if current_price > current_bb_upper - (bb_range * 0.1):
            bb_position = "NEAR_UPPER"
        elif current_price < current_bb_lower + (bb_range * 0.1):
            bb_position = "NEAR_LOWER"

        direction = "BULLISH" if bull_break else "BEARISH"
        
        # Hacim TL cinsinden
        volume_try = current_price * current_volume
        
        signal = SignalInfo(
            symbol=symbol,
            timeframe=timeframe,
            direction=direction,
            price=current_price,
            rsi=current_rsi,
            rsi_ema=current_rsi_ema,
            volume_ratio=volume_ratio,
            volume_try=volume_try,
            macd_signal=macd_signal_str,
            bb_position=bb_position,
            strength_score=0.0,
            timestamp=df.index[-1].strftime('%Y-%m-%d %H:%M')
        )
        
        signal.strength_score = calculate_signal_strength(signal)
        return signal
        
    except Exception as e:
        logger.error(f"Analysis error {symbol} {timeframe}: {e}")
        return None

def send_enhanced_alert(signal: SignalInfo):
    """Gelişmiş Telegram uyarısı gönder"""
    # Cooldown kontrolü
    side_key = "LONG" if signal.direction == "BULLISH" else "SHORT"
    key = (signal.symbol, signal.timeframe, side_key)
    last_ts = LAST_ALERT.get(key, 0.0)
    now_ts = time.time()
    
    if now_ts - last_ts < ALERT_COOLDOWN_MIN * 60:
        return
        
    LAST_ALERT[key] = now_ts

    # Mesaj formatı
    if signal.direction == "BULLISH":
        emoji = "🟢📈"
        direction_text = "YUKARI KIRILIM"
    else:
        emoji = "🔴📉"
        direction_text = "AŞAĞI KIRILIM"

    strength_stars = "⭐" * min(5, int(signal.strength_score / 2))
    
    # Hacim göstergesi
    if signal.volume_ratio > 5.0:
        volume_emoji = "🚀"
    elif signal.volume_ratio > 3.0:
        volume_emoji = "🔥"
    elif signal.volume_ratio > 2.0:
        volume_emoji = "📊"
    else:
        volume_emoji = "📈"

    # Hacim formatı
    if signal.volume_try >= 1000000:
        volume_str = f"{signal.volume_try/1000000:.1f}M TL"
    elif signal.volume_try >= 1000:
        volume_str = f"{signal.volume_try/1000:.0f}K TL"
    else:
        volume_str = f"{signal.volume_try:.0f} TL"

    msg = f"""<b>{emoji} RSI TRENDLINE BREAK</b>

📊 <b>Hisse:</b> {signal.symbol}.IS
⏰ <b>Zaman:</b> {signal.timeframe}
🎯 <b>Yön:</b> {direction_text}
💰 <b>Fiyat:</b> ₺{signal.price:.2f}

📈 <b>Teknik Durum:</b>
• RSI({RSI_LEN}): {signal.rsi:.1f}
• RSI EMA({RSI_EMA_LEN}): {signal.rsi_ema:.1f}
• MACD: {signal.macd_signal}
• BB Pozisyon: {signal.bb_position}

{volume_emoji} <b>Hacim:</b> {signal.volume_ratio:.1f}x ({volume_str})
⚡ <b>Güç:</b> {signal.strength_score:.1f}/10 {strength_stars}

🕐 <b>Zaman:</b> {signal.timestamp} UTC
🔍 <b>Tarama:</b> {SCAN_MODE} ({len(ACTIVE_STOCKS)} hisse)

#BIST #{signal.symbol} #{signal.timeframe} #{SCAN_MODE}"""

    success = send_telegram(msg)
    if success:
        logger.info(f"✅ Alert sent: {signal.symbol} {signal.timeframe} {signal.direction} (Güç: {signal.strength_score:.1f})")
    else:
        logger.error(f"❌ Alert failed: {signal.symbol} {signal.timeframe}")

def parallel_scan_stocks(stocks: List[str], timeframe: str) -> List[SignalInfo]:
    """Paralel hisse tarama"""
    signals = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for stock in stocks:
            future = executor.submit(analyze_symbol_timeframe, stock, timeframe)
            futures[future] = stock
        
        for future in as_completed(futures):
            stock = futures[future]
            try:
                signal = future.result()
                if signal:
                    signals.append(signal)
            except Exception as e:
                logger.debug(f"Scan error {stock} {timeframe}: {e}")
    
    return signals

def job():
    """Ana tarama işi"""
    global LAST_SCAN_TIME, ACTIVE_STOCKS
    
    start_time = time.time()
    logger.info(f"🔍 Tam BIST taraması başladı - {SCAN_MODE} modu")
    
    # Aktif hisseleri güncelle (her 4 taramada bir)
    scan_count = getattr(job, 'count', 0)
    job.count = scan_count + 1
    
    if scan_count % 4 == 0:  # Her 1 saatte bir (15dk * 4 = 60dk)
        ACTIVE_STOCKS = discover_active_stocks()
    
    if not ACTIVE_STOCKS:
        logger.warning("❌ Aktif hisse bulunamadı, varsayılan listeyi kullanıyor")
        ACTIVE_STOCKS = set(BIST_30_STOCKS)
    
    total_signals = 0
    total_scanned = 0
    errors = 0
    
    try:
        # Her timeframe için paralel tarama
        for timeframe in TIMEFRAMES:
            tf_start = time.time()
            active_list = list(ACTIVE_STOCKS)
            
            logger.info(f"⏰ {timeframe} taraması başladı - {len(active_list)} hisse")
            
            signals = parallel_scan_stocks(active_list, timeframe)
            
            # Sinyalleri güç skoruna göre sırala
            signals.sort(key=lambda s: s.strength_score, reverse=True)
            
            # En güçlü sinyalleri gönder (spam önlemek için max 5 per timeframe)
            for signal in signals[:5]:
                send_enhanced_alert(signal)
                time.sleep(1)  # Rate limiting
                total_signals += 1
            
            total_scanned += len(active_list)
            tf_duration = time.time() - tf_start
            
            logger.info(f"✅ {timeframe}: {len(signals)} sinyal, {tf_duration:.1f}s")
            
            # Memory cleanup
            gc.collect()
        
        scan_duration = time.time() - start_time
        LAST_SCAN_TIME = datetime.now(timezone.utc)
        
        # Özet raporu
        logger.info(f"""
🎯 TARAMA TAMAMLANDI
• Mod: {SCAN_MODE}
• Taranan hisse: {len(ACTIVE_STOCKS)}
• Toplam kontrol: {total_scanned}
• Bulunan sinyal: {total_signals}
• Süre: {scan_duration:.1f}s
• Blacklist: {len(BLACKLISTED_STOCKS)}
        """.strip())
        
        # Günlük özet (her 4 saatte bir)
        if scan_count % 16 == 0:  # 4 saat
            send_daily_summary()
        
    except Exception as e:
        logger.error(f"❌ Kritik tarama hatası: {e}")
    
    # Memory cleanup
    gc.collect()

def send_daily_summary():
    """Günlük özet raporu"""
    uptime_hours = (time.time() - START_TIME) / 3600
    
    summary = f"""📈 <b>BIST BOT GÜNLÜK ÖZET</b>

🤖 <b>Sistem Durumu:</b>
• Mod: {SCAN_MODE}
• Çalışma süresi: {uptime_hours:.1f} saat
• Aktif hisse: {len(ACTIVE_STOCKS)}
• Blacklist: {len(BLACKLISTED_STOCKS)}

⏰ <b>Tarama Ayarları:</b>
• Zaman dilimleri: {', '.join(TIMEFRAMES)}
• Tarama aralığı: {CHECK_EVERY_MIN}dk
• Son tarama: {LAST_SCAN_TIME.strftime('%H:%M') if LAST_SCAN_TIME else 'Henüz yok'}

💪 <b>Filtreler:</b>
• Min fiyat: ₺{MIN_PRICE}
• Min hacim: {MIN_VOLUME_TRY/1000000:.1f}M TL
• Min hacim oranı: {MIN_VOLUME_RATIO}x

🎯 Bot aktif olarak {len(ACTIVE_STOCKS)} BIST hissesini izliyor!
#GünlükÖzet #BIST #{SCAN_MODE}"""
    
    send_telegram(summary)

def test_telegram():
    """Telegram bağlantı testi"""
    test_msg = f"""🚀 <b>FULL BIST SCANNER STARTED</b>

✅ Bot başarıyla başlatıldı!
🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC

📊 <b>Tarama Konfigürasyonu:</b>
• Mod: <b>{SCAN_MODE}</b>
• Hedef hisse sayısı: {len(TICKERS)}
• Zaman dilimleri: {', '.join(TIMEFRAMES)}
• Tarama aralığı: {CHECK_EVERY_MIN} dakika

🎯 <b>Filtreler:</b>
• Fiyat aralığı: ₺{MIN_PRICE} - ₺{MAX_PRICE}
• Min hacim: {MIN_VOLUME_TRY/1000000:.1f}M TL/gün
• Min hacim oranı: {MIN_VOLUME_RATIO}x

🔧 <b>Teknik Parametreler:</b>
• RSI({RSI_LEN}) + EMA({RSI_EMA_LEN})
• MACD({MACD_FAST},{MACD_SLOW},{MACD_SIGNAL})
• Bollinger Bands({BB_PERIOD},{BB_MULT})

🚀 Tüm BIST için hazır!
#BotStarted #BIST #{SCAN_MODE}"""
    
    send_telegram(test_msg)

def main():
    """Ana fonksiyon"""
    logger.info("🚀 Full BIST Scanner başlatılıyor...")
    logger.info(f"📊 Tarama modu: {SCAN_MODE}")
    logger.info(f"🎯 Hedef hisse sayısı: {len(TICKERS)}")
    logger.info(f"⚙️ Max worker: {MAX_WORKERS}")
    logger.info(f"💾 Max data points: {MAX_DATA_POINTS}")
    logger.info(f"🌐 Health server port: {RENDER_PORT}")
    
    # Health server başlat
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()
    
    # Telegram test
    try:
        test_telegram()
    except Exception as e:
        logger.error(f"Telegram test failed: {e}")
    
    # İlk tarama
    try:
        job()
    except Exception as e:
        logger.error(f"İlk tarama hatası: {e}")
    
    # Zamanlayıcı kurulumu
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        job, 
        "cron", 
        minute=f"*/{CHECK_EVERY_MIN}",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60
    )
    
    try:
        logger.info("⏰ Zamanlayıcı başlatıldı - Full BIST Scanner aktif!")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("🛑 Bot kapatılıyor...")
    except Exception as e:
        logger.error(f"Zamanlayıcı hatası: {e}")

if __name__ == "__main__":
    main()