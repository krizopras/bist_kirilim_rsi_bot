#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tüm BIST Hisselerini Tarayan RSI Trendline Breakout Bot
- Tüm BIST hisselerini otomatik tarar (anlık alım satıma açık olanları)
- Yahoo Finance'dan veri çeker (investpy yerine yfinance kullanılması önerilir)
- RSI trendline kırılımlarını tespit eder
- Volume ve teknik analiz filtresi uygular
"""

import os
import time
import logging
import threading
import gc
import json
import math
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Dict, Optional, Set
from dataclasses import dataclass
from http.server import HTTPServer, BaseHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
import yfinance as yf  # investpy yerine yfinance kullan (daha stabil)

# Render.com için timezone ayarı
os.environ['TZ'] = 'Europe/Istanbul'  # Türkiye saati için

# ----------------------- BIST Hisse Listeleri -----------------------
BIST_100_STOCKS = [
    "THYAO.IS", "SAHOL.IS", "ASTOR.IS", "AKBNK.IS", "SISE.IS", "BIMAS.IS", "EREGL.IS", 
    "KCHOL.IS", "PETKM.IS", "VAKBN.IS", "TCELL.IS", "TUPRS.IS", "TTKOM.IS", "TOASO.IS", 
    "PGSUS.IS", "ULKER.IS", "DOHOL.IS", "KOZAA.IS", "KOZAL.IS", "MGROS.IS", "ARCLK.IS", 
    "VESTL.IS", "KRDMD.IS", "FROTO.IS", "ECILC.IS", "HALKB.IS", "ISCTR.IS", "GARAN.IS", 
    "AKSA.IS", "ALARK.IS", "AYGAZ.IS", "BRSAN.IS", "CCOLA.IS", "CIMSA.IS", "DOAS.IS", 
    "EGEEN.IS", "EKGYO.IS", "ENKAI.IS", "ENJSA.IS", "ERBOS.IS", "GUBRF.IS", "HEKTS.IS", 
    "IPEKE.IS", "KARSN.IS", "KLMSN.IS", "KONYA.IS", "KORDS.IS", "LOGO.IS", "MAVI.IS", 
    "ODAS.IS", "OTKAR.IS", "OYAKC.IS", "PARSN.IS", "PENGD.IS", "PRKME.IS", "QUAGR.IS", 
    "RAYSG.IS", "SARKY.IS", "SELEC.IS", "SKBNK.IS", "SOKM.IS", "TATGD.IS", "TAVHL.IS", 
    "TKFEN.IS", "TLMAN.IS", "TMSN.IS", "TRGYO.IS", "TSKB.IS", "TURSG.IS", "UMPAS.IS", 
    "VAKKO.IS", "VERUS.IS", "YATAS.IS", "YKBNK.IS", "ZOREN.IS"
]

BIST_30_STOCKS = [
    "THYAO.IS", "SAHOL.IS", "ASTOR.IS", "AKBNK.IS", "SISE.IS", "BIMAS.IS", "EREGL.IS", 
    "KCHOL.IS", "PETKM.IS", "VAKBN.IS", "TCELL.IS", "TUPRS.IS", "TTKOM.IS", "PGSUS.IS", 
    "DOHOL.IS", "KOZAA.IS", "MGROS.IS", "ARCLK.IS", "VESTL.IS", "KRDMD.IS", "FROTO.IS", 
    "HALKB.IS", "ISCTR.IS", "GARAN.IS", "AKSA.IS", "ALARK.IS", "AYGAZ.IS", "CCOLA.IS", 
    "EKGYO.IS", "GUBRF.IS"
]

# Ek popüler BIST hisseleri
ADDITIONAL_BIST_STOCKS = [
    "AGHOL.IS", "AGROT.IS", "AHGAZ.IS", "AIDCP.IS", "AKCNS.IS", "AKGRT.IS", "AKMGY.IS", 
    "AKSGY.IS", "AKSUN.IS", "ALBRK.IS", "ALCTL.IS", "ALFAS.IS", "ALGYO.IS", "ALMAD.IS", 
    "ALTIN.IS", "ANELE.IS", "ANHYT.IS", "ANSGR.IS", "ANTUR.IS", "ARDYZ.IS", "ARENA.IS", 
    "ARMDA.IS", "ARSAN.IS", "ARTMS.IS", "ARZUM.IS", "ASGYO.IS", "ASLAN.IS", "ASSET.IS", 
    "ATAKP.IS", "ATATP.IS", "ATEKS.IS", "ATLAS.IS", "ATSYH.IS", "AVHOL.IS", "AVTUR.IS", 
    "AYCES.IS", "AYDEM.IS", "BFREN.IS", "BIGCH.IS", "BIZIM.IS", "BLCYT.IS", "BMSCH.IS", 
    "BMSTL.IS", "BNTAS.IS", "BOBET.IS", "BOSSA.IS", "BRDGS.IS", "BRKO.IS", "BRKVY.IS", 
    "BRMEN.IS", "BURCE.IS", "BURVA.IS", "BTCIM.IS", "BUCIM.IS", "CARSI.IS", "CELHA.IS"
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
    TICKERS = [f"{t}.IS" if not t.endswith(".IS") else t for t in CUSTOM_TICKERS]
else:
    TICKERS = BIST_30_STOCKS  # Default

# Filtreleme parametreleri
MIN_PRICE = float(os.getenv("MIN_PRICE", "1.0"))
MAX_PRICE = float(os.getenv("MAX_PRICE", "1000.0"))
MIN_VOLUME_TRY = float(os.getenv("MIN_VOLUME_TRY", "1000000"))
MIN_MARKET_CAP_MILLION = float(os.getenv("MIN_MARKET_CAP_MILLION", "100"))

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
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))  # Render'da CPU limiti düşük
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))  # Timeout artırıldı
MAX_DATA_POINTS = int(os.getenv("MAX_DATA_POINTS", "500"))

YF_INTRADAY_PERIOD = os.getenv("YF_INTRADAY_PERIOD", "60d")
YF_DAILY_PERIOD = os.getenv("YF_DAILY_PERIOD", "2y")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Render ayarları
RENDER_PORT = int(os.getenv("PORT", "10000"))

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
STOCK_INFO_CACHE: Dict[str, dict] = {}

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
    breakout_angle: Optional[float] = None
    price_change_24h: Optional[float] = None

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
    
    # Batch processing - küçük gruplar halinde işle
    batch_size = 20
    for i in range(0, len(TICKERS), batch_size):
        batch = TICKERS[i:i+batch_size]
        logger.info(f"📦 Batch {i//batch_size + 1}/{math.ceil(len(TICKERS)/batch_size)} işleniyor...")
        
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(batch))) as executor:
            futures = {}
            for ticker in batch:
                future = executor.submit(check_stock_activity, ticker)
                futures[future] = ticker
            
            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    is_active, info = future.result()
                    if is_active:
                        active_stocks.add(ticker)
                        STOCK_INFO_CACHE[ticker] = info
                        logger.debug(f"✅ {ticker}: Aktif (₺{info.get('price', 0):.2f})")
                    else:
                        failed_stocks.add(ticker)
                        logger.debug(f"❌ {ticker}: İnaktif/Veri yok")
                except Exception as e:
                    failed_stocks.add(ticker)
                    logger.warning(f"⚠️ {ticker} kontrol hatası: {e}")
        
        # API rate limit için bekleme
        time.sleep(2)
    
    logger.info(f"✅ {len(active_stocks)} aktif hisse bulundu, {len(failed_stocks)} hisse filtrelendi")
    return active_stocks

def check_stock_activity(ticker: str) -> Tuple[bool, Dict]:
    """Tek bir hissenin aktif olup olmadığını yfinance ile kontrol et"""
    try:
        # .IS uzantısını ekle
        symbol = ticker if ticker.endswith(".IS") else f"{ticker}.IS"
        
        # yfinance ticker oluştur
        yf_ticker = yf.Ticker(symbol)
        
        # Son 10 günlük veri al
        hist = yf_ticker.history(period="10d", interval="1d", timeout=REQUEST_TIMEOUT)
        
        if hist is None or hist.empty or len(hist) < 2:
            return False, {"reason": "no_data"}
        
        # Sütun isimlerini küçük harfe çevir
        hist.columns = hist.columns.str.lower()
        
        last_close = hist["close"].iloc[-1]
        last_volume = hist["volume"].iloc[-1]
        
        # Filtreler
        if last_close < MIN_PRICE or last_close > MAX_PRICE:
            return False, {"reason": "price_range", "price": last_close}
        
        # Hacim kontrolü (TL cinsinden)
        volume_try = last_close * last_volume
        if volume_try < MIN_VOLUME_TRY:
            return False, {"reason": "low_volume", "volume_try": volume_try}
        
        # Son 3 günde işlem var mı kontrol et
        recent_volumes = hist["volume"].tail(3)
        if all(vol == 0 for vol in recent_volumes):
            return False, {"reason": "no_recent_activity"}
        
        # Hisse bilgilerini al
        info = yf_ticker.info
        market_cap = info.get('marketCap', 0) if info else 0
        
        # Market cap filtresi (TL cinsinden)
        if market_cap > 0:
            market_cap_try = market_cap  # Yahoo'dan gelen değer zaten TL olabilir
            if market_cap_try < MIN_MARKET_CAP_MILLION * 1000000:
                return False, {"reason": "low_market_cap", "market_cap": market_cap_try}
        
        # 24 saatlik değişim hesapla
        price_change_24h = 0.0
        if len(hist) >= 2:
            price_change_24h = ((last_close - hist["close"].iloc[-2]) / hist["close"].iloc[-2]) * 100
        
        return True, {
            "price": last_close,
            "volume": last_volume,
            "volume_try": volume_try,
            "market_cap": market_cap,
            "price_change_24h": price_change_24h,
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
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            uptime = time.time() - START_TIME
            response = {
                "status": "healthy",
                "service": "full-bist-scanner",
                "version": "2.0",
                "mode": SCAN_MODE,
                "total_stocks": len(TICKERS),
                "active_stocks": len(ACTIVE_STOCKS),
                "blacklisted": len(BLACKLISTED_STOCKS),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "uptime_seconds": uptime,
                "uptime_formatted": f"{uptime//3600:.0f}h {(uptime%3600)//60:.0f}m",
                "last_scan": LAST_SCAN_TIME.isoformat() if LAST_SCAN_TIME else None,
                "config": {
                    "check_interval_min": CHECK_EVERY_MIN,
                    "min_price": MIN_PRICE,
                    "max_price": MAX_PRICE,
                    "min_volume_try": MIN_VOLUME_TRY,
                    "timeframes": TIMEFRAMES
                }
            }
            self.wfile.write(json.dumps(response, indent=2).encode())
            
        elif self.path == '/stocks':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            # Cached stock info'yu dahil et
            stocks_with_info = []
            for symbol in ACTIVE_STOCKS:
                stock_data = {"symbol": symbol}
                if symbol in STOCK_INFO_CACHE:
                    stock_data.update(STOCK_INFO_CACHE[symbol])
                stocks_with_info.append(stock_data)
            
            response = {
                "active_stocks": stocks_with_info,
                "blacklisted_stocks": list(BLACKLISTED_STOCKS),
                "total_monitored": len(ACTIVE_STOCKS),
                "last_discovery": LAST_SCAN_TIME.isoformat() if LAST_SCAN_TIME else None
            }
            self.wfile.write(json.dumps(response, indent=2, default=str).encode())
            
        elif self.path == '/':
            # Basit web dashboard
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            
            html = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>BIST Scanner Dashboard</title>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
                    .container {{ max-width: 1200px; margin: 0 auto; }}
                    .card {{ background: white; padding: 20px; margin: 20px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                    .status {{ display: inline-block; padding: 4px 12px; border-radius: 4px; color: white; font-weight: bold; }}
                    .healthy {{ background: #28a745; }}
                    .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; }}
                    .stat {{ text-align: center; }}
                    .stat-value {{ font-size: 2em; font-weight: bold; color: #007bff; }}
                    .stat-label {{ color: #666; }}
                    pre {{ background: #f8f9fa; padding: 15px; border-radius: 4px; overflow-x: auto; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>🚀 BIST Scanner Dashboard</h1>
                    
                    <div class="card">
                        <h2>Status: <span class="status healthy">HEALTHY</span></h2>
                        <p><strong>Mode:</strong> {SCAN_MODE}</p>
                        <p><strong>Uptime:</strong> {(time.time() - START_TIME)//3600:.0f}h {((time.time() - START_TIME)%3600)//60:.0f}m</p>
                        <p><strong>Last Scan:</strong> {LAST_SCAN_TIME.strftime('%Y-%m-%d %H:%M UTC') if LAST_SCAN_TIME else 'Not started'}</p>
                    </div>
                    
                    <div class="card">
                        <h2>📊 Statistics</h2>
                        <div class="stats">
                            <div class="stat">
                                <div class="stat-value">{len(ACTIVE_STOCKS)}</div>
                                <div class="stat-label">Active Stocks</div>
                            </div>
                            <div class="stat">
                                <div class="stat-value">{len(BLACKLISTED_STOCKS)}</div>
                                <div class="stat-label">Blacklisted</div>
                            </div>
                            <div class="stat">
                                <div class="stat-value">{CHECK_EVERY_MIN}min</div>
                                <div class="stat-label">Check Interval</div>
                            </div>
                            <div class="stat">
                                <div class="stat-value">{len(TIMEFRAMES)}</div>
                                <div class="stat-label">Timeframes</div>
                            </div>
                        </div>
                    </div>
                    
                    <div class="card">
                        <h2>🔗 API Endpoints</h2>
                        <ul>
                            <li><a href="/health">/health</a> - Health check JSON</li>
                            <li><a href="/stocks">/stocks</a> - Active stocks list JSON</li>
                        </ul>
                    </div>
                </div>
                
                <script>
                    // Auto refresh her 30 saniyede
                    setTimeout(() => window.location.reload(), 30000);
                </script>
            </body>
            </html>
            """
            self.wfile.write(html.encode())
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
        logger.info("📱 Telegram not configured, printing:")
        print(text)
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            r = requests.post(url, json=payload, timeout=10)
            if r.status_code == 200:
                logger.debug("✅ Telegram message sent successfully")
                return True
            else:
                logger.warning(f"⚠️ Telegram attempt {attempt + 1} failed: {r.status_code} - {r.text}")
        except Exception as e:
            logger.warning(f"⚠️ Telegram error attempt {attempt + 1}: {e}")
        
        if attempt < max_retries - 1:
            time.sleep(2 ** attempt)  # Exponential backoff
    
    logger.error("❌ All Telegram attempts failed")
    return False

def rma(series: pd.Series, length: int) -> pd.Series:
    """Wilder's smoothing (RMA)"""
    alpha = 1.0 / length
    return series.ewm(alpha=alpha, adjust=False).mean()

def rsi_from_close(close: pd.Series, length: int) -> pd.Series:
    """RSI hesaplama"""
    delta = close.diff()
    up = np.where(delta > 0, delta, 0.0)
    down = np.where(delta < 0, -delta, 0.0)
    up_rma = rma(pd.Series(up, index=close.index), length)
    down_rma = rma(pd.Series(down, index=close.index), length)
    rs = up_rma / down_rma.replace(0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.fillna(50.0)

def ema(series: pd.Series, length: int) -> pd.Series:
    """Exponential Moving Average"""
    return series.ewm(span=length, adjust=False).mean()

def sma(series: pd.Series, length: int) -> pd.Series:
    """Simple Moving Average"""
    return series.rolling(window=length).mean()

def calculate_macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    """MACD hesaplama"""
    ema_fast = ema(close, fast)
    ema_slow = ema(close, slow)
    macd_line = ema_fast - ema_slow
    signal_line = ema(macd_line, signal)
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def calculate_bollinger_bands(close: pd.Series, period: int = 20, mult: float = 2.0):
    """Bollinger Bands hesaplama"""
    middle = sma(close, period)
    std = close.rolling(window=period).std()
    upper = middle + (std * mult)
    lower = middle - (std * mult)
    return upper, middle, lower

def find_pivots(arr: pd.Series, left: int, right: int) -> Tuple[List[int], List[int]]:
    """Pivot noktalarını bul"""
    highs, lows = [], []
    n = len(arr)
    
    for i in range(left, n - right):
        # Local maximum check
        is_high = True
        for j in range(i - left, i + right + 1):
            if j != i and arr.iloc[j] >= arr.iloc[i]:
                is_high = False
                break
        if is_high:
            highs.append(i)
        
        # Local minimum check
        is_low = True
        for j in range(i - left, i + right + 1):
            if j != i and arr.iloc[j] <= arr.iloc[i]:
                is_low = False
                break
        if is_low:
            lows.append(i)
    
    return highs, lows

def trendline_from_pivots(idx1: int, val1: float, idx2: int, val2: float) -> Tuple[float, float]:
    """İki pivot arasından trendline hesapla"""
    if idx2 == idx1:
        return 0.0, val2
    m = (val2 - val1) / (idx2 - idx1)
    b = val2 - m * idx2
    return m, b

def value_on_line(m: float, b: float, x: float) -> float:
    """Trendline üzerindeki değeri hesapla"""
    return m * x + b

def detect_breakouts(rsi: pd.Series, highs_idx: List[int], lows_idx: List[int], now_i: int) -> Tuple[bool, bool, Optional[float]]:
    """RSI trendline kırılımlarını tespit et"""
    bull_break = False
    bear_break = False
    breakout_angle = None
    
    # Bullish breakout (Düşen trend çizgisinin yukarı kırılması)
    if len(highs_idx) >= 2:
        # Son iki yüksek pivot
        i2, i1 = highs_idx[-1], highs_idx[-2]
        v2, v1 = rsi.iloc[i2], rsi.iloc[i1]
        
        # Düşen trendline (resistance)
        if v2 < v1 and i2 > i1:
            m, b = trendline_from_pivots(i1, v1, i2, v2)
            y_now = value_on_line(m, b, now_i)
            
            # RSI trendline'ı yukarı kırdı mı?
            if rsi.iloc[now_i] > y_now + 2:  # 2 birimlik buffer
                bull_break = True
                breakout_angle = math.degrees(math.atan(m))
    
    # Bearish breakout (Yükselen trend çizgisinin aşağı kırılması)
    if len(lows_idx) >= 2:
        # Son iki düşük pivot
        j2, j1 = lows_idx[-1], lows_idx[-2]
        w2, w1 = rsi.iloc[j2], rsi.iloc[j1]
        
        # Yükselen trendline (support)
        if w2 > w1 and j2 > j1:
            m2, b2 = trendline_from_pivots(j1, w1, j2, w2)
            y_now2 = value_on_line(m2, b2, now_i)
            
            # RSI trendline'ı aşağı kırdı mı?
            if rsi.iloc[now_i] < y_now2 - 2:  # 2 birimlik buffer
                bear_break = True
                breakout_angle = math.degrees(math.atan(m2))
    
    return bull_break, bear_break, breakout_angle

def calculate_signal_strength(signal: SignalInfo) -> float:
    """Gelişmiş sinyal gücü hesaplama"""
    score = 5.0
    
    # 1. RSI Seviyesine göre puanlama
    if signal.direction == "BULLISH":
        if signal.rsi < 30:
            score += 3.0  # Oversold'dan çıkış
        elif 30 <= signal.rsi < 50:
            score += 2.0  # Alt bölgeden momentum
        elif 50 <= signal.rsi < 60:
            score += 1.0  # Neutral'dan yukarı
    else:  # BEARISH
        if signal.rsi > 70:
            score += 3.0  # Overbought'tan düşüş
        elif 50 < signal.rsi <= 70:
            score += 2.0  # Üst bölgeden momentum
        elif 40 < signal.rsi <= 50:
            score += 1.0  # Neutral'dan aşağı
            
    # 2. Hacim Puanı (daha detaylı)
    if signal.volume_ratio > 5.0:
        score += 3.0  # Çok yüksek hacim
    elif signal.volume_ratio > 3.0:
        score += 2.5
    elif signal.volume_ratio > 2.0:
        score += 2.0
    elif signal.volume_ratio > 1.5:
        score += 1.0
    else:
        score -= 1.0  # Düşük hacim ceza
    
    # 3. MACD Onayı
    if signal.direction == "BULLISH" and signal.macd_signal == "BULLISH":
        score += 2.0  # Güçlü onay
    elif signal.direction == "BEARISH" and signal.macd_signal == "BEARISH":
        score += 2.0
    elif signal.macd_signal == "NEUTRAL":
        score += 0.5  # Nötr durum
    else:
        score -= 0.5  # Karşıt sinyal ceza
    
    # 4. RSI-EMA Momentum Onayı
    rsi_ema_diff = signal.rsi - signal.rsi_ema
    if signal.direction == "BULLISH" and rsi_ema_diff > 5:
        score += 1.5  # Güçlü momentum
    elif signal.direction == "BULLISH" and rsi_ema_diff > 0:
        score += 1.0
    elif signal.direction == "BEARISH" and rsi_ema_diff < -5:
        score += 1.5
    elif signal.direction == "BEARISH" and rsi_ema_diff < 0:
        score += 1.0
    
    # 5. Bollinger Bands Pozisyon Puanı
    if signal.direction == "BULLISH":
        if signal.bb_position == "NEAR_LOWER":
            score += 1.0  # Alt banddan yukarı hareket
        elif signal.bb_position == "NEAR_UPPER":
            score -= 0.5  # Üst bandda risk
    else:  # BEARISH
        if signal.bb_position == "NEAR_UPPER":
            score += 1.0  # Üst banddan aşağı hareket
        elif signal.bb_position == "NEAR_LOWER":
            score -= 0.5  # Alt bandda risk
    
    # 6. Fiyat Seviyesi ve Likidite Puanı
    if signal.price >= 50:
        score += 1.0  # Yüksek fiyatlı, likit hisseler
    elif signal.price >= 10:
        score += 0.5
    elif signal.price < 2:
        score -= 1.0  # Çok düşük fiyatlı hisseler riski
    
    # 7. Kırılım Açısı Puanı (Geliştirilmiş)
    if signal.breakout_angle is not None:
        angle_abs = abs(signal.breakout_angle)
        if angle_abs > 45:
            score += 3.0  # Çok dik kırılım - güçlü sinyal
        elif angle_abs > 30:
            score += 2.5  # Dik kırılım
        elif angle_abs > 15:
            score += 2.0  # Orta kırılım
        elif angle_abs > 5:
            score += 1.0  # Normal kırılım
        else:
            score += 0.5  # Yavaş kırılım
    
    # 8. 24 saatlik performans bonusu
    if hasattr(signal, 'price_change_24h') and signal.price_change_24h is not None:
        if signal.direction == "BULLISH" and signal.price_change_24h > 0:
            score += 0.5  # Pozitif momentum
        elif signal.direction == "BEARISH" and signal.price_change_24h < 0:
            score += 0.5  # Negatif momentum
    
    return min(10.0, max(0.0, score))

def ensure_is_suffix(ticker: str) -> str:
    """Ticker'a .IS uzantısını ekle"""
    return ticker if ticker.upper().endswith(".IS") else (ticker.upper() + ".IS")

# ----------------------- Data Fetching -----------------------
def fetch_yf_df(symbol: str, timeframe: str) -> pd.DataFrame:
    """Yahoo Finance'dan veri çek"""
    try:
        # .IS uzantısını garanti et
        yf_symbol = ensure_is_suffix(symbol)
        
        # Timeframe'e göre period ve interval ayarla
        if timeframe == "1h":
            period = YF_INTRADAY_PERIOD
            interval = "1h"
        elif timeframe == "4h":
            period = YF_INTRADAY_PERIOD  
            interval = "1h"  # 4h için 1h veriyi resample edeceğiz
        elif timeframe == "1d":
            period = YF_DAILY_PERIOD
            interval = "1d"
        else:
            raise ValueError(f"Desteklenmeyen timeframe: {timeframe}")
        
        # Yahoo Finance'dan veri çek
        yf_ticker = yf.Ticker(yf_symbol)
        hist = yf_ticker.history(
            period=period, 
            interval=interval, 
            timeout=REQUEST_TIMEOUT,
            prepost=False,
            auto_adjust=True,
            back_adjust=False
        )
        
        if hist is None or hist.empty:
            raise RuntimeError(f"No data for {yf_symbol}")
        
        # Sütun isimlerini standardize et
        hist.columns = hist.columns.str.lower()
        
        # Timezone handling
        if hist.index.tz is not None:
            hist.index = hist.index.tz_convert("Europe/Istanbul").tz_localize(None)
        
        # NaN değerleri temizle
        hist = hist.dropna(subset=["close"])
        
        if len(hist) < 50:
            raise RuntimeError(f"Insufficient data: {len(hist)} bars for {yf_symbol}")
        
        # 4h resampling
        if timeframe == "4h":
            hist_4h = hist.resample("4h", label="right", closed="right").agg({
                'open': 'first',
                'high': 'max', 
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna()
            hist = hist_4h
        
        # Memory optimization
        if len(hist) > MAX_DATA_POINTS:
            hist = hist.tail(MAX_DATA_POINTS)
        
        logger.debug(f"✅ Data fetched for {yf_symbol} {timeframe}: {len(hist)} bars")
        return hist
        
    except Exception as e:
        logger.debug(f"❌ Fetch error for {symbol} {timeframe}: {e}")
        raise RuntimeError(f"Fetch error for {symbol}: {e}")

# ----------------------- Analysis -----------------------
def analyze_symbol_timeframe(symbol: str, timeframe: str) -> Optional[SignalInfo]:
    """Tekil hisse analizi"""
    try:
        df = fetch_yf_df(symbol, timeframe)
    except Exception as e:
        # Stock'u blacklist'e ekle
        BLACKLISTED_STOCKS.add(symbol)
        if symbol in ACTIVE_STOCKS:
            ACTIVE_STOCKS.remove(symbol)
        logger.debug(f"❌ {symbol} blacklisted: {e}")
        return None

    try:
        close = df["close"]
        volume = df["volume"]
        high = df["high"]
        low = df["low"]
        
        # Teknik indikatörler
        rsi_series = rsi_from_close(close, RSI_LEN)
        rsi_ema = ema(rsi_series, RSI_EMA_LEN)
        
        macd_line, macd_signal_line, macd_hist = calculate_macd(close, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        bb_upper, bb_middle, bb_lower = calculate_bollinger_bands(close, BB_PERIOD, BB_MULT)
        
        # Hacim analizi (geliştirilmiş)
        volume_sma = sma(volume, 20)
        volume_ema = ema(volume, 10)  # Daha hassas hacim tespiti
        current_volume = volume.iloc[-1]
        avg_volume = volume_sma.iloc[-1]
        avg_volume_ema = volume_ema.iloc[-1]
        
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1.0
        volume_ratio_ema = current_volume / avg_volume_ema if avg_volume_ema > 0 else 1.0

        # Pivot noktalarını bul
        highs_idx, lows_idx = find_pivots(rsi_series, PIVOT_PERIOD, PIVOT_PERIOD)
        now_i = len(rsi_series) - 1

        # Breakout tespiti
        bull_break, bear_break, breakout_angle = detect_breakouts(rsi_series, highs_idx, lows_idx, now_i)

        current_rsi = rsi_series.iloc[now_i]
        current_rsi_ema = rsi_ema.iloc[now_i]
        current_price = close.iloc[-1]

        # MACD sinyali analizi (geliştirilmiş)
        macd_signal_str = "NEUTRAL"
        if len(macd_hist) >= 3:
            current_hist = macd_hist.iloc[-1]
            prev_hist = macd_hist.iloc[-2]
            prev2_hist = macd_hist.iloc[-3]
            
            # Histogram zero line cross
            if current_hist > 0 and prev_hist <= 0:
                macd_signal_str = "BULLISH"
            elif current_hist < 0 and prev_hist >= 0:
                macd_signal_str = "BEARISH"
            # Histogram momentum
            elif current_hist > prev_hist > prev2_hist and current_hist > 0:
                macd_signal_str = "BULLISH"
            elif current_hist < prev_hist < prev2_hist and current_hist < 0:
                macd_signal_str = "BEARISH"

        # Gelişmiş sinyal mantığı
        is_bull_signal = False
        is_bear_signal = False
        
        # Bullish koşullar
        if bull_break:
            # RSI trendline kırılımı + ek koşullar
            if (current_rsi > 25 and  # Çok düşük RSI'dan kaçın
                volume_ratio >= MIN_VOLUME_RATIO and
                current_rsi > current_rsi_ema):  # RSI EMA üstünde
                is_bull_signal = True
        
        # MACD ile ek bullish sinyaller
        elif (macd_signal_str == "BULLISH" and 
              current_rsi > 35 and current_rsi < 75 and
              volume_ratio >= MIN_VOLUME_RATIO and
              current_rsi > current_rsi_ema):
            is_bull_signal = True
        
        # Bearish koşullar  
        if bear_break:
            # RSI trendline kırılımı + ek koşullar
            if (current_rsi < 75 and  # Çok yüksek RSI'dan kaçın
                volume_ratio >= MIN_VOLUME_RATIO and
                current_rsi < current_rsi_ema):  # RSI EMA altında
                is_bear_signal = True
        
        # MACD ile ek bearish sinyaller
        elif (macd_signal_str == "BEARISH" and 
              current_rsi > 25 and current_rsi < 65 and
              volume_ratio >= MIN_VOLUME_RATIO and
              current_rsi < current_rsi_ema):
            is_bear_signal = True

        if not (is_bull_signal or is_bear_signal):
            return None

        # Bollinger Bands pozisyon analizi
        bb_position = "MIDDLE"
        bb_range = bb_upper.iloc[-1] - bb_lower.iloc[-1]
        
        if current_price > bb_upper.iloc[-1] - (bb_range * 0.15):
            bb_position = "NEAR_UPPER"
        elif current_price < bb_lower.iloc[-1] + (bb_range * 0.15):
            bb_position = "NEAR_LOWER"
        elif current_price > bb_middle.iloc[-1]:
            bb_position = "UPPER_HALF"
        else:
            bb_position = "LOWER_HALF"

        direction = "BULLISH" if is_bull_signal else "BEARISH"
        
        # Hacim TL cinsinden
        volume_try = current_price * current_volume
        
        # 24 saatlik değişim (cache'den al)
        price_change_24h = None
        if symbol in STOCK_INFO_CACHE:
            price_change_24h = STOCK_INFO_CACHE[symbol].get('price_change_24h', 0.0)
        
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
            timestamp=df.index[-1].strftime('%Y-%m-%d %H:%M'),
            breakout_angle=breakout_angle,
            price_change_24h=price_change_24h
        )
        
        signal.strength_score = calculate_signal_strength(signal)
        
        # Minimum güç skoru filtresi
        if signal.strength_score < 6.0:
            return None
            
        return signal
        
    except Exception as e:
        logger.error(f"❌ Analysis error {symbol} {timeframe}: {e}")
        return None

def send_enhanced_alert(signal: SignalInfo):
    """Gelişmiş Telegram uyarısı gönder"""
    # Cooldown kontrolü
    side_key = "LONG" if signal.direction == "BULLISH" else "SHORT"
    key = (signal.symbol, signal.timeframe, side_key)
    last_ts = LAST_ALERT.get(key, 0.0)
    now_ts = time.time()
    
    if now_ts - last_ts < ALERT_COOLDOWN_MIN * 60:
        logger.debug(f"🔇 Cooldown aktif: {signal.symbol} {signal.timeframe}")
        return
        
    LAST_ALERT[key] = now_ts

    # Mesaj formatı
    if signal.direction == "BULLISH":
        emoji = "🟢📈"
        direction_text = "YUKARI KIRILIM"
        action_text = "🎯 LONG POZİSYON ÖNERİSİ"
    else:
        emoji = "🔴📉"
        direction_text = "AŞAĞI KIRILIM"
        action_text = "🎯 SHORT POZİSYON ÖNERİSİ"

    # Güç seviyesi göstergesi
    strength_stars = "⭐" * min(5, int(signal.strength_score / 2))
    if signal.strength_score >= 9.0:
        strength_text = "🔥 ÇOK GÜÇLÜ"
    elif signal.strength_score >= 8.0:
        strength_text = "💪 GÜÇLÜ"
    elif signal.strength_score >= 7.0:
        strength_text = "👍 İYİ"
    else:
        strength_text = "⚠️ ORTA"
    
    # Hacim göstergesi
    if signal.volume_ratio > 5.0:
        volume_emoji = "🚀🚀"
    elif signal.volume_ratio > 3.0:
        volume_emoji = "🚀"
    elif signal.volume_ratio > 2.0:
        volume_emoji = "🔥"
    else:
        volume_emoji = "📊"

    # Hacim formatı
    if signal.volume_try >= 1000000:
        volume_str = f"{signal.volume_try/1000000:.1f}M TL"
    elif signal.volume_try >= 1000:
        volume_str = f"{signal.volume_try/1000:.0f}K TL"
    else:
        volume_str = f"{signal.volume_try:.0f} TL"

    # Kırılım açısı formatı
    angle_str = ""
    if signal.breakout_angle is not None:
        angle_abs = abs(signal.breakout_angle)
        if angle_abs > 30:
            angle_str = f"📐 DİK KIRILIM ({signal.breakout_angle:.1f}°)"
        else:
            angle_str = f"📐 Açı: {signal.breakout_angle:.1f}°"
    
    # 24h değişim
    change_24h_str = ""
    if signal.price_change_24h is not None:
        change_emoji = "📈" if signal.price_change_24h > 0 else "📉"
        change_24h_str = f"{change_emoji} 24h: %{signal.price_change_24h:+.1f}"
    
    msg = f"""<b>{emoji} RSI TRENDLINE BREAKOUT</b>

📊 <b>Hisse:</b> {signal.symbol}
⏰ <b>Zaman Dilimi:</b> {signal.timeframe}
{action_text}
💰 <b>Fiyat:</b> ₺{signal.price:.2f} {change_24h_str}

📈 <b>Teknik Analiz:</b>
• RSI({RSI_LEN}): {signal.rsi:.1f}
• RSI EMA({RSI_EMA_LEN}): {signal.rsi_ema:.1f}
• MACD Durum: {signal.macd_signal}
• BB Pozisyon: {signal.bb_position}
{angle_str}

{volume_emoji} <b>Hacim:</b> {signal.volume_ratio:.1f}x ({volume_str})
⚡ <b>Sinyal Gücü:</b> {signal.strength_score:.1f}/10 {strength_stars}
🏆 <b>Seviye:</b> {strength_text}

🕐 <b>Zaman:</b> {signal.timestamp}
🔍 <b>Tarama:</b> {SCAN_MODE} ({len(ACTIVE_STOCKS)} aktif hisse)

#BIST #{signal.symbol.replace('.IS', '')} #{signal.timeframe} #{signal.direction} #{SCAN_MODE}"""

    success = send_telegram(msg)
    if success:
        logger.info(f"✅ Alert sent: {signal.symbol} {signal.timeframe} {signal.direction} (Güç: {signal.strength_score:.1f})")
    else:
        logger.error(f"❌ Alert failed: {signal.symbol} {signal.timeframe}")

def parallel_scan_stocks(stocks: List[str], timeframe: str) -> List[SignalInfo]:
    """Paralel hisse tarama"""
    logger.info(f"🔄 {timeframe} için {len(stocks)} hisse paralel taranıyor...")
    
    # Render'da CPU limiti olduğu için worker sayısını kısıtla
    num_workers = min(MAX_WORKERS, len(stocks), 6)
    
    signals = []
    completed = 0
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(analyze_symbol_timeframe, s, timeframe): s for s in stocks}
        
        for future in as_completed(futures):
            symbol = futures[future]
            completed += 1
            
            try:
                signal = future.result()
                if signal:
                    signals.append(signal)
                    logger.info(f"🎯 Signal: {signal.symbol} {signal.timeframe} {signal.direction} (Güç: {signal.strength_score:.1f})")
                
                # Progress log
                if completed % 10 == 0:
                    logger.info(f"📊 Progress: {completed}/{len(stocks)} completed")
                    
            except Exception as e:
                logger.error(f"❌ Future error for {symbol}: {e}")
    
    logger.info(f"✅ {timeframe} tarama tamamlandı: {len(signals)} sinyal bulundu")
    return signals

def run_scan():
    """Ana tarama fonksiyonu"""
    global LAST_SCAN_TIME, ACTIVE_STOCKS
    
    scan_start = time.time()
    logger.info("="*60)
    logger.info("🚀 BIST Full Scanner başlatıldı - %s modu", SCAN_MODE)
    
    # Aktif hisseleri keşfet (her 4. taramada bir)
    if not ACTIVE_STOCKS or len(ACTIVE_STOCKS) < 10:
        logger.info("🔍 Aktif hisse keşfi başlatılıyor...")
        ACTIVE_STOCKS = discover_active_stocks()

    if not ACTIVE_STOCKS:
        logger.warning("🚫 Taranacak aktif hisse bulunamadı.")
        LAST_SCAN_TIME = datetime.now(timezone.utc)
        return
        
    logger.info(f"📈 {len(ACTIVE_STOCKS)} aktif hisse taranacak")
    
    all_signals = []
    
    # Her timeframe için tarama
    for tf_idx, timeframe in enumerate(TIMEFRAMES):
        logger.info(f"⏳ [{tf_idx+1}/{len(TIMEFRAMES)}] {timeframe} zaman dilimi taranıyor...")
        
        try:
            signals = parallel_scan_stocks(list(ACTIVE_STOCKS), timeframe)
            all_signals.extend(signals)
        except Exception as e:
            logger.error(f"❌ Timeframe {timeframe} tarama hatası: {e}")
        
        # API rate limit için bekleme
        if tf_idx < len(TIMEFRAMES) - 1:
            time.sleep(3)

    # Sinyalleri güce göre sırala ve gönder
    if all_signals:
        all_signals.sort(key=lambda s: s.strength_score, reverse=True)
        
        logger.info(f"🎯 Toplam {len(all_signals)} sinyal bulundu:")
        
        # En güçlü 20 sinyali gönder (spam'i önlemek için)
        top_signals = all_signals[:20]
        
        for i, signal in enumerate(top_signals):
            try:
                send_enhanced_alert(signal)
                # Rate limiting için bekleme
                if i < len(top_signals) - 1:
                    time.sleep(1)
            except Exception as e:
                logger.error(f"❌ Alert sending error for {signal.symbol}: {e}")
        
        # Özet mesajı
        if len(all_signals) > 20:
            summary_msg = f"""
📊 <b>TARAMA ÖZETİ</b>

🔍 <b>Taranan:</b> {len(ACTIVE_STOCKS)} hisse
🎯 <b>Toplam Sinyal:</b> {len(all_signals)}
⭐ <b>Gönderilen:</b> {len(top_signals)} (en güçlü)
⏱️ <b>Süre:</b> {time.time() - scan_start:.1f}s

<b>Güç Dağılımı:</b>
• 9.0+ Güç: {len([s for s in all_signals if s.strength_score >= 9.0])}
• 8.0-8.9: {len([s for s in all_signals if 8.0 <= s.strength_score < 9.0])}
• 7.0-7.9: {len([s for s in all_signals if 7.0 <= s.strength_score < 8.0])}
• 6.0-6.9: {len([s for s in all_signals if 6.0 <= s.strength_score < 7.0])}

#BIST #TaramaÖzeti #{SCAN_MODE}"""
            send_telegram(summary_msg)
    else:
        logger.info("🤷 Hiçbir zaman diliminde sinyal bulunamadı.")
        
        # Sessizlik bildirgesi (isteğe bağlı)
        if datetime.now().hour in [9, 12, 15, 18]:  # Günde 4 kez sessizlik raporu
            silence_msg = f"""
🔇 <b>Sessizlik Raporu</b>

📊 {len(ACTIVE_STOCKS)} aktif hisse tarandı
⏰ Zaman dilimleri: {', '.join(TIMEFRAMES)}
🎯 Sinyal bulunamadı

<i>Piyasa sakin veya koşullar uygun değil.</i>
<i>Sonraki tarama: {CHECK_EVERY_MIN} dakika sonra</i>

#BIST #Sessizlik #{SCAN_MODE}"""
            send_telegram(silence_msg)
    
    # Cleanup
    LAST_SCAN_TIME = datetime.now(timezone.utc)
    scan_duration = time.time() - scan_start
    
    logger.info(f"✅ Tarama tamamlandı - Süre: {scan_duration:.1f}s")
    logger.info(f"⏳ Sonraki tarama: {CHECK_EVERY_MIN} dakika sonra")
    logger.info("="*60)
    
    # Memory cleanup
    gc.collect()

def periodic_cleanup():
    """Periyodik temizlik"""
    global BLACKLISTED_STOCKS, LAST_ALERT
    
    current_time = time.time()
    
    # Eski alert kayıtlarını temizle (24 saat öncesi)
    old_alerts = {k: v for k, v in LAST_ALERT.items() 
                  if current_time - v > 24 * 3600}
    for key in old_alerts:
        del LAST_ALERT[key]
    
    # Blacklist'i temizle (12 saat sonra tekrar dene)
    if len(BLACKLISTED_STOCKS) > 50:
        BLACKLISTED_STOCKS.clear()
        logger.info("🧹 Blacklist temizlendi")
    
    # Cache temizliği
    if len(STOCK_INFO_CACHE) > 200:
        STOCK_INFO_CACHE.clear()
        logger.info("🧹 Stock info cache temizlendi")
    
    logger.info(f"🧹 Cleanup completed - Alerts: {len(LAST_ALERT)}, Blacklist: {len(BLACKLISTED_STOCKS)}")

def rediscover_stocks():
    """Aktif hisseleri yeniden keşfet"""
    global ACTIVE_STOCKS
    logger.info("🔄 Aktif hisse listesi yenileniyor...")
    ACTIVE_STOCKS = discover_active_stocks()

def main():
    """Ana fonksiyon"""
    logger.info("🚀 Full BIST Scanner v2.0 başlatılıyor...")
    logger.info("📊 Tarama modu: %s", SCAN_MODE)
    logger.info("🎯 Hedef hisse sayısı: %d", len(TICKERS))
    logger.info("⚙️ Max worker: %d", MAX_WORKERS)
    logger.info("💾 Max data points: %d", MAX_DATA_POINTS)
    logger.info("🌐 Health server port: %d", RENDER_PORT)
    logger.info("📱 Telegram configured: %s", "YES" if TELEGRAM_BOT_TOKEN else "NO")

    # Health server'ı başlat
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()
    
    # Scheduler'ı kur
    scheduler = BlockingScheduler(timezone=timezone.utc)
    
    # Ana tarama job'u
    scheduler.add_job(
        run_scan, 
        'interval', 
        minutes=CHECK_EVERY_MIN, 
        misfire_grace_time=300,
        id='main_scan'
    )
    
    # Temizlik job'u (her 2 saatte bir)
    scheduler.add_job(
        periodic_cleanup,
        'interval',
        hours=2,
        id='cleanup'
    )
    
    # Hisse keşfi job'u (her 6 saatte bir)
    scheduler.add_job(
        rediscover_stocks,
        'interval', 
        hours=6,
        id='rediscover'
    )
    
    # Başlangıç mesajı
    startup_msg = f"""
🚀 <b>BIST Scanner Başlatıldı!</b>

⚙️ <b>Konfigürasyon:</b>
• Mod: {SCAN_MODE}
• Hisse Sayısı: {len(TICKERS)}
• Zaman Dilimleri: {', '.join(TIMEFRAMES)}
• Tarama Sıklığı: {CHECK_EVERY_MIN} dakika
• Min Hacim: {MIN_VOLUME_TRY/1000000:.1f}M TL
• Min Fiyat: ₺{MIN_PRICE} - ₺{MAX_PRICE}

🎯 <b>İlk tarama başlatılıyor...</b>

#BIST #Scanner #Başlatıldı #{SCAN_MODE}"""
    
    send_telegram(startup_msg)
    
    # İlk çalıştırma
    try:
        logger.info("🎬 İlk tarama başlatılıyor...")
        run_scan()
    except Exception as e:
        logger.error(f"❌ İlk tarama hatası: {e}")
    
    # Scheduler'ı başlat
    try:
        logger.info("⏰ Scheduler başlatılıyor...")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("🛑 Scheduler durduruluyor...")
        
        # Kapanış mesajı
        shutdown_msg = f"""
🛑 <b>BIST Scanner Durduruldu</b>

📊 <b>İstatistikler:</b>
• Çalışma Süresi: {(time.time() - START_TIME)//3600:.0f}h {((time.time() - START_TIME)%3600)//60:.0f}m
• Son Tarama: {LAST_SCAN_TIME.strftime('%H:%M') if LAST_SCAN_TIME else 'Yok'}
• Aktif Hisse: {len(ACTIVE_STOCKS)}
• Blacklist: {len(BLACKLISTED_STOCKS)}

#BIST #Scanner #Durduruldu"""
        
        send_telegram(shutdown_msg)
    except Exception as e:
        logger.error(f"❌ Scheduler error: {e}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"💥 Critical error: {e}")
        
        error_msg = f"""
💥 <b>KRITIK HATA!</b>

❌ <b>Hata:</b> {str(e)[:200]}
🕐 <b>Zaman:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
🔄 <b>Yeniden başlatma gerekiyor</b>

#BIST #Error #Critical"""
        
        send_telegram(error_msg)
        raise
