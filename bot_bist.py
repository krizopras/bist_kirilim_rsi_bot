#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TÜM BIST Hisselerini Paralel Tarayan Çoklu Zaman Dilimi ve Mum Formasyonu Analiz Botu
- Borsa saatleri: 09:00-18:30 (İstanbul Saati) arası çalışır
- 15 dakikada bir tarama yapar
- Günde aynı hisse için aynı yönde sadece 1 sinyal gönderir
- Gün sonu 18:30'da detaylı performans raporu sunar
- TÜM BIST hisselerini tarar (500+ hisse)
- Sinyal filtreleme ve puanlama mantığı geliştirildi.
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
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import pandas as pd
import math
import yfinance as yf
import http.client
import pytz
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from dotenv import load_dotenv
import investpy

# Yerel test için .env dosyasını yükler. Render'da bu satır bir şey yapmaz.
load_dotenv()

# --- 1. AYARLAR VE GÜVENLİK ---
# Ortam değişkenlerinden çekilir.
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
COLLECTAPI_KEY = os.getenv("COLLECTAPI_KEY")

if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, COLLECTAPI_KEY]):
    raise ValueError("Ortam değişkenleri (TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, COLLECTAPI_KEY) ayarlanmalı!")

# Diğer Ayarlar
LOG_FILE = os.getenv('LOG_FILE', 'trading_bot.log')
DATA_CACHE_DIR = "data_cache"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
HEALTH_CHECK_PORT = int(os.getenv("PORT", 8080))
IST_TZ = pytz.timezone('Europe/Istanbul')

# Loglama ayarları
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
logger = logging.getLogger("bist_scanner")

# ----------------------- BORSA VE TARAMA AYARLARI -----------------------
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

# --- Hisse Listeleri (Kullanıcının Verdiği Eksiksiz Liste) ---
ALL_BIST_STOCKS = [
    # BIST 30
    "THYAO","SAHOL","ASTOR","AKBNK","SISE","BIMAS","EREGL","KCHOL","PETKM","VAKBN",
    "TCELL","TUPRS","TTKOM","PGSUS","DOHOL","KOZAA","MGROS","ARCLK","VESTL","KRDMD",
    "FROTO","HALKB","ISCTR","GARAN","AKSA","ALARK","AYGAZ","CCOLA","EKGYO","GUBRF",
    
    # BIST 50 ve diğer büyük şirketler
    "SODA","TOASO","OTKAR","ULKER","TKFEN","ENKAI","IZMDC","KARSN","KONTR","LOGO",
    "MAVI","NETAS","ODAS","PAPIL","PENGN","POLHO","PRKAB","REEDR","SELEC","SKBNK",
    "SOKM","TAVHL","TBORG","TCELL","TEZOL","TMSN","TRILC","YKBNK","ZOREN","ADEL",
    
    # Diğer BIST 100 hisseleri
    "AEFES","AFYON","AGESA","AGHOL","AGROT","AHGAZ","AHRTM","AKENR","AKFGY","AKFYE",
    "AKGRT","AKSEN","AKSGY","AKSUE","ALBRK","ALCAR","ALCTL","ALFAS","ALGYO","ALKA",
    "ALMAD","ALTIN","ALYAG","ANACM","ANELE","ANHYT","ANSGR","ARASE","ARSAN","ARZUM",
    "ASELS","ASUZU","ATAGY","ATAKP","ATATP","ATEKS","ATLAS","ATSYH","AVGYO","AVHOL",
    "AVISA","AVTUR","AYCES","AYEN","BAGFS","BAHKM","BAKAB","BALAT","BANVT","BASCM",
    "BASGZ","BATI","BAYRK","BEGYO","BFREN","BIGCH","BIMAS","BINBN","BIOEN","BIZIM",
    "BJKAS","BLCYT","BMSCH","BMSTL","BNTAS","BOBET","BOSSA","BRISA","BRKSN","BRKVY",
    "BRYAT","BSOKE","BTCIM","BUCIM","BURCE","BURVA","CANTE","CASA","CATES","CCOLA",
    "CELHA","CEMTS","CEOEM","CER","CIMSA","CLEBI","CMBTN","CMENT","CML","CONSE",
    "COSMO","CRDFA","CRFSA","CWENE","DAGI","DAGHL","DARDL","DENGE","DERHL","DERIM",
    "DESPC","DEVA","DGATE","DGGYO","DGNMO","DIRIT","DOBUR","DOCO","DOGUB","DOHOL",
    "DOKTA","DURDO","DYOBY","DZGYO","ECILC","EDATA","EDIP","EFORC","EGPRO","EIMVT",
    "EKGYO","EKOS","EKSUN","ELITE","EMKEL","EMNIS","ENERY","ENJSA","ENKAI","EPLAS",
    "ERBOS","ERCB","EREGL","ERSU","ESCAR","ESCOM","ESEN","ETILR","ETYAT","EUHOL",
    "EUKYO","EUREN","EUYO","EYGYO","FADE","FENER","FLAP","FMIZP","FONET","FORMT",
    "FORTE","FRIGO","FROTO","FZLGY","GARAN","GARFA","GEDIK","GEDZA","GENEL","GENTS",
    "GEREL","GESAN","GIPTA","GLBMD","GLCVY","GLRYH","GLYHO","GMTAS","GOKNR","GOLTS",
    "GOODY","GOZDE","GRNYO","GRSEL","GRTRK","GSDDE","GSDHO","GSRAY","GUBRF","GWIND",
    "GZNMI","HALKB","HATSN","HDFGS","HEDEF","HEKTS","HKTM","HLGYO","HTTBT","HURGZ",
    "HUNER","HZNP","ICBCT","ICUGS","IDEAS","IDGYO","IEYHO","IHAAS","IHLAS","IHYAY",
    "IMASM","INDES","INFO","INTEM","INVEO","IPEKE","ISATR","ISBIR","ISBTR","ISCTR",
    "ISKUR","ISMEN","ISS","ISYAT","IZENR","IZFAS","IZGYO","IZINV","IZMDC","JANTS",
    "KAPLM","KAPOL","KARSN","KARTN","KATMR","KAYSE","KCAER","KCHOL","KENT","KERVT",
    "KFEIN","KGYO","KIMMR","KLMSN","KLRHO","KLSER","KLSYN","KMPUR","KNFRT","KONKA",
    "KONTR","KONYA","KOPOL","KORDS","KOZAA","KOZAL","KRDMA","KRDMB","KRDMD","KRPLS",
    "KRSTL","KRTEK","KRVGD","KSTUR","KUTPO","KUVVA","KUYAS","KZBGY","LIDER","LIDFA",
    "LINK","LKMNH","LMKDC","LOGO","LUKSK","LYDHO","MAALT","MACKO","MAGEN","MAKIM",
    "MAKTK","MARBL","MARKA","MARTI","MAVI","MEDTR","MEGAP","MEPET","MERCN","MERIT",
    "METRO","MGROS","MHRGY","MIATK","MIGRS","MKPOL","MNDRS","MOBTL","MODERN","MPARK",
    "MRGYO","MRSHL","MSGYO","MTRKS","MTRYO","MZHLD","NATEN","NETAS","NIBAS","NTGAZ",
    "NTHOL","NUGYO","NUHCM","OBAMS","OBASE","ODAS","ODINE","OKANT","OLMIP","ONCSM",
    "ORCAY","ORGE","ORHB","OSTIM","OTKAR","OTTO","OYAKC","OYAYO","OYLUM","OZBAL",
    "OZEN","OZGYO","OZKGY","OZRDN","OZSUB","PAGEN","PAMEL","PAPIL","PARSN","PASEU",
    "PATEK","PCILT","PENGN","PENTA","PETKM","PETUN","PGSUS","PINSU","PKART","PKENT",
    "PLTUR","PNSUT","POLHO","POLTK","PRTAS","PRZMA","PSDTC","PTOFS","QNBFB","QNBFL",
    "QUAGR","RADIŞ","RALYH","RAYSG","REEDR","RGYAS","RODRG","ROYAL","RTALB","RUBNS",
    "RYSAS","SAFKR","SAHOL","SAMAT","SANEL","SANFM","SANKO","SARKY","SASA","SAYAS",
    "SDTTR","SEKFK","SELEC","SELGD","SELVA","SEYKM","SILVR","SISE","SKBNK","SKPLC",
    "SKYMD","SMART","SMRTG","SNGYO","SNKRN","SNPAM","SODSN","SOKM","SONME","SRVGY",
    "SUMAS","SUNTK","SUWEN","TABGD","TARKM","TATGD","TAVHL","TBORG","TCELL","TDGYO",
    "TEKTU","TEMP","TETMT","TEZOL","THYAO","TIRE","TKFEN","TKNSA","TLMAN","TMPOL",
    "TMSN","TNZTP","TOASO","TSGYO","TSKB","TTKOM","TTRAK","TUCLK","TUKAS","TUPRS",
    "TUREX","TURSG","UFUK","ULAS","ULUSE","ULUUN","UNLU","UNYEC","USAK","UZERB",
    "VAKBN","VAKFN","VANGD","VBTYZ","VERUS","VESBE","VESTL","VGYO","VKGYO","VKING",
    "VRGYO","YAPRK","YATAS","YAYLA","YBTAS","YEOTK","YESIL","YGGYO","YGYO","YIGIT",
    "YKBNK","YUNSA","YYLGD","ZEDUR","ZOREN","ZRGYO"
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
RSI_LEN = int(os.getenv("RSI_LEN", "22"))
RSI_EMA_LEN = int(os.getenv("RSI_EMA", "66"))
PIVOT_PERIOD = int(os.getenv("PIVOT_PERIOD", "10"))
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
MIN_SIGNAL_SCORE = float(os.getenv("MIN_SIGNAL_SCORE", "10.0")) 

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

# --- Sağlık Kontrolü ---
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

# --- Utility Functions ---
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
                    if r.status == 429:
                        try:
                            retry_after = r.headers.get("retry-after", 10)
                            logger.info(f"Telegram 429 hatası aldı, {retry_after} saniye bekleniyor...")
                            await asyncio.sleep(int(retry_after) + 1)
                        except (ValueError, TypeError):
                            logger.info("retry-after değeri okunamadı, 60 saniye bekleniyor...")
                            await asyncio.sleep(60)
    except Exception as e:
        logger.warning(f"Telegram error: {e}")

async def send_daily_report():
    """Günlük sinyal raporunu gönderir."""
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
        report_msg += f"• Zaman: {dt.datetime.fromisoformat(signal['sent_time']).strftime('%H:%M')}"
        
    await send_telegram(report_msg)
    logger.info(f"📊 Günlük rapor gönderildi: {len(DAILY_SIGNALS)} sinyal")

# --- TA helpers ---
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

# --- Yeni Mum Formasyonu Fonksiyonları ---
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
        last_c['close'] < last_c['open']
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
        c1['close'] < c1['open'] and c1['open'] - c1['close'] > abs(c1['high'] - c1['low']) * 0.6 and # Long red
        abs(c2['open'] - c2['close']) < abs(c2['high'] - c2['low']) * 0.4 and # Small body
        c3['close'] > c3['open'] and c3['close'] > (c1['open'] + c1['close']) / 2 # Long green, closes above midpoint of first candle
    )

def is_evening_star(df):
    if len(df) < 3: return False
    c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    return (
        c1['close'] > c1['open'] and c1['close'] - c1['open'] > abs(c1['high'] - c1['low']) * 0.6 and # Long green
        abs(c2['open'] - c2['close']) < abs(c2['high'] - c2['low']) * 0.4 and # Small body
        c3['close'] < c3['open'] and c3['close'] < (c1['open'] + c1['close']) / 2 # Long red, closes below midpoint of first candle
    )

def detect_candle_formation(df: pd.DataFrame) -> Optional[str]:
    if len(df) < 3:
        return None
    last_candle = df.iloc[-1]
    prev_candle = df.iloc[-2]
    open_p, high_p, low_p, close_p = last_candle['open'], last_candle['high'], last_candle['low'], last_candle['close']

    # 3 Mum Formasyonları (Önce kontrol edilir, çünkü daha güçlü sinyallerdir)
    if is_three_white_soldiers(df): return "Three White Soldiers"
    if is_three_black_crows(df): return "Three Black Crows"
    if is_morning_star(df): return "Morning Star"
    if is_evening_star(df): return "Evening Star"
    
    # 2 Mum Formasyonları
    if is_bullish_engulfing(prev_candle, last_candle): return "Bullish Engulfing"
    if is_bearish_engulfing(prev_candle, last_candle): return "Bearish Engulfing"
    if is_piercing_pattern(df): return "Piercing Pattern"
    if is_dark_cloud_cover(df): return "Dark Cloud Cover"
    if is_bullish_harami(prev_candle, last_candle): return "Bullish Harami"
    if is_bearish_harami(prev_candle, last_candle): return "Bearish Harami"
    if is_tweezer_bottoms(df): return "Tweezer Bottoms"
    if is_tweezer_tops(df): return "Tweezer Tops"

    # Tek Mum Formasyonları
    # Bu formasyonlar sadece belirli trendlerde anlamlı olduğu için, trend yönü ile birlikte yorumlanır.
    # Bu mantık aşağıda, ana analiz kısmında uygulanacak.
    # Burada sadece formasyonu tespit ediyoruz.
    if is_doji(open_p, close_p, high_p, low_p): return "Doji"
    if is_hammer(open_p, close_p, low_p, high_p): return "Hammer"
    if is_inverted_hammer(open_p, close_p, low_p, high_p): return "Inverted Hammer"
    if is_hanging_man(open_p, close_p, low_p, high_p): return "Hanging Man"
    if is_shooting_star(open_p, close_p, low_p, high_p): return "Shooting Star"

    return None

def calculate_tsi(close: pd.Series, long_period: int = 25, short_period: int = 13, signal_period: int = 13) -> pd.Series:
    diff = close.diff()
    abs_diff = abs(diff)
    
    # EMA'lar
    ema_diff_long = ema(diff, long_period)
    ema_abs_diff_long = ema(abs_diff, long_period)
    ema_diff_short = ema(ema_diff_long, short_period)
    ema_abs_diff_short = ema(ema_abs_diff_long, short_period)
    
    # TSI değeri
    tsi = 100 * (ema_diff_short / ema_abs_diff_short)
    return tsi

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
        else: # BEARISH
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
    
    # Yeni Eklenen Zaman Dilimi Ağırlıklandırması
    if signal.timeframe == '15m':
        score += 2.5
    elif signal.timeframe == '1h':
        score += 1.5
    elif signal.timeframe == '4h':
        score += 0.5
    
    # RSI puanlaması
    if signal.direction == "BULLISH":
        if signal.rsi < 30: score += 3.0
        elif signal.rsi < 50: score += 1.0
    else:
        if signal.rsi > 70: score += 3.0
        elif signal.rsi > 50: score += 1.0
        
    # Hacim puanlaması (Ağırlık Artırıldı)
    if signal.volume_ratio > 4.0: score += 4.0 
    elif signal.volume_ratio > 3.0: score += 3.0
    elif signal.volume_ratio > 2.0: score += 2.0
    elif signal.volume_ratio > 1.5: score += 1.0
    
    # MACD ve RSI Uyumu (Ağırlık Artırıldı)
    if (signal.direction == "BULLISH" and signal.macd_signal == "BULLISH") or \
       (signal.direction == "BEARISH" and signal.macd_signal == "BEARISH"):
        score += 2.5
        
    # RSI EMA puanlaması
    if (signal.direction == "BULLISH" and signal.rsi > signal.rsi_ema) or \
       (signal.direction == "BEARISH" and signal.rsi < signal.rsi_ema):
        score += 1.0
        
    # Bollinger Bandı pozisyonu
    if signal.bb_position in ("NEAR_LOWER", "LOWER") and signal.direction == "BULLISH":
        score += 0.5
    if signal.bb_position in ("NEAR_UPPER", "UPPER") and signal.direction == "BEARISH":
        score += 0.5
        
    # Fiyat puanlaması
    if signal.price > 10: score += 0.5
    
    # Kırılım açısı puanlaması
    if signal.breakout_angle is not None:
        angle_abs = abs(signal.breakout_angle)
        if angle_abs > 30: score += 3.0
        elif angle_abs > 15: score += 2.0
        elif angle_abs > 5: score += 1.0
        
    # Mum formasyonu puanlaması (YENİ)
    if signal.candle_formation:
        if signal.candle_formation in ["Morning Star", "Three White Soldiers", "Bullish Engulfing"]:
            score += 4.0
        elif signal.candle_formation in ["Hammer", "Piercing Pattern", "Bullish Harami", "Tweezer Bottoms"]:
            score += 3.0
        elif signal.candle_formation in ["Inverted Hammer"]:
            score += 2.0
        
        # Ayı Formasyonları
        if signal.candle_formation in ["Evening Star", "Three Black Crows", "Bearish Engulfing"]:
            score += 4.0
        elif signal.candle_formation in ["Shooting Star", "Dark Cloud Cover", "Bearish Harami", "Tweezer Tops"]:
            score += 3.0
        elif signal.candle_formation in ["Hanging Man"]:
            score += 2.0
            
    # StochRSI puanlaması (Ağırlık Artırıldı)
    if signal.stochrsi_k is not None and signal.stochrsi_d is not None:
        if signal.direction == "BULLISH" and signal.stochrsi_k > signal.stochrsi_d and signal.stochrsi_k < 20:
            score += 2.0
        elif signal.direction == "BEARISH" and signal.stochrsi_k < signal.stochrsi_d and signal.stochrsi_k > 80:
            score += 2.0
            
    # Hareketli Ortalama Kesişimi (Ağırlık Artırıldı)
    if signal.ma_cross:
        if signal.direction == "BULLISH" and signal.ma_cross == "GOLDEN_CROSS":
            score += 4.0
        elif signal.direction == "BEARISH" and signal.ma_cross == "DEATH_CROSS":
            score += 4.0
    
    # TSI Uyumu
    if signal.tsi_value is not None:
        if signal.direction == "BULLISH" and signal.tsi_value > 0:
            score += 2.0
        elif signal.direction == "BEARISH" and signal.tsi_value < 0:
            score += 2.0

    # SAR Uyumu
    if signal.sar_status == 'BULLISH' and signal.direction == 'BULLISH':
        score += 2.5
    elif signal.sar_status == 'BEARISH' and signal.direction == 'BEARISH':
        score += 2.5
        
    return float(max(0.0, min(10.0, score)))

# --- Veri Çekme & Analiz ---
def _resample_ohlcv(df: pd.DataFrame, rule: str) -> pd.DataFrame:
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
            df = _resample_ohlcv(raw, '4h')
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
        
        # SIKI FİLTRELER: Fiyat ve hacim kontrolü
        if last_close < MIN_PRICE or last_close * last_vol < MIN_VOLUME_TRY:
            return None

        close, volume = df["close"], df["volume"]
        rsi_series = rsi_from_close(close, RSI_LEN)
        rsi_ema = ema(rsi_series, RSI_EMA_LEN)
        macd_line, macd_sig, macd_hist = calculate_macd(close, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        bb_upper, bb_middle, bb_lower = calculate_bollinger_bands(close, BB_PERIOD, BB_MULT)
        
        # TSI ve SAR hesaplamaları
        tsi_series = calculate_tsi(close)
        sar_series = calculate_sar(df['high'], df['low'])
        sar_status = "BULLISH" if sar_series.iloc[-1] < close.iloc[-1] else "BEARISH"

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

        is_bull_signal = bull_break or (macd_signal_str == "BULLISH" and current_rsi < 50)
        is_bear_signal = bear_break or (macd_signal_str == "BEARISH" and current_rsi > 50)

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
            multi_tf_score=0.0, stochrsi_k=stochrsi_k, stochrsi_d=stochrsi_d, ma_cross=ma_cross_type,
            tsi_value=float(tsi_series.iloc[-1]) if not tsi_series.empty else None,
            sar_status=sar_status
        )
        
        base_signal.strength_score = calculate_signal_strength(base_signal)
        return base_signal
        
    except Exception as e:
        logger.warning(f"Error analyzing {symbol} on {timeframe}: {e}")
        return None

def get_signal_label(signal: Optional[SignalInfo]) -> str:
    if not signal:
        return "Nötr"
    if signal.direction == "BULLISH":
        return "AL"
    else:
        return "SAT"

async def scan_and_report():
    global LAST_SCAN_TIME, DAILY_SIGNALS
    logger.info("⏳ BIST taraması başlıyor...")
    LAST_SCAN_TIME = dt.datetime.now(IST_TZ)

    if is_market_hours():
        found_signals = []
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
            loop = asyncio.get_running_loop()
            tasks = []
            
            for symbol in TICKERS:
                tasks.append(loop.run_in_executor(executor, fetch_history, f"{symbol}.IS", "1d", "1y"))
            
            daily_dfs = await asyncio.gather(*tasks, return_exceptions=True)

            for i, symbol in enumerate(TICKERS):
                df_daily = daily_dfs[i]
                if isinstance(df_daily, Exception):
                    logger.warning(f"Error fetching daily data for {symbol}: {df_daily}")
                    continue
                
                # Çoklu zaman dilimi sinyal toplama
                multi_tf_results = {}
                for tf in TIMEFRAMES:
                    signal = fetch_and_analyze_data(symbol, tf)
                    if signal:
                        multi_tf_results[tf] = signal
                
                if multi_tf_results:
                    highest_score_signal = max(multi_tf_results.values(), key=lambda s: s.strength_score)
                    
                    if highest_score_signal.direction == "BULLISH" and highest_score_signal.strength_score >= MIN_SIGNAL_SCORE:
                        
                        # Günlük sinyal kontrolü
                        today_date_str = dt.datetime.now(IST_TZ).strftime('%Y-%m-%d')
                        if symbol in DAILY_SIGNALS and DAILY_SIGNALS[symbol].get('direction') == "BULLISH":
                            logger.info(f"🚫 {symbol}.IS için bugün zaten AL sinyali gönderildi, tekrar gönderilmiyor.")
                            continue
                            
                        # Tek mesaj formatı oluşturma
                        message = f"<b>🟢 AL SİNYALİ - {symbol}.IS</b>\n\n"
                        message += f"<b>Güç:</b> {highest_score_signal.strength_score:.1f}/10 ({highest_score_signal.timeframe} zaman dilimi)\n"
                        message += f"<b>Fiyat:</b> {highest_score_signal.price:.2f} TL\n"
                        message += "---"
                        
                        # Tüm zaman dilimi özetini ekle
                        summary_text = ""
                        for tf in TIMEFRAMES:
                            status = get_signal_label(multi_tf_results.get(tf))
                            summary_text += f"{tf}: <b>{status}</b> - "
                        message += f"\n\nZaman Dilimi Özeti:\n{summary_text.strip(' - ')}"
                        
                        # Yeni eklenen teknik gösterge detayları
                        message += f"\n\n<b>Detaylı Analiz:</b>"
                        message += f"\n• <b>RSI:</b> {highest_score_signal.rsi:.2f}"
                        message += f"\n• <b>MACD Sinyali:</b> {highest_score_signal.macd_signal}"
                        message += f"\n• <b>Hacim Oranı:</b> {highest_score_signal.volume_ratio:.2f}x"
                        message += f"\n• <b>TSI:</b> {highest_score_signal.tsi_value:.2f}"
                        message += f"\n• <b>SAR:</b> {highest_score_signal.sar_status}"
                        if highest_score_signal.candle_formation:
                             message += f"\n• <b>Mum Formasyonu:</b> {highest_score_signal.candle_formation}"

                        found_signals.append(highest_score_signal)
                        DAILY_SIGNALS[symbol] = asdict(highest_score_signal)
                        await send_telegram(message)
                        logger.info(f"🎉 Sinyal Bulundu: {symbol} - {highest_score_signal.timeframe} - Güç: {highest_score_signal.strength_score:.1f}")

        logger.info(f"✅ BIST taraması tamamlandı. {len(found_signals)} sinyal bulundu.")
    else:
        logger.info("❌ Borsa kapalı, tarama yapılmadı.")
        now_ist = dt.datetime.now(IST_TZ)
        if now_ist.time() >= dt.time(DAILY_REPORT_HOUR, DAILY_REPORT_MINUTE) and \
           (now_ist.time() - dt.timedelta(minutes=CHECK_EVERY_MIN)) < dt.time(DAILY_REPORT_HOUR, DAILY_REPORT_MINUTE):
            await send_daily_report()
            DAILY_SIGNALS.clear() # Yeni gün için sıfırla

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
            await asyncio.sleep(sleep_duration)

        except Exception as e:
            logger.error(f"Main loop error: {e}")
            await asyncio.sleep(60)

async def start_server():
    app = web.Application()
    app.router.add_get('/health', HealthHandler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', HEALTH_CHECK_PORT)
    await site.start()
    logger.info(f"✅ Sağlık kontrol sunucusu {HEALTH_CHECK_PORT} portunda başlatıldı.")

async def main():
    await asyncio.gather(
        start_server(),
        run_scanner_periodically()
    )

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot kapatılıyor.")
