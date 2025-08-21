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
from dataclasses import dataclass
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
from ta.momentum import RSIIndicator, StochRSIIndicator
from ta.trend import MACD, EMAIndicator, ADXIndicator, CCIIndicator
from ta.volatility import BollingerBands, AverageTrueRange
from ta.volume import OnBalanceVolumeIndicator

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

# YENİ ZAMAN DİLİMLERİ VE AĞIRLIKLARI
ZAMAN_DILIMLERI = {
    '15m': {'limit': 150, 'agirlik': 0.15},
    '1h': {'limit': 150, 'agirlik': 0.35},
    '4h': {'limit': 150, 'agirlik': 0.30},
    '1d': {'limit': 150, 'agirlik': 0.20}
}
TIMEFRAMES = list(ZAMAN_DILIMLERI.keys())

MIN_PRICE = float(os.getenv("MIN_PRICE", "1.0"))
MIN_VOLUME_TRY = float(os.getenv("MIN_VOLUME_TRY", "5000000")) # DEĞİŞTİ!
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "2.0")) # DEĞİŞTİ!
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
MIN_SIGNAL_SCORE = float(os.getenv("MIN_SIGNAL_SCORE", "10.0")) # DEĞİŞTİ!
MIN_MULTI_TF_SCORE = float(os.getenv("MIN_MULTI_TF_SCORE", "5.0")) # Yeni parametre

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
    stoch_rsi: float
    macd_val: float
    macd_signal_val: float
    ema_7: float
    ema_25: float
    adx: float
    volume_ratio: float
    volume_try: float
    bb_position: str
    strength_score: float
    timestamp: str
    breakout_angle: Optional[float] = None
    candle_formation: Optional[str] = None
    ma_cross: Optional[str] = None
    
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

def calculate_single_timeframe_score(df: pd.DataFrame) -> Tuple[float, Dict]:
    puan = 0
    puan_detaylari = {}
    
    last_close = df['close'].iloc[-1]
    
    # 1. Trend Analizi (EMA)
    ema_7 = df['ema_7'].iloc[-1]
    ema_25 = df['ema_25'].iloc[-1]
    if ema_7 > ema_25:
        puan += 2
        puan_detaylari['trend'] = 2
    else:
        puan -= 2
        puan_detaylari['trend'] = -2
    
    # 2. RSI Analizi
    rsi = df['rsi'].iloc[-1]
    if rsi < 30:
        puan += 2
        puan_detaylari['rsi'] = 2
    elif rsi > 70:
        puan -= 2
        puan_detaylari['rsi'] = -2
    elif 30 <= rsi < 40:
        puan += 1
        puan_detaylari['rsi'] = 1
    elif 60 < rsi <= 70:
        puan -= 1
        puan_detaylari['rsi'] = -1
    else:
        puan_detaylari['rsi'] = 0

    # 3. MACD Analizi
    macd_val = df['macd'].iloc[-1]
    macd_signal = df['macd_signal'].iloc[-1]
    if macd_val > macd_signal and macd_val > 0:
        puan += 2
        puan_detaylari['macd'] = 2
    elif macd_val < macd_signal and macd_val < 0:
        puan -= 2
        puan_detaylari['macd'] = -2
    elif macd_val > macd_signal:
        puan += 1
        puan_detaylari['macd'] = 1
    elif macd_val < macd_signal:
        puan -= 1
        puan_detaylari['macd'] = -1
    else:
        puan_detaylari['macd'] = 0
    
    # 4. StochRSI Analizi
    stoch_rsi = df['stoch_rsi'].iloc[-1]
    if stoch_rsi < 20:
        puan += 1
        puan_detaylari['stoch_rsi'] = 1
    elif stoch_rsi > 80:
        puan -= 1
        puan_detaylari['stoch_rsi'] = -1
    else:
        puan_detaylari['stoch_rsi'] = 0

    # 5. Bollinger Bands Analizi
    bb_upper = df['bb_upper'].iloc[-1]
    bb_lower = df['bb_lower'].iloc[-1]
    if last_close < bb_lower:
        puan += 1
        puan_detaylari['bollinger'] = 1
    elif last_close > bb_upper:
        puan -= 1
        puan_detaylari['bollinger'] = -1
    else:
        puan_detaylari['bollinger'] = 0

    # 6. ADX Analizi
    adx = df['adx'].iloc[-1]
    if adx > 25:
        adx_puani = 1 if puan > 0 else -1
        puan += adx_puani
        puan_detaylari['adx'] = adx_puani
    else:
        puan_detaylari['adx'] = 0

    # 7. Hacim ve Volatilite Analizi
    avg_volume_20 = df['volume'].rolling(window=20).mean().iloc[-1]
    volume_ratio = df['volume'].iloc[-1] / avg_volume_20 if avg_volume_20 > 0 else 1

    avg_atr_20 = df['atr'].rolling(window=20).mean().iloc[-1]
    current_atr = df['atr'].iloc[-1]

    if volume_ratio > 2.0 and current_atr > avg_atr_20:
        volume_puani = 1 if puan > 0 else -1
        puan += volume_puani
        puan_detaylari['volume'] = volume_puani
    else:
        puan_detaylari['volume'] = 0
        
    return puan, puan_detaylari

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

class TeknikAnaliz:
    @staticmethod
    def get_indicators(df: pd.DataFrame):
        """ta kütüphanesini kullanarak göstergeleri hesaplar (sınıf tabanlı API)"""
        if len(df) < 30: # StochRSI ve ADX için minimum veri sayısını artır
            raise ValueError("Hesaplama için yetersiz veri noktası.")
            
        close = df['close']
        high = df['high']
        low = df['low']

        # RSI
        rsi_ind = RSIIndicator(close=close)
        df['rsi'] = rsi_ind.rsi()
            
        # StochRSI
        stoch_rsi_ind = StochRSIIndicator(close=close)
        df['stoch_rsi'] = stoch_rsi_ind.stochrsi()

        # MACD
        macd_ind = MACD(close=close)
        df['macd'] = macd_ind.macd()
        df['macd_signal'] = macd_ind.macd_signal()

        # Bollinger
        bb = BollingerBands(close=close)
        df['bb_upper'] = bb.bollinger_hband()
        df['bb_middle'] = bb.bollinger_mavg()
        df['bb_lower'] = bb.bollinger_lband()

        # EMA'lar
        df['ema_7'] = EMAIndicator(close=close, window=7).ema_indicator()
        df['ema_25'] = EMAIndicator(close=close, window=25).ema_indicator()

        # ATR (avg_true_range yerine)
        atr_ind = AverageTrueRange(high=high, low=low, close=close, window=20)
        df['atr'] = atr_ind.average_true_range().replace(0, np.nan)
            
        # ADX
        adx_ind = ADXIndicator(high=high, low=low, close=close, window=14)
        df['adx'] = adx_ind.adx()
        
        return df

def fetch_and_analyze_data(symbol: str) -> Optional[SignalInfo]:
    try:
        yf_symbol = f"{symbol}.IS"
        
        # Çoklu zaman dilimleri için verileri çek ve analiz et
        signal_scores_by_tf = {}
        data_by_tf = {}
        
        for tf, params in ZAMAN_DILIMLERI.items():
            if tf == '1d':
                base_interval, period = '1d', f"{params['limit']}d"
                df = fetch_history(yf_symbol, base_interval, period)
                df = _standardize_df(df)
            elif tf == '1h':
                base_interval, period = '1h', f"{params['limit']//2}d"
                df = fetch_history(yf_symbol, base_interval, period)
                df = _standardize_df(df)
            elif tf == '4h':
                base_interval, period = '1h', f"{params['limit']}d"
                raw = fetch_history(yf_symbol, base_interval, period)
                if raw is None or raw.empty: continue
                raw = _standardize_df(raw)
                df = _resample_ohlcv(raw, '4h')
            elif tf == '15m':
                base_interval, period = '15m', f"{params['limit']//2}d"
                df = fetch_history(yf_symbol, base_interval, period)
                df = _standardize_df(df)
            else:
                continue
        
            if df is None or df.empty or len(df) < 30:
                continue

            df = df.dropna().tail(params['limit'])
            
            # SIKI FİLTRELER: Fiyat ve hacim kontrolü
            last_close = float(df['close'].iloc[-1])
            last_vol = float(df['volume'].iloc[-1])
            if last_close < MIN_PRICE or last_close * last_vol < MIN_VOLUME_TRY:
                continue
                
            df_indicators = TeknikAnaliz.get_indicators(df.copy())
            df_indicators = df_indicators.ffill().bfill() # NaN değerleri doldur

            score, details = calculate_single_timeframe_score(df_indicators)
            signal_scores_by_tf[tf] = score
            data_by_tf[tf] = df_indicators

        if not signal_scores_by_tf:
            return None
            
        # Çoklu zaman dilimi puanı hesaplama
        total_score = 0
        for tf, score in signal_scores_by_tf.items():
            agirlik = ZAMAN_DILIMLERI[tf]['agirlik']
            total_score += score * agirlik
            
        # Yön belirleme (Çoğunluk puanına göre)
        direction = "BULLISH" if total_score > 0 else "BEARISH"
        
        # En detaylı TF (genelde 15m) için ek bilgiler
        tf_to_report = '15m' if '15m' in data_by_tf else next(iter(data_by_tf))
        df_report = data_by_tf[tf_to_report]
        
        # Eski analizin ek özellikleri
        close = df_report['close']
        rsi_series = rsi_from_close(close, RSI_LEN)
        highs_idx, lows_idx = find_pivots(rsi_series, PIVOT_PERIOD, PIVOT_PERIOD)
        now_i = len(rsi_series) - 1
        bull_break, bear_break, breakout_angle = detect_breakouts(rsi_series, highs_idx, lows_idx, now_i)
        
        candle_form = detect_candle_formation(df_report)
        ma_cross_type = detect_ma_cross(close, MA_SHORT, MA_LONG)

        final_signal_score = (10 + total_score) / 2 # Puanı 0-10 arasına ölçekle
        final_signal_score = max(0, min(10, final_signal_score))
        
        if final_signal_score < MIN_MULTI_TF_SCORE:
            return None
        
        # Hacim kontrolü (Ana zaman diliminde)
        avg_volume_20 = df_report['volume'].rolling(window=20).mean().iloc[-1]
        volume_ratio = df_report['volume'].iloc[-1] / avg_volume_20 if avg_volume_20 > 0 else 1
        if volume_ratio < MIN_VOLUME_RATIO:
            return None

        # Sinyal Yönü Kontrolü
        if direction == "BULLISH" and (final_signal_score < 7.5 or (df_report['macd'].iloc[-1] < df_report['macd_signal'].iloc[-1])):
            return None
        if direction == "BEARISH" and (final_signal_score < 7.5 or (df_report['macd'].iloc[-1] > df_report['macd_signal'].iloc[-1])):
            return None
        
        current_price = float(df_report['close'].iloc[-1])
        volume_try = current_price * float(df_report['volume'].iloc[-1])
        
        bb_pos = "" # Artık ana puanlamada kullanılmadığı için boş bırakılabilir
        
        base_signal = SignalInfo(
            symbol=symbol, timeframe=tf_to_report, direction=direction, price=current_price,
            rsi=float(df_report['rsi'].iloc[-1]),
            stoch_rsi=float(df_report['stoch_rsi'].iloc[-1]),
            macd_val=float(df_report['macd'].iloc[-1]),
            macd_signal_val=float(df_report['macd_signal'].iloc[-1]),
            ema_7=float(df_report['ema_7'].iloc[-1]),
            ema_25=float(df_report['ema_25'].iloc[-1]),
            adx=float(df_report['adx'].iloc[-1]),
            volume_ratio=volume_ratio,
            volume_try=volume_try,
            bb_position=bb_pos,
            strength_score=final_signal_score,
            timestamp=df_report.index[-1].strftime('%Y-%m-%d %H:%M'),
            breakout_angle=breakout_angle,
            candle_formation=candle_form,
            ma_cross=ma_cross_type
        )
        
        return base_signal
    except Exception as e:
        logger.error(f"Analysis error {symbol}: {e}")
        return None

# --- Sinyal Veritabanı (Disk Tabanlı) ---
def get_signals_db_file():
    """Günlük sinyal dosyası adı"""
    today_str = dt.datetime.now(IST_TZ).strftime("%Y-%m-%d")
    return f"signals_{today_str}.json"

def load_daily_signals_from_disk():
    """Disk tabanlı günlük sinyalleri yükle"""
    global DAILY_SIGNALS
    db_file = get_signals_db_file()
    
    if os.path.exists(db_file) and os.stat(db_file).st_size > 0:
        try:
            with open(db_file, 'r', encoding='utf-8') as f:
                DAILY_SIGNALS = json.load(f)
            logger.info(f"📂 {len(DAILY_SIGNALS)} günlük sinyal yüklendi")
        except Exception as e:
            logger.error(f"Günlük sinyaller yüklenemedi: {e}")
            DAILY_SIGNALS = {}
    else:
        DAILY_SIGNALS = {}

def save_daily_signals_to_disk():
    """Günlük sinyalleri diske kaydet"""
    db_file = get_signals_db_file()
    try:
        with open(db_file, 'w', encoding='utf-8') as f:
            json.dump(DAILY_SIGNALS, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Günlük sinyaller kaydedilemedi: {e}")

def is_signal_already_sent_today(symbol: str, direction: str) -> bool:
    """Bugün bu hisse için bu yönde sinyal gönderildi mi? - Disk tabanlı kontrol"""
    today_str = dt.datetime.now(IST_TZ).strftime("%Y-%m-%d")
    signal_key = f"{symbol}_{direction}"
    
    if signal_key in DAILY_SIGNALS:
        signal_date = DAILY_SIGNALS[signal_key].get('date', '')
        if signal_date == today_str:
            return True
    
    return False

def save_daily_signal(signal: SignalInfo):
    """Günlük sinyal veritabanına kaydet - Disk tabanlı"""
    today_str = dt.datetime.now(IST_TZ).strftime("%Y-%m-%d")
    signal_key = f"{signal.symbol}_{signal.direction}"
    
    DAILY_SIGNALS[signal_key] = {
        'date': today_str,
        'symbol': signal.symbol,
        'direction': signal.direction,
        'price': signal.price,
        'timeframe': signal.timeframe,
        'strength_score': signal.strength_score,
        'timestamp': signal.timestamp,
        'volume_ratio': signal.volume_ratio,
        'sent_time': dt.datetime.now(IST_TZ).isoformat()
    }
    save_daily_signals_to_disk()
    
async def send_enhanced_alert(signal: SignalInfo):
    """Gelişmiş sinyal uyarısını Telegram'a gönderir."""
    emoji = "🟢" if signal.direction == "BULLISH" else "🔴"
    trend_emoji = "📈" if signal.direction == "BULLISH" else "📉"
    
    message = (
        f"<b>{emoji} Sinyal: {signal.symbol}.IS</b>\n\n"
        f"• Yön: <b>{signal.direction}</b>\n"
        f"• Zaman Dilimi: <b>{signal.timeframe.upper()}</b>\n"
        f"• Son Fiyat: <b>{signal.price:.2f} ₺</b>\n"
        f"• Sinyal Gücü: <b>{signal.strength_score:.1f}/10</b>\n"
        f"• Hacim Oranı: {signal.volume_ratio:.1f}x\n"
        f"• MACD Durumu: {signal.macd_val:.2f} / {signal.macd_signal_val:.2f}\n"
        f"• RSI: {signal.rsi:.2f}\n"
    )
    
    if signal.candle_formation:
        message += f"• Mum Formasyonu: <b>{signal.candle_formation}</b>\n"
    
    if signal.breakout_angle:
        message += f"• RSI Kırılımı: <b>{signal.breakout_angle:.1f}°</b>\n"

    if signal.ma_cross:
        message += f"• Trend Değişimi: <b>{signal.ma_cross}</b>\n"
    
    if signal.adx > 25:
        message += f"• Trend Gücü (ADX): <b>Güçlü Trend ({signal.adx:.1f})</b>\n"

    message += f"\n{trend_emoji} {signal.symbol} için güçlü bir {signal.direction} sinyali oluştu."
    
    await send_telegram(message)
    logger.info(f"✅ Telegram'a sinyal gönderildi: {signal.symbol} - {signal.direction} - {signal.strength_score:.1f}/10")

# --- Ana Tarama Döngüsü ---
async def scan_stock_async(symbol: str) -> List[SignalInfo]:
    """Her bir hisse senedi için asenkron tarama ve analiz yapar."""
    signals: List[SignalInfo] = []
    signal = await asyncio.to_thread(fetch_and_analyze_data, symbol)
    if signal and signal.strength_score >= MIN_SIGNAL_SCORE:
        signals.append(signal)
        logger.info(f"🎉 Sinyal Bulundu: {symbol} - Güç: {signal.strength_score:.1f}")
    return signals

async def run_scan_async():
    """Tüm hisseleri paralel olarak tarar."""
    global LAST_SCAN_TIME
    logger.info("⏳ BIST taraması başlıyor...")
    
    # Günlük sinyal veritabanını yükle
    load_daily_signals_from_disk()

    tasks = [scan_stock_async(ticker) for ticker in TICKERS]
    
    # Paralel işlem limiti belirleme
    semaphore = asyncio.Semaphore(10) # Aynı anda 10 task çalıştırır
    async def limited_scan(task):
        async with semaphore:
            return await task
    
    results = await asyncio.gather(*[limited_scan(t) for t in tasks])
    
    total_signals_found = 0
    for signals_list in results:
        for signal in signals_list:
            if not is_signal_already_sent_today(signal.symbol, signal.direction):
                await send_enhanced_alert(signal)
                save_daily_signal(signal)
                total_signals_found += 1
    
    LAST_SCAN_TIME = dt.datetime.now(IST_TZ)
    logger.info(f"✅ Tarama tamamlandı. Toplam bulunan yeni sinyal: {total_signals_found}")

async def main_loop():
    """Botun ana döngüsünü yönetir."""
    first_run = True
    while True:
        now_ist = dt.datetime.now(IST_TZ)
        
        # Pazar ve hafta sonu kontrolü
        if not is_market_hours():
            logger.info("Borsa kapalı. Pazar saati bekleniyor.")
            
            # Günlük raporu gönderme
            if now_ist.hour == DAILY_REPORT_HOUR and now_ist.minute == DAILY_REPORT_MINUTE:
                if 'daily_report_sent' not in DAILY_SIGNALS:
                    await send_daily_report()
                    DAILY_SIGNALS['daily_report_sent'] = 'sent' # Raporun gönderildiğini işaretle
                    save_daily_signals_to_disk()
            
            # Günlük sinyal veritabanını resetleme (yeni gün başlangıcı)
            if now_ist.hour == 0 and now_ist.minute < CHECK_EVERY_MIN and 'daily_report_sent' in DAILY_SIGNALS:
                logger.info("Yeni gün başladı, günlük sinyal listesi sıfırlanıyor.")
                DAILY_SIGNALS.clear()
                save_daily_signals_to_disk()
            
            await asyncio.sleep(60)
            continue

        if first_run or (LAST_SCAN_TIME and (now_ist - LAST_SCAN_TIME).total_seconds() >= CHECK_EVERY_MIN * 60):
            await run_scan_async()
            first_run = False
        
        # Sonraki kontrol zamanına kadar bekleme
        await asyncio.sleep(30)
        
# --- Bot Başlangıcı ---
if __name__ == "__main__":
    app = web.Application()
    app.router.add_get('/health', HealthHandler)
    
    # Arka plan görevlerini yönetmek için aiohttp'nin yerleşik mekanizması
    async def start_background_tasks(app):
        app['main_loop_task'] = asyncio.create_task(main_loop())
    
    async def cleanup_background_tasks(app):
        app['main_loop_task'].cancel()
        await app['main_loop_task']

    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)

    web.run_app(app, host='0.0.0.0', port=HEALTH_CHECK_PORT)
