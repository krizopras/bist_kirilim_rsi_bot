#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CakmaUstad RSI Strategy - TradingView Entegrasyonlu (İyileştirilmiş)
"""

import io
import os
import asyncio
import aiohttp
from aiohttp import web
import datetime as dt
import logging
import json
import sys
import locale
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
from dotenv import load_dotenv


# Karakter kodlama düzeltmesi
try:
    locale.setlocale(locale.LC_ALL, 'tr_TR.UTF-8')
except:
    try:
        locale.setlocale(locale.LC_ALL, 'Turkish_Turkey.1254')
    except:
        pass

# Load .env
load_dotenv()

# --- AYARLAR VE GÜVENLİK ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
    raise ValueError("Ortam değişkenleri (TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID) ayarlanmalı!")

# Ortam değişkenlerinden güvenli dönüşüm ve varsayılan değerler
LOG_FILE_PATH = os.getenv('LOG_FILE', 'trading_bot.log')
HEALTH_CHECK_PORT = int(os.getenv("PORT", "8080"))
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# --- LOGLAMA AYARLARI ---
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH, encoding='utf-8'),
        logging.StreamHandler(sys.stdout) # sys.stdout eklemek daha iyi
    ]
)
logger = logging.getLogger(__name__) # __name__ kullanmak en iyi pratiktir

# Rate limiting için semaphore
CONCURRENT_REQUESTS = int(os.getenv("CONCURRENT_REQUESTS", "10"))
request_semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

# --- ZAMAN DİLİMİ AYARI ---
IST_TZ = pytz.timezone('Europe/Istanbul')

# ----------------------- TARAMA AYARLARI -----------------------
# BIST Seans Saatleri
MARKET_OPEN = dt.time(9, 30)
MARKET_CLOSE = dt.time(18, 5) # 18:00 seans sonu, 18:05 kapanış fiyatı oluşur
LUNCH_START = dt.time(13, 0)
LUNCH_END = dt.time(14, 0)

def is_market_hours() -> bool:
    """Borsa İstanbul işlem saatleri kontrolü"""
    if TEST_MODE:
        logger.debug("TEST_MODE aktif - piyasa her zaman açık kabul ediliyor.")
        return True

    now_ist = dt.datetime.now(IST_TZ)

    # Hafta sonu kontrolü
    if now_ist.weekday() >= 5: 
        return False
    
    current_time = now_ist.time()
    
    # Öğle molası kontrolü
    if LUNCH_START <= current_time < LUNCH_END:
        logger.debug("Öğle molası - piyasa kapalı")
        return False
        
    return MARKET_OPEN <= current_time <= MARKET_CLOSE

# --- TARAMA PARAMETRELERİ ---
SCAN_MODE = os.getenv("SCAN_MODE", "ALL")
CUSTOM_TICKERS_STR = os.getenv("CUSTOM_TICKERS", "")

# Hisse listesi
ALL_BIST_STOCKS = [
    "ACSEL", "ADEL", "ADESE", "ADGYO", "AFYON", "AGHOL", "AGESA", "AGROT", "AHSGY", "AHGAZ",
    "AKBNK", "AKCNS", "AKYHO", "AKENR", "AKFGY", "AKFIS", "AKFYE", "ATEKS", "AKSGY", "AKMGY",
    "AKSA", "AKSEN", "AKGRT", "AKSUE", "ALCAR", "ALGYO", "ALARK", "ALBRK", "ALKIM", "ALKA",
    "ALVES", "ANSGR", "AEFES", "ANHYT", "ASUZU", "ANGEN", "ANELE", "ARCLK", "ARDYZ", "ARENA",
    "ARMGD", "ARSAN", "ARTMS", "ARZUM", "ASGYO", "ASELS", "ASTOR", "ATAGY", "ATAKP", "AGYO",
    "ATSYH", "ATLAS", "ATATP", "AVOD", "AVGYO", "AVTUR", "AVHOL", "AVPGY", "AYDEM", "AYEN",
    "AYES", "AYGAZ", "AZTEK", "A1CAP", "ACP", "A1YEN", "BAGFS", "BAHKM", "BAKAB", "BALAT",
    "BALSU", "BNTAS", "BANVT", "BARMA", "BSRFK", "BASGZ", "BASCM", "BEGYO", "BTCIM", "BSOKE",
    "BYDNR", "BAYRK", "BERA", "BRKT", "BRKSN", "BESLR", "BJKAS", "BEYAZ", "BIENY", "BLCYT",
    "BLKOM", "BIMAS", "BINBN", "BIOEN", "BRKVY", "BRKO", "BIGEN", "BRLSM", "BRMEN", "BIZIM",
    "BMSTL", "BMSCH", "BNPPI", "BOBET", "BORSK", "BORLS", "BRSAN", "BRYAT", "BFREN", "BOSSA",
    "BRISA", "BULGS", "BLS", "BLSMD", "BURCE", "BURVA", "BRGFK", "BUCIM", "BVSAN", "BIGCH",
    "CRFSA", "CASA", "CEMZY", "CEOEM", "CCOLA", "CONSE", "COSMO", "CRDFA", "CVKMD", "CWENE",
    "CGCAM", "CAGFA", "CANTE", "CATES", "CLEBI", "CELHA", "CLKMT", "CEMAS", "CEMTS", "CMBTN",
    "CMENT", "CIMSA", "CUSAN", "DVRLK", "DYBNK", "DAGI", "DAPGM", "DARDL", "DGATE", "DCTTR",
    "DGRVK", "DMSAS", "DENGE", "DENFA", "DNFIN", "DZGYO", "DZY", "DZYMK", "DENIZ", "DNZ",
    "DERIM", "DERHL", "DESA", "DESPC", "DSTKF", "DTBMK", "DEVA", "DNISI", "DIRIT", "DITAS",
    "DMRGD", "DOCO", "DOFER", "DOBUR", "DOHOL", "DTRND", "DGNMO", "ARASE", "DOGUB", "DGGYO",
    "DOAS", "DFKTR", "DOKTA", "DURDO", "DURKN", "DNYVA", "DYOBY", "EBEBK", "ECZYT", "EDATA",
    "EDIP", "EFORC", "EGEEN", "EGGUB", "EGPRO", "EGSER", "EPLAS", "EGEGY", "ECILC", "EKER",
    "EKIZ", "EKOFA", "EKOS", "EKOVR", "EKSUN", "ELITE", "EMKEL", "EMNIS", "EMIRV", "EKTVK",
    "DMLKT", "EKGYO", "EMVAR", "ENDAE", "ENJSA", "ENERY", "ENKAI", "ENSRI", "ERBOS", "ERCB",
    "EREGL", "KIMMR", "ERSU", "ESCAR", "ESCOM", "ESEN", "ETILR", "EUKYO", "EUYO", "ETYAT",
    "EUHOL", "TEZOL", "EUREN", "EUPWR", "EYGYO", "FADE", "FSDAT", "FMIZP", "FENER", "FIBAF",
    "FBB", "FBBNK", "FLAP", "FONET", "FROTO", "FORMT", "FORTE", "FRIGO", "FZLGY", "GWIND",
    "GSRAY", "GARFA", "GARFL", "GRNYO", "GEDIK", "GEDZA", "GLCVY", "GENIL", "GENTS", "GEREL",
    "GZNMI", "GIPTA", "GMTAS", "GESAN", "GLB", "GLBMD", "GLYHO", "GGBVK", "GSIPD", "GOODY",
    "GOKNR", "GOLTS", "GOZDE", "GRTHO", "GSDDE", "GSDHO", "GUBRF", "GLRYH", "GLRMK", "GUNDG",
    "GRSEL", "SAHOL", "HALKF", "HLGYO", "HLVKS", "HALKI", "HLY", "HRKET", "HATEK", "HATSN",
    "HAYVK", "HDFFL", "HDFGS", "HEDEF", "HDFVK", "HDFYB", "HYB", "HEKTS", "HEPFN", "HKTM",
    "HTTBT", "HOROZ", "HUBVC", "HUNER", "HUZFA", "HURGZ", "ENTRA", "ICB", "ICBCT", "ICUGS",
    "INGRM", "INVEO", "IAZ", "INVAZ", "INVES", "ISKPL", "IEYHO", "IDGYO", "IHEVA", "IHLGM",
    "IHGZT", "IHAAS", "IHLAS", "IHYAY", "IMASM", "INALR", "INDES", "INFO", "IYF", "INTEK",
    "INTEM", "IPEKE", "ISDMR", "ISTFK", "ISFAK", "ISFIN", "ISGYO", "ISGSY", "ISMEN", "IYM",
    "ISYAT", "ISBIR", "ISSEN", "IZINV", "IZENR", "IZMDC", "IZFAS", "JANTS", "KFEIN", "KLKIM",
    "KLSER", "KLVKS", "KLYPV", "KTEST", "KAPLM", "KRDMA", "KRDMB", "KRDMD", "KAREL", "KARSN",
    "KRTEK", "KARTN", "KATVK", "KTLEV", "KATMR", "KAYSE", "KNTFA", "KENT", "KRVGD", "KERVN",
    "TCKRC", "KZBGY", "KLGYO", "KLRHO", "KMPUR", "KLMSN", "KCAER", "KFKTF", "KOCFN", "KCHOL",
    "KOCMT", "KLSYN", "KNFRT", "KONTR", "KONYA", "KONKA", "KGYO", "KORDS", "KRPLS", "KORTS",
    "KOTON", "KOZAL", "KOZAA", "KOPOL", "KRGYO", "KRSTL", "KRONT", "KTKVK", "KTSVK", "KSTUR",
    "KUVVA", "KUYAS", "KBORU", "KZGYO", "KUTPO", "KTSKR", "LIDER", "LVTVK", "LIDFA", "LILAK",
    "LMKDC", "LINK", "LOGO", "LKMNH", "LRSHO", "LUKSK", "LYDHO", "LYDYE", "MACKO", "MAKIM",
    "MAKTK", "MANAS", "MRBAS", "MRS", "MAGEN", "MRMAG", "MARKA", "MAALT", "MRSHL", "MRGYO",
    "MARTI", "MTRKS", "MAVI", "MZHLD", "MEDTR", "MEGMT", "MEGAP", "MEKAG", "MEKMD", "MSA",
    "MNDRS", "MEPET", "MERCN", "MRBKF", "MBFTR", "MERIT", "MERKO", "METUR", "METRO", "MTRYO",
    "MHRGY", "MIATK", "MGROS", "MSGYO", "MSY", "MSYBN", "MPARK", "MMCAS", "MNGFA", "MOBTL",
    "MOGAN", "MNDTR", "MOPAS", "EGEPO", "NATEN", "NTGAZ", "NTHOL", "NETAS", "NIBAS", "NUHCM",
    "NUGYO", "NRHOL", "NRLIN", "NURVK", "NRBNK", "NYB", "OBAMS", "OBASE", "ODAS", "ODINE",
    "OFSYM", "ONCSM", "ONRYT", "OPET", "ORCAY", "ORFIN", "ORGE", "ORMA", "OMD", "OSMEN",
    "OSTIM", "OTKAR", "OTOKC", "OTOSR", "OTTO", "OYAKC", "OYA", "OYYAT", "OYAYO", "OYLUM",
    "OZKGY", "OZATD", "OZGYO", "OZRDN", "OZSUB", "OZYSR", "PAMEL", "PNLSN", "PAGYO", "PAPIL",
    "PRFFK", "PRDGS", "PRKME", "PARSN", "PBT", "PBTR", "PASEU", "PSGYO", "PATEK", "PCILT",
    "PGSUS", "PEKGY", "PENGD", "PENTA", "PSDTC", "PETKM", "PKENT", "PETUN", "PINSU", "PNSUT",
    "PKART", "PLTUR", "POLHO", "POLTK", "PRZMA", "QFINF", "QYATB", "YBQ", "QYHOL", "FIN",
    "QNBTR", "QNBFF", "QNBFK", "QNBVK", "QUAGR", "QUFIN", "RNPOL", "RALYH", "RAYSG", "REEDR",
    "RYGYO", "RYSAS", "RODRG", "ROYAL", "RGYAS", "RTALB", "RUBNS", "RUZYE", "SAFKR", "SANEL",
    "SNICA", "SANFM", "SANKO", "SAMAT", "SARKY", "SARTN", "SASA", "SAYAS", "SDTTR", "SEGMN",
    "SEKUR", "SELEC", "SELGD", "SELVA", "SNKRN", "SRNT", "SRVGY", "SEYKM", "SILVR", "SNGYO",
    "SKYLP", "SMRTG", "SMART", "SODSN", "SOKE", "SKTAS", "SONME", "SNPAM", "SUMAS", "SUNTK",
    "SURGY", "SUWEN", "SZUKI", "SMRFA", "SMRVA", "SEKFK", "SEGYO", "SKY", "SKYMD", "SEK",
    "SKBNK", "SOKM", "TABGD", "TAC", "TCRYT", "TAMFA", "TNZTP", "TARKM", "TATGD", "TATEN",
    "TAVHL", "DRPHN", "TEBFA", "TEBCE", "TEKTU", "TKFEN", "TKNSA", "TMPOL", "TRFFA", "DAGHL",
    "TAE", "TRBNK", "TERA", "TRA", "TEHOL", "TFNVK", "TGSAS", "TIMUR", "TRYKI", "TOASO",
    "TRGYO", "TLMAN", "TSPOR", "TDGYO", "TRMEN", "TVM", "TSGYO", "TUCLK", "TUKAS", "TRCAS",
    "TUREX", "MARBL", "TRKFN", "TRILC", "TCELL", "TBA", "TRKSH", "TRKNT", "TMSN", "TUPRS",
    "THYAO", "PRKAB", "TTKOM", "TTRAK", "TBORG", "TURGG", "GARAN", "TGB", "HALKB", "THL",
    "EXIMB", "THR", "ISATR", "ISBTR", "ISCTR", "ISKUR", "TIB", "KLN", "KLNMA", "TSK", "TSKB",
    "TURSG", "SISE", "TVB", "VAKBN", "TV8TV", "UFUK", "ULAS", "ULUFA", "ULUSE", "ULUUN",
    "UMPAS", "USAK", "ULKER", "UNLU", "VAKFA", "VAKFN", "VKGYO", "VKFYO", "VAKVK", "VAKKO",
    "VANGD", "VBTYZ", "VDFFA", "VDFLO", "VRGYO", "VERUS", "VERTU", "VESBE", "VESTL", "VKING",
    "VSNMD", "VDFAS", "YKFKT", "YKFIN", "YKR", "YKYAT", "YKB", "YKBNK", "YAPRK", "YATAS",
    "YAT", "YFMEN", "YATVK", "YYLGD", "YAYLA", "YGGYO", "YEOTK", "YGYO", "YYAPI", "YESIL",
    "YBTAS", "YIGIT", "YONGA", "YKSLN", "YUNSA", "ZEDUR", "ZRGYO", "ZKBVK", "ZKBVR", "ZOREN",
    "BINHO"
]

if SCAN_MODE.upper() == "CUSTOM" and CUSTOM_TICKERS_STR:
    TICKERS = [t.strip() for t in CUSTOM_TICKERS_STR.split(',')]
else:
    TICKERS = ALL_BIST_STOCKS

CHECK_EVERY_MIN = int(os.getenv("CHECK_EVERY_MIN", "30"))
TIMEFRAMES = [t.strip() for t in os.getenv("TIMEFRAMES", "").split(',') if t.strip()] or ["1h", "4h", "1d"]
MIN_PRICE = float(os.getenv("MIN_PRICE", "1.0"))
MIN_VOLUME_TRY = float(os.getenv("MIN_VOLUME_TRY", "1000000"))
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "2.0"))
RSI_LEN = int(os.getenv("RSI_LEN", "22"))
RSI_EMA_LEN = int(os.getenv("RSI_EMA", "66"))
PIVOT_PERIOD = int(os.getenv("PIVOT_PERIOD", "10"))
MIN_SIGNAL_SCORE = float(os.getenv("MIN_SIGNAL_SCORE", "2.15"))
MAX_RETRY_ATTEMPTS = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))

# --- GLOBAL DURUM DEĞİŞKENLERİ ---
LAST_SCAN_TIME: Optional[dt.datetime] = None
START_TIME = time.time()
DAILY_SIGNALS: Dict[str, Dict] = {}
FAILED_SYMBOLS: Set[str] = set()

# --- VERİ YAPILARI ---
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
    strength_score: float
    timestamp: str
    trend_break: Optional[str] = None
    confidence: float = 0.0

# ----------------------- YARDIMCI FONKSİYONLAR -----------------------
def cleanup_old_signals():
    """Eski sinyalleri temizle"""
    global DAILY_SIGNALS, FAILED_SYMBOLS
    now = dt.datetime.now(IST_TZ)
    
    # Gece yarısı sinyalleri temizle
    if now.hour == 0 and now.minute < CHECK_EVERY_MIN:
        old_count = len(DAILY_SIGNALS)
        DAILY_SIGNALS.clear()
        FAILED_SYMBOLS.clear()
        logger.info(f"Günlük sinyaller temizlendi. ({old_count} sinyal silindi)")

def safe_float_convert(value, default: float = 0.0) -> float:
    """Güvenli float dönüştürme"""
    try:
        if value is None or value == '' or str(value).lower() in ['nan', 'none']:
            return default
        return float(value)
    except (ValueError, TypeError):
        return default



# ----------------------- CAKMAUSTAD RSI STRATEJİ FONKSİYONLARI -----------------------
def rma(series: pd.Series, length: int) -> pd.Series:
    """Running Moving Average (RMA) hesapla"""
    alpha = 1.0 / length
    return series.ewm(alpha=alpha, adjust=False).mean()

def rsi_pine(close: pd.Series, length: int = 14) -> pd.Series:
    """Pine Script benzeri RSI hesaplama"""
    if len(close) < length:
        return pd.Series([50.0] * len(close), index=close.index)
    
    delta = close.diff()
    up = delta.where(delta > 0, 0.0)
    down = (-delta).where(delta < 0, 0.0)
    up_rma = rma(up, length)
    down_rma = rma(down, length)
    rs = up_rma / down_rma.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50)

def ema(series: pd.Series, length: int) -> pd.Series:
    """Exponential Moving Average"""
    return series.ewm(span=length, adjust=False).mean()

def detect_trend_breakouts(rsi_series: pd.Series, pivot_period: int = 10) -> Dict[str, bool]:
    """Geliştirilmiş trend kırılım tespiti"""
    if len(rsi_series) < pivot_period * 2:
        return {'bull_break': False, 'bear_break': False}
    
    bull_break = False
    bear_break = False
    
    # Son değerleri analiz et
    recent_rsi = rsi_series.tail(20)
    current_rsi = recent_rsi.iloc[-1]
    
    if len(recent_rsi) >= 10:
        # Trend analizi
        first_half_max = recent_rsi.iloc[:10].max()
        second_half_max = recent_rsi.iloc[10:].max()
        first_half_min = recent_rsi.iloc[:10].min()
        second_half_min = recent_rsi.iloc[10:].min()
        
        # Bullish breakout: Düşen trendden çıkış
        if (first_half_max > second_half_max and 
            current_rsi > second_half_max and 
            current_rsi > recent_rsi.iloc[-2]):
            bull_break = True
        
        # Bearish breakout: Yükselen trendden çıkış
        if (first_half_min < second_half_min and 
            current_rsi < second_half_min and 
            current_rsi < recent_rsi.iloc[-2]):
            bear_break = True
    
    return {'bull_break': bull_break, 'bear_break': bear_break}

def calculate_signal_strength(signal: SignalInfo) -> float:
    """Geliştirilmiş sinyal gücü hesaplama"""
    score = 0.0
    
    # RSI seviyesi skorlaması
    if signal.direction == "BULLISH":
        if signal.rsi <= 25:
            score += 5.0
        elif signal.rsi <= 30:
            score += 4.0
        elif signal.rsi <= 35:
            score += 3.0
        elif signal.rsi <= 40:
            score += 2.0
        elif signal.rsi <= 45:
            score += 1.0
    else:  # BEARISH
        if signal.rsi >= 75:
            score += 5.0
        elif signal.rsi >= 70:
            score += 4.0
        elif signal.rsi >= 65:
            score += 3.0
        elif signal.rsi >= 60:
            score += 2.0
        elif signal.rsi >= 55:
            score += 1.0
    
    # RSI-EMA ilişki skorlaması
    rsi_ema_diff = abs(signal.rsi - signal.rsi_ema)
    if signal.direction == "BULLISH" and signal.rsi > signal.rsi_ema:
        score += min(3.0, rsi_ema_diff / 10.0)
    elif signal.direction == "BEARISH" and signal.rsi < signal.rsi_ema:
        score += min(3.0, rsi_ema_diff / 10.0)
    
    # Trend kırılımı bonus
    if signal.trend_break:
        score += 2.5
    
    # Hacim gücü skorlaması
    if signal.volume_ratio >= 4.0:
        score += 2.5
    elif signal.volume_ratio >= 3.0:
        score += 2.0
    elif signal.volume_ratio >= 2.0:
        score += 1.5
    elif signal.volume_ratio >= 1.5:
        score += 1.0
    
    # Zaman dilimi ağırlığı
    timeframe_weights = {'15m': 0.5, '1h': 1.0, '4h': 2.0, '1d': 3.0}
    score += timeframe_weights.get(signal.timeframe, 1.0)
    
    # Fiyat seviyesi kontrolü (çok düşük fiyatlı hisseler için)
    if signal.price < 5.0:
        score *= 0.8  # Penaltı
    elif signal.price > 100.0:
        score *= 1.1  # Bonus
    
    return min(10.0, max(0.0, score))

def calculate_confidence_level(signal: SignalInfo, market_conditions: Dict[str, Any] = None) -> float:
    """Sinyal güven seviyesi hesaplama"""
    confidence = 0.0
    
    # Temel güven skorlaması
    if signal.strength_score >= 8.0:
        confidence += 0.4
    elif signal.strength_score >= 6.0:
        confidence += 0.3
    elif signal.strength_score >= 4.0:
        confidence += 0.2
    else:
        confidence += 0.1
    
    # RSI ekstrem seviyelerde daha yüksek güven
    if signal.direction == "BULLISH" and signal.rsi <= 30:
        confidence += 0.3
    elif signal.direction == "BEARISH" and signal.rsi >= 70:
        confidence += 0.3
    
    # Zaman dilimi güveni
    if signal.timeframe in ['4h', '1d']:
        confidence += 0.2
    
    # Hacim onayı
    if signal.volume_ratio >= 2.0:
        confidence += 0.1
    
    return min(1.0, confidence)

# ----------------------- YAHOO FINANCE VERİ ÇEKME FONKSİYONLARI -----------------------

# Global rate limiting kaldırıldı, yfinance kendi limitlerini yönetir

def get_yfinance_ticker(symbol: str) -> str:
    """BIST sembolünü Yahoo Finance formatına dönüştürür."""
    if not symbol.endswith(".IS"):
        return f"{symbol}.IS"
    return symbol

def calculate_rsi(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """DataFrame üzerinden RSI hesaplar."""
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_ema(df: pd.DataFrame, period: int = 20) -> pd.Series:
    """DataFrame üzerinden EMA hesaplar."""
    return df['Close'].ewm(span=period, adjust=False).mean()

def calculate_macd(df: pd.DataFrame, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9) -> Tuple[pd.Series, pd.Series]:
    """DataFrame üzerinden MACD ve Signal Line hesaplar."""
    ema_fast = df['Close'].ewm(span=fast_period, adjust=False).mean()
    ema_slow = df['Close'].ewm(span=slow_period, adjust=False).mean()
    macd = ema_fast - ema_slow
    signal = macd.ewm(span=signal_period, adjust=False).mean()
    return macd, signal

async def fetch_yfinance_data(symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
    """Yahoo Finance'dan hisse senedi verisi çeker ve teknik göstergeleri hesaplar."""
    yf_symbol = get_yfinance_ticker(symbol)
    
    # yfinance için zaman dilimi eşlemesi
    # TradingView'dan farklı olarak, yfinance'da "1h", "1d" gibi stringler doğrudan çalışır
    
    try:
        logger.debug(f"Yahoo Finance verisi çekiliyor: {yf_symbol} ({timeframe})")
        ticker = yf.Ticker(yf_symbol)
        history = ticker.history(period="1y", interval=timeframe)
        
        if history.empty or 'Close' not in history.columns:
            logger.warning(f"Yahoo Finance'dan veri çekilemedi veya eksik veri: {yf_symbol}")
            return None
        
        # Son veriyi al
        latest_data = history.iloc[-1]
        
        # Göstergeleri hesapla
        history['RSI'] = calculate_rsi(history)
        history['EMA'] = calculate_ema(history)
        history['MACD'] = calculate_macd(history)[0]
        history['MACD_Signal'] = calculate_macd(history)[1]

        # Sadece son veriyi döndür
        result = {
            'Open': safe_float_convert(latest_data.get('Open')),
            'High': safe_float_convert(latest_data.get('High')),
            'Low': safe_float_convert(latest_data.get('Low')),
            'Close': safe_float_convert(latest_data.get('Close')),
            'Volume': safe_float_convert(latest_data.get('Volume')),
            'RSI': safe_float_convert(history['RSI'].iloc[-1], 50.0),
            'EMA': safe_float_convert(history['EMA'].iloc[-1]),
            'MACD.macd': safe_float_convert(history['MACD'].iloc[-1]),
            'MACD.signal': safe_float_convert(history['MACD_Signal'].iloc[-1]),
            # Diğer göstergeler yfinance'da bulunmadığı için varsayılan değerler
            'ADX': 25.0,
            'CCI': 0.0,
            'BB.upper': latest_data.get('Close') * 1.02,
            'BB.lower': latest_data.get('Close') * 0.98,
            'BB.middle': latest_data.get('Close'),
            'summary': 'NEUTRAL',
            'oscillators': 'NEUTRAL',
            'moving_averages': 'NEUTRAL'
        }
        
        return result
        
    except Exception as e:
        error_msg = str(e)
        if "No timezone found" in error_msg:
            logger.warning(f"Sembol bulunamadı veya veri çekilemedi: {yf_symbol}")
        else:
            logger.error(f"Yahoo Finance genel hata {yf_symbol} - {timeframe}: {error_msg}")
        return None

async def fetch_data_with_retry(symbol: str, timeframe: str, max_retries: int = MAX_RETRY_ATTEMPTS) -> Optional[Dict[str, Any]]:
    """Retry mekanizmalı veri çekme (Yahoo Finance için güncellendi)"""
    symbol_key = f"{symbol}_{timeframe}"
    
    if symbol_key in FAILED_SYMBOLS:
        import random
        if random.randint(1, 10) != 1:
            return None
    
    for attempt in range(max_retries):
        try:
            result = await fetch_yfinance_data(symbol, timeframe)
            if result and validate_data_integrity_enhanced(result, symbol):
                FAILED_SYMBOLS.discard(symbol_key)
                return result
            else:
                logger.debug(f"Veri doğrulama başarısız - {symbol} (Deneme {attempt + 1})")
        except Exception as e:
            logger.warning(f"Veri çekme denemesi {attempt + 1}/{max_retries} başarısız - {symbol}: {e}")
            if attempt < max_retries - 1:
                wait_time = min(30, (2 ** attempt) * 1.5)
                await asyncio.sleep(wait_time)
    
    FAILED_SYMBOLS.add(symbol_key)
    logger.error(f"Yahoo Finance'dan {symbol} için {max_retries} denemede veri çekilemedi.")
    return None

def validate_data_integrity_enhanced(data: Dict[str, Any], symbol: str) -> bool:
    """Geliştirilmiş veri bütünlüğü kontrolü (mevcut haliyle bırakıldı)"""
    try:
        required_fields = ['Close', 'Volume', 'RSI']
        
        for field in required_fields:
            if field not in data or data[field] is None:
                logger.debug(f"Eksik alan {symbol}: {field}")
                return False
                
            try:
                value = float(data[field])
                
                if field == 'Close' and (value <= 0 or value > 20000):
                    logger.debug(f"Mantıksız fiyat {symbol}: {value}")
                    return False
                elif field == 'Volume' and value < 0:
                    logger.debug(f"Negatif hacim {symbol}: {value}")
                    return False
                elif field == 'RSI' and (value < 0 or value > 100):
                    logger.debug(f"RSI aralık dışı {symbol}: {value}")
                    return False
            except (ValueError, TypeError):
                logger.debug(f"Sayıya dönüştürülemeyen değer {symbol} - {field}: {data[field]}")
                return False
        
        high = float(data.get('High', data['Close']))
        low = float(data.get('Low', data['Close']))
        close = float(data['Close'])
        
        if high < close or low > close:
            logger.debug(f"OHLC mantık hatası {symbol}: H={high}, L={low}, C={close}")
            return False
            
        return True
        
    except Exception as e:
        logger.warning(f"Veri doğrulama hatası {symbol}: {e}")
        return False
    
def safe_float_convert(value, default_value=None):
    """Değeri güvenli bir şekilde float'a dönüştürür. Hata durumunda varsayılan değeri döner."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default_value

async def test_yfinance_connectivity():
    """Yahoo Finance bağlantısını test eder."""
    test_symbols = ["AKBNK", "GARAN", "ISCTR"]
    
    logger.info("Yahoo Finance bağlantısı test ediliyor...")
    
    for symbol in test_symbols:
        try:
            data = await fetch_yfinance_data(symbol, "1d")
            if data and data.get('Close'):
                logger.info(f"Yahoo Finance test başarılı: {symbol} = {data['Close']:.3f} TL")
                return True
        except Exception as e:
            logger.warning(f"Yahoo Finance test hatası {symbol}: {e}")
    
    logger.error("Yahoo Finance bağlantı testi başarısız!")
    return False

import logging
from typing import Tuple

logger = logging.getLogger(__name__)

def calculate_volume_metrics(last_volume: float, symbol: str) -> Tuple[float, float]:
    """
    Belirtilen hisse için hacim oranını ve ortalama hacmi hesaplar.
    """
    try:
        # Daha iyi bir yaklaşım için, hissenin geçmiş verilerini kullanarak
        # ortalama hacmi dinamik olarak hesaplayabilirsiniz.
        avg_volume = 1_000_000.0  # Şimdilik sabit bir değer
        
        volume_ratio = last_volume / avg_volume if avg_volume > 0 else 0.0
        
        return (volume_ratio, avg_volume)
    except Exception as e:
        logger.error(f"Hacim metriği hesaplanırken hata oluştu: {e}")
        return (0.0, 0.0)

# ----------------------- ANA ANALİZ FONKSİYONU -----------------------
async def fetch_and_analyze_data(session: aiohttp.ClientSession, symbol: str, timeframe: str) -> Tuple[Optional["SignalInfo"], Optional[pd.DataFrame], Optional[Dict[str, Any]]]:
    """Geliştirilmiş hisse senedi analizi (Yahoo Finance entegrasyonlu)"""
    try:
        # Başarısız sembolları atla
        symbol_key = f"{symbol}_{timeframe}"
        if symbol_key in FAILED_SYMBOLS:
            return None, None, None

        # Yahoo Finance'dan veriyi çek
        data = await fetch_yfinance_data(symbol, timeframe)
        if not data:
            return None, None, None

        # Temel filtreler
        last_close = data['Close']
        last_volume = data['Volume']
        
        # Minimum fiyat ve hacim kontrolü
        if last_close < MIN_PRICE:
            logger.debug(f"{symbol} minimum fiyatın altında: {last_close}")
            return None, None, None
            
        volume_try = last_close * last_volume
        if volume_try < MIN_VOLUME_TRY:
            logger.debug(f"{symbol} minimum hacim altında: {volume_try:,.0f} TL")
            return None, None, None

        # RSI ve diğer indikatörleri al
        rsi_value = data['RSI']
        ema_value = data['EMA']
        
        # Hacim metriklerini hesapla
        volume_ratio, avg_volume = calculate_volume_metrics(last_volume, symbol)
        
        if volume_ratio < MIN_VOLUME_RATIO:
            logger.debug(f"{symbol} hacim oranı düşük: {volume_ratio:.2f}")
            return None, None, None

        # Sinyal yönünü belirle (basit ve net mantık)
        direction = None
        
        # Bullish sinyaller
        if (rsi_value < 45 and last_close > ema_value):
            direction = "BULLISH"
        
        # Bearish sinyaller
        elif (rsi_value > 55 and last_close < ema_value):
            direction = "BEARISH"

        if direction is None:
            return None, None, None

        # Sinyal nesnesini oluştur
        signal = SignalInfo(
            symbol=symbol,
            timeframe=timeframe,
            direction=direction,
            price=last_close,
            rsi=rsi_value,
            rsi_ema=ema_value,
            volume_ratio=volume_ratio,
            volume_try=volume_try,
            strength_score=0.0,
            timestamp=dt.datetime.now(IST_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            trend_break=None, # Bu alan için yeni bir mantık eklenmedi
            confidence=0.0
        )

        # Güç skorunu ve güven seviyesini hesapla
        signal.strength_score = calculate_signal_strength(signal)
        signal.confidence = calculate_confidence_level(signal)
        
        # Minimum skor kontrolü
        if signal.strength_score < MIN_SIGNAL_SCORE:
            logger.debug(f"{symbol} sinyal gücü yetersiz: {signal.strength_score:.1f}")
            return None, None, None

        # İndikatör verilerini hazırla (OHLCV bilgisi yok, sadece indikatörler var)
        indicators = {
            'rsi': rsi_value,
            'rsi_ema': ema_value,
            'volume_ratio': volume_ratio
        }

        # DataFrame yerine sadece indikatör verilerini döndür
        logger.info(f"🔍 Analiz tamamlandı - {symbol} ({timeframe}): RSI={rsi_value:.1f}, Güç={signal.strength_score:.1f}, Güven={signal.confidence:.2f}")
        return signal, None, indicators
    
    except Exception as e:
        logger.error(f"{symbol} analiz hatası: {e}")
        FAILED_SYMBOLS.add(f"{symbol}_{timeframe}")
        return None, None, None

# ----------------------- TELEGRAM FONKSİYONLARI -----------------------
async def send_telegram_with_retry(text: str, max_retries: int = 3):
    """Retry mekanizmalı Telegram mesajı gönder"""
    for attempt in range(max_retries):
        try:
            await send_telegram(text)
            return True
        except Exception as e:
            logger.warning(f"Telegram gönderme denemesi {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2 * (attempt + 1))
    
    logger.error(f"Telegram mesajı {max_retries} denemede gönderilemedi")
    return False

async def send_telegram(text: str):
    """Telegram mesajı gönder"""
    payload = {
        "chat_id": TELEGRAM_CHAT_ID, 
        "text": text, 
        "parse_mode": "HTML", 
        "disable_web_page_preview": True
    }
    
    try:
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(f"{TELEGRAM_API_URL}/sendMessage", json=payload) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.warning(f"Telegram API hatası: {response.status} - {error_text}")
                    raise Exception(f"HTTP {response.status}")
                else:
                    logger.debug("Telegram mesajı başarıyla gönderildi")
    except asyncio.TimeoutError:
        logger.error("Telegram API timeout")
        raise
    except Exception as e:
        logger.error(f"Telegram gönderme hatası: {e}")
        raise

async def send_chart_to_telegram(token: str, chat_id: str, title: str, df: pd.DataFrame, ind: Dict[str, Any]):
    """Geliştirilmiş grafik gönderme"""
    try:
        matplotlib.use("Agg")
        plt.style.use('dark_background')
        
        fig, (ax_price, ax_rsi, ax_volume) = plt.subplots(3, 1, figsize=(14, 10), 
                                                         gridspec_kw={'height_ratios': [3, 2, 1]})
        
        # Zaman serisi için x ekseni (tek nokta olduğu için basit)
        xs = [0]
        
        # Fiyat grafiği
        closes = [df['Close'].iloc[0]]
        highs = [df['High'].iloc[0]]
        lows = [df['Low'].iloc[0]]
        
        ax_price.plot(xs, closes, 'o-', color='cyan', linewidth=3, markersize=8, label='Kapanış')
        ax_price.plot(xs, highs, '^', color='lime', markersize=6, label='Yüksek')
        ax_price.plot(xs, lows, 'v', color='red', markersize=6, label='Düşük')
        
        ax_price.set_title(f"{title} - Fiyat Analizi", fontsize=16, fontweight='bold', color='white')
        ax_price.grid(True, alpha=0.3)
        ax_price.legend(loc='upper left')
        ax_price.set_ylabel('Fiyat (TL)', color='white')
        
        # RSI grafiği
        if 'rsi' in ind and 'rsi_ema' in ind:
            rsi_vals = [ind['rsi'].iloc[0]]
            rsi_ema_vals = [ind['rsi_ema'].iloc[0]]
            
            ax_rsi.plot(xs, rsi_vals, 'o-', color='orange', linewidth=3, markersize=8, label=f'RSI ({rsi_vals[0]:.1f})')
            ax_rsi.axhline(70, color='red', linestyle='--', alpha=0.8, label='Aşırı Alım (70)')
            ax_rsi.axhline(30, color='lime', linestyle='--', alpha=0.8, label='Aşırı Satım (30)')
            ax_rsi.axhline(50, color='gray', linestyle='-', alpha=0.5, label='Orta Hat')
            
            # RSI seviye renklemesi
            rsi_color = 'red' if rsi_vals[0] > 70 else 'lime' if rsi_vals[0] < 30 else 'orange'
            ax_rsi.fill_between(xs, [0], rsi_vals, alpha=0.3, color=rsi_color)
            
            ax_rsi.set_ylim(0, 100)
            ax_rsi.set_ylabel('RSI', color='white')
            ax_rsi.grid(True, alpha=0.3)
            ax_rsi.legend(loc='upper left')
        
        # Hacim grafiği
        volumes = [df['Volume'].iloc[0]]
        volume_ratio = ind.get('volume_ratio', 1.0)
        
        volume_color = 'lime' if volume_ratio > 2.0 else 'orange' if volume_ratio > 1.5 else 'red'
        ax_volume.bar(xs, volumes, color=volume_color, alpha=0.7, 
                     label=f'Hacim Oranı: {volume_ratio:.1f}x')
        
        ax_volume.set_ylabel('Hacim', color='white')
        ax_volume.legend(loc='upper left')
        ax_volume.grid(True, alpha=0.3)
        
        # Genel görünüm ayarları
        plt.tight_layout()
        
        # Grafik kaydet ve gönder
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight', 
                   facecolor='black', edgecolor='none')
        plt.close(fig)
        buf.seek(0)

        url = f"https://api.telegram.org/bot{token}/sendPhoto"
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            form = aiohttp.FormData()
            form.add_field('chat_id', chat_id)
            form.add_field('photo', buf, filename=f'{title}_chart.png', content_type='image/png')
            
            async with session.post(url, data=form) as response:
                if response.status != 200:
                    logger.error(f"Grafik gönderme hatası: {response.status}")

    except Exception as e:
        logger.error(f"Grafik oluşturma/gönderme hatası: {e}")

async def send_signal_with_chart(sig: SignalInfo, df: pd.DataFrame, ind: Dict[str, Any]):
    """Geliştirilmiş sinyal mesajı"""
    try:
        # Emoji seçimi
        direction_emoji = "🚀" if sig.direction == "BULLISH" else "📉"
        strength_emoji = "💪" if sig.strength_score >= 8 else "👍" if sig.strength_score >= 6 else "👌"
        confidence_emoji = "🔥" if sig.confidence >= 0.8 else "⭐" if sig.confidence >= 0.6 else "✨"
        
        # Ana mesaj
        message = f"<b>{direction_emoji} CAKMAUSTAD SİNYALİ - {sig.timeframe}</b>\n\n"
        message += f"<b>📊 {sig.symbol}.IS</b>\n"
        message += f"<b>🎯 Yön:</b> {sig.direction}\n"
        message += f"<b>{strength_emoji} Güç:</b> {sig.strength_score:.1f}/10.0\n"
        message += f"<b>{confidence_emoji} Güven:</b> {sig.confidence*100:.0f}%\n"
        message += f"<b>💰 Fiyat:</b> {sig.price:.3f} TL\n"
        message += f"<b>📈 RSI:</b> {sig.rsi:.1f}\n"
        message += f"<b>📊 EMA:</b> {sig.rsi_ema:.2f}\n"
        message += f"<b>🔊 Hacim:</b> {sig.volume_try / 1_000_000:.2f}M TL ({sig.volume_ratio:.1f}x)\n"
        
        if sig.trend_break:
            message += f"<b>⚡ Trend Kırılımı:</b> {sig.trend_break}\n"      
        message += f"\n<b>🕐 Zaman:</b> {sig.timestamp}"
        message += f"\n<b>🏷️ #{sig.symbol} #{sig.timeframe} #{sig.direction}</b>"
        
        # Önce mesajı gönder
        await send_telegram_with_retry(message)
        
        # Sonra grafiği gönder
        await send_chart_to_telegram(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, sig.symbol, df, ind)
        
        logger.info(f"📤 Sinyal gönderildi: {sig.symbol} - {sig.direction} - Güç: {sig.strength_score:.1f}")
        
    except Exception as e:
        logger.error(f"Sinyal gönderme hatası: {e}")
        # En azından temel mesajı göndermeyi dene
        try:
            basic_message = f"🚨 SİNYAL: {sig.symbol} - {sig.direction} - Güç: {sig.strength_score:.1f}"
            await send_telegram(basic_message)
        except:
            logger.error("Temel sinyal mesajı da gönderilemedi")

# ----------------------- ANA DÖNGÜ -----------------------
async def scan_and_report():
    """Geliştirilmiş tarama ve raporlama"""
    global LAST_SCAN_TIME, DAILY_SIGNALS
    
    start_time = time.time()
    now_ist = dt.datetime.now(IST_TZ)
    logger.info(f"⏳ Tarama başlatılıyor... ({len(TICKERS)} hisse x {len(TIMEFRAMES)} zaman dilimi)")

    # Eski sinyalleri temizle
    cleanup_old_signals()

    if not is_market_hours():
        logger.info(f"⌚ Borsa kapalı ({now_ist.strftime('%H:%M')}), tarama atlandı")
        return

    # İstatistikler
    total_tasks = len(TICKERS) * len(TIMEFRAMES)
    processed_count = 0
    error_count = 0
    signal_count = 0

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
        connector=aiohttp.TCPConnector(limit=20, limit_per_host=10)
    ) as session:
        
        found_signals: Set[str] = set()
        tasks = []

        # Görevleri oluştur
        for symbol in TICKERS:
            for tf in TIMEFRAMES:
                if f"{symbol}_{tf}" not in FAILED_SYMBOLS:  # Başarısız olanları atla
                    task = asyncio.create_task(
                        fetch_and_analyze_data(session, symbol, tf)
                    )
                    tasks.append((task, symbol, tf))

        # Batch olarak işle (aşırı yüklenmeyi önle)
        batch_size = 50
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            batch_tasks = [task[0] for task in batch]
            
            logger.info(f"📊 Batch {i//batch_size + 1}/{(len(tasks) + batch_size - 1)//batch_size} işleniyor...")
            
            try:
                results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                for j, result in enumerate(results):
                    _, symbol, tf = batch[j]
                    processed_count += 1
                    
                    if isinstance(result, Exception):
                        logger.error(f"❌ Görev hatası {symbol}_{tf}: {result}")
                        error_count += 1
                        FAILED_SYMBOLS.add(f"{symbol}_{tf}")
                        continue

                    signal, df, ind = result
                    
                    if signal:
                        signal_key = f"{signal.symbol}_{signal.timeframe}"
                        
                        if signal_key not in found_signals and signal_key not in DAILY_SIGNALS:
                            found_signals.add(signal_key)
                            DAILY_SIGNALS[signal_key] = asdict(signal)
                            
                            # Sinyali gönder
                            await send_signal_with_chart(signal, df, ind)
                            signal_count += 1
                            
                            logger.info(f"🎯 YENİ SİNYAL: {signal.symbol} ({signal.timeframe}) - "
                                      f"{signal.direction} - Güç: {signal.strength_score:.1f} - "
                                      f"Güven: {signal.confidence*100:.0f}%")
                
                # Batch'ler arası kısa bekleme
                if i + batch_size < len(tasks):
                    await asyncio.sleep(1)
                    
            except Exception as e:
                logger.error(f"Batch işleme hatası: {e}")
                error_count += len(batch_tasks)

    # Sonuç raporu
    elapsed_time = time.time() - start_time
    success_rate = ((processed_count - error_count) / max(processed_count, 1)) * 100
    
    summary_message = (
        f"✅ Tarama tamamlandı!\n"
        f"📊 İşlenen: {processed_count}/{total_tasks}\n"
        f"🎯 Bulunan sinyal: {signal_count}\n"
        f"❌ Hata: {error_count}\n"
        f"📈 Başarı oranı: {success_rate:.1f}%\n"
        f"⏱️ Süre: {elapsed_time:.1f} saniye\n"
        f"🔄 Toplam günlük sinyal: {len(DAILY_SIGNALS)}"
    )
    
    logger.info(summary_message.replace('\n', ' | '))
    
    # Özet mesajını sadece sinyal varsa gönder
    if signal_count > 0:
        await send_telegram_with_retry(f"📋 <b>Tarama Özeti</b>\n{summary_message}")
    
    LAST_SCAN_TIME = now_ist

async def send_daily_summary():
    """Günlük özet raporu"""
    try:
        now = dt.datetime.now(IST_TZ)
        if now.hour == 18 and now.minute <= CHECK_EVERY_MIN:  # Borsa kapanışında
            if DAILY_SIGNALS:
                total_signals = len(DAILY_SIGNALS)
                bullish_signals = sum(1 for s in DAILY_SIGNALS.values() if s['direction'] == 'BULLISH')
                bearish_signals = total_signals - bullish_signals
                
                avg_strength = sum(s['strength_score'] for s in DAILY_SIGNALS.values()) / total_signals
                avg_confidence = sum(s['confidence'] for s in DAILY_SIGNALS.values()) / total_signals
                
                summary = (
                    f"📈 <b>Günlük Sinyal Özeti</b> 📉\n\n"
                    f"📊 Toplam Sinyal: {total_signals}\n"
                    f"🚀 Bullish: {bullish_signals}\n"
                    f"📉 Bearish: {bearish_signals}\n"
                    f"💪 Ortalama Güç: {avg_strength:.1f}/10\n"
                    f"⭐ Ortalama Güven: {avg_confidence*100:.1f}%\n"
                    f"🕐 Tarih: {now.strftime('%Y-%m-%d')}\n\n"
                    f"#GünlükÖzet #BIST #CakmaUstad"
                )
                
                await send_telegram_with_retry(summary)
                logger.info("📋 Günlük özet gönderildi")
    except Exception as e:
        logger.error(f"Günlük özet hatası: {e}")

async def run_scanner_periodically():
    """Periyodik tarama çalıştırıcı"""
    logger.info(f"🤖 CakmaUstad bot başlatıldı! Tarama aralığı: {CHECK_EVERY_MIN} dakika")
    
    # Başlangıç mesajı
    start_message = (
        f"🤖 <b>CakmaUstad Bot Başlatıldı!</b>\n\n"
        f"📊 İzlenen hisse: {len(TICKERS)}\n"
        f"⏰ Tarama aralığı: {CHECK_EVERY_MIN} dakika\n"
        f"📈 Zaman dilimleri: {', '.join(TIMEFRAMES)}\n"
        f"🎯 Min. sinyal gücü: {MIN_SIGNAL_SCORE}\n"
        f"💰 Min. fiyat: {MIN_PRICE} TL\n"
        f"🔊 Min. hacim: {MIN_VOLUME_TRY:,.0f} TL\n"
        f"🚀 Hazırım!"
    )
    
    await send_telegram_with_retry(start_message)
    
    while True:
        try:
            # Ana taramayı çalıştır
            await scan_and_report()
            
            # Günlük özet kontrolü
            await send_daily_summary()
            
            # Sonraki taramaya kadar bekle
            logger.info(f"💤 {CHECK_EVERY_MIN} dakika bekleniyor...")
            await asyncio.sleep(CHECK_EVERY_MIN * 60)
            
        except KeyboardInterrupt:
            logger.info("👋 Bot kapatılıyor...")
            break
        except Exception as e:
            logger.error(f"🔥 Tarama döngü hatası: {e}")
            error_message = f"⚠️ <b>Bot Hatası!</b>\n\nHata: {str(e)[:200]}...\n🔄 60 saniye sonra yeniden denenecek"
            await send_telegram_with_retry(error_message)
            await asyncio.sleep(60)  # Hata durumunda kısa bekleme

    # Kapanış mesajı
    shutdown_message = "👋 <b>CakmaUstad Bot Kapatıldı</b>\n\nGörüşmek üzere! 🚀"
    try:
        await send_telegram(shutdown_message)
    except:
        pass

# ----------------------- SAĞLIK KONTROLÜ -----------------------
class HealthHandler(web.View):
    """Geliştirilmiş sağlık kontrolü"""
    async def get(self):
        uptime_seconds = int(time.time() - START_TIME)
        uptime_hours = uptime_seconds // 3600
        uptime_minutes = (uptime_seconds % 3600) // 60
        
        # Bellek kullanımı (basit)
        import psutil
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        
        response = {
            "status": "healthy",
            "service": "cakmaustad-rsi-scanner",
            "version": "2.0.0",
            "market_status": "AÇIK" if is_market_hours() else "KAPALI",
            "uptime": f"{uptime_hours}s {uptime_minutes}d",
            "uptime_seconds": uptime_seconds,
            "scanned_stocks": len(TICKERS),
            "timeframes": TIMEFRAMES,
            "daily_signals": len(DAILY_SIGNALS),
            "failed_symbols": len(FAILED_SYMBOLS),
            "last_scan": LAST_SCAN_TIME.isoformat() if LAST_SCAN_TIME else None,
            "memory_usage_mb": round(memory_mb, 2),
            "settings": {
                "check_interval_min": CHECK_EVERY_MIN,
                "min_signal_score": MIN_SIGNAL_SCORE,
                "min_price": MIN_PRICE,
                "min_volume_try": MIN_VOLUME_TRY,
                "concurrent_requests": CONCURRENT_REQUESTS
            },
            "timestamp": dt.datetime.now(IST_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
        }
        
        return web.json_response(response)

class StatsHandler(web.View):
    """İstatistik endpoint'i"""
    async def get(self):
        try:
            # Sinyal istatistikleri
            bullish_count = sum(1 for s in DAILY_SIGNALS.values() if s['direction'] == 'BULLISH')
            bearish_count = sum(1 for s in DAILY_SIGNALS.values() if s['direction'] == 'BEARISH')
            
            total_signals = len(DAILY_SIGNALS)
            if total_signals > 0:
                avg_strength = sum(s['strength_score'] for s in DAILY_SIGNALS.values()) / total_signals
                avg_confidence = sum(s['confidence'] for s in DAILY_SIGNALS.values()) / total_signals
                
                # Zaman dilimi dağılımı
                timeframe_dist = {}
                for signal in DAILY_SIGNALS.values():
                    tf = signal['timeframe']
                    timeframe_dist[tf] = timeframe_dist.get(tf, 0) + 1
            else:
                avg_strength = 0
                avg_confidence = 0
                timeframe_dist = {}
            
            # En aktif hisseler (sinyal sayısına göre)
            symbol_count = {}
            for signal in DAILY_SIGNALS.values():
                symbol = signal['symbol']
                symbol_count[symbol] = symbol_count.get(symbol, 0) + 1
            
            top_symbols = sorted(symbol_count.items(), key=lambda x: x[1], reverse=True)[:10]
            
            response = {
                "total_signals": total_signals,
                "signal_distribution": {
                    "bullish": bullish_count,
                    "bearish": bearish_count
                },
                "average_strength": round(avg_strength, 2),
                "average_confidence": round(avg_confidence * 100, 1),
                "timeframe_distribution": timeframe_dist,
                "top_symbols": dict(top_symbols),
                "failed_symbols_count": len(FAILED_SYMBOLS),
                "scan_settings": {
                    "total_stocks": len(TICKERS),
                    "timeframes": TIMEFRAMES,
                    "scan_interval": f"{CHECK_EVERY_MIN} minutes"
                },
                "last_update": dt.datetime.now(IST_TZ).isoformat()
            }
            
            return web.json_response(response)
            
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

class SignalsHandler(web.View):
    """Günlük sinyalleri listele"""
    async def get(self):
        try:
            # Query parametrelerini al
            limit = int(self.request.query.get('limit', 50))
            direction = self.request.query.get('direction', '').upper()
            min_strength = float(self.request.query.get('min_strength', 0))
            
            # Sinyalleri filtrele ve sırala
            signals = list(DAILY_SIGNALS.values())
            
            if direction and direction in ['BULLISH', 'BEARISH']:
                signals = [s for s in signals if s['direction'] == direction]
            
            if min_strength > 0:
                signals = [s for s in signals if s['strength_score'] >= min_strength]
            
            # Güç skoruna göre sırala
            signals.sort(key=lambda x: x['strength_score'], reverse=True)
            
            # Limit uygula
            signals = signals[:limit]
            
            response = {
                "signals": signals,
                "total_count": len(signals),
                "filters_applied": {
                    "direction": direction or "ALL",
                    "min_strength": min_strength,
                    "limit": limit
                },
                "generated_at": dt.datetime.now(IST_TZ).isoformat()
            }
            
            return web.json_response(response)
            
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

class ResetHandler(web.View):
    """Sinyalleri ve cache'i temizle"""
    async def post(self):
        try:
            global DAILY_SIGNALS, FAILED_SYMBOLS
            
            old_signals = len(DAILY_SIGNALS)
            old_failed = len(FAILED_SYMBOLS)
            
            DAILY_SIGNALS.clear()
            FAILED_SYMBOLS.clear()
            
            response = {
                "status": "success",
                "message": "Cache temizlendi",
                "cleared": {
                    "signals": old_signals,
                    "failed_symbols": old_failed
                },
                "timestamp": dt.datetime.now(IST_TZ).isoformat()
            }
            
            logger.info(f"🗑️ Cache temizlendi: {old_signals} sinyal, {old_failed} başarısız sembol")
            return web.json_response(response)
            
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

async def start_web_server():
    """Web server'ı başlat"""
    app = web.Application()
    
    # Route'ları ekle
    app.router.add_get('/health', HealthHandler)
    app.router.add_get('/stats', StatsHandler)
    app.router.add_get('/signals', SignalsHandler)
    app.router.add_post('/reset', ResetHandler)
    
    # CORS desteği (basit)
    async def cors_handler(request, handler):
        response = await handler(request)
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
        return response
    
    app.middlewares.append(cors_handler)
    
    # Server'ı başlat
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, '0.0.0.0', HEALTH_CHECK_PORT)
    await site.start()
    
    logger.info(f"🌐 Web server başlatıldı: http://0.0.0.0:{HEALTH_CHECK_PORT}")
    logger.info(f"📋 Endpoints:")
    logger.info(f"   GET  /health  - Sağlık kontrolü")
    logger.info(f"   GET  /stats   - İstatistikler")
    logger.info(f"   GET  /signals - Günlük sinyaller")
    logger.info(f"   POST /reset   - Cache temizle")

# ----------------------- ANA FONKSİYON -----------------------
async def main():
    """Ana uygulama"""
    try:
        logger.info("🚀 CakmaUstad RSI Scanner başlatılıyor...")
        
        # Ayarları logla
        logger.info(f"📊 Ayarlar:")
        logger.info(f"   Hisse sayısı: {len(TICKERS)}")
        logger.info(f"   Zaman dilimleri: {TIMEFRAMES}")
        logger.info(f"   Tarama aralığı: {CHECK_EVERY_MIN} dakika")
        logger.info(f"   Min sinyal gücü: {MIN_SIGNAL_SCORE}")
        logger.info(f"   Min fiyat: {MIN_PRICE} TL")
        logger.info(f"   Min hacim: {MIN_VOLUME_TRY:,} TL")
        logger.info(f"   Eşzamanlı istek: {CONCURRENT_REQUESTS}")
        
        # Web server ve tarayıcıyı eşzamanlı başlat
        await asyncio.gather(
            start_web_server(),
            run_scanner_periodically()
        )
        
    except KeyboardInterrupt:
        logger.info("👋 Uygulama kullanıcı tarafından sonlandırıldı")
    except Exception as e:
        logger.error(f"🔥 Kritik hata: {e}")
        # Acil durum mesajı
        try:
            emergency_msg = f"🚨 <b>KRİTİK HATA!</b>\n\nBot durdu: {str(e)[:200]}...\n\nLütfen kontrol edin!"
            await send_telegram(emergency_msg)
        except:
            pass
        raise
    finally:
        logger.info("🛑 CakmaUstad Bot kapatıldı")

if __name__ == '__main__':
    try:
        # Asyncio policy ayarla (Windows uyumluluğu için)
        if hasattr(asyncio, 'set_event_loop_policy'):
            if os.name == 'nt':  # Windows
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
        # Ana uygulamayı çalıştır
        asyncio.run(main())
        
    except KeyboardInterrupt:
        print("\n👋 Bot kapatılıyor...")
    except Exception as e:
        print(f"\n🔥 Başlatma hatası: {e}")
        sys.exit(1)
