#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CakmaUstad RSI Strategy - TradingView Entegrasyonlu
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
from dotenv import load_dotenv
from tradingview_ta import TA_Handler, Interval, Exchange

# Load .env
load_dotenv()

# --- AYARLAR VE GÜVENLİK ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
    raise ValueError("Ortam değişkenleri (TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID) ayarlanmalı!")

LOG_FILE = os.getenv('LOG_FILE', 'trading_bot.log')
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
HEALTH_CHECK_PORT = int(os.getenv("PORT", 8080))
IST_TZ = pytz.timezone('Europe/Istanbul')

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])
logger = logging.getLogger("cakmaustad_scanner")

# ----------------------- TARAMA AYARLARI -----------------------
MARKET_OPEN_HOUR = 9
MARKET_CLOSE_HOUR = 18
MARKET_CLOSE_MINUTE = 30
def is_market_hours() -> bool:
    now_ist = dt.datetime.now(IST_TZ)
    if now_ist.weekday() >= 5: return False
    market_open = dt.time(MARKET_OPEN_HOUR, 0)
    market_close = dt.time(MARKET_CLOSE_HOUR, MARKET_CLOSE_MINUTE)
    return market_open <= now_ist.time() <= market_close

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
    "BINHO"]

SCAN_MODE = os.getenv("SCAN_MODE", "ALL")
CUSTOM_TICKERS_STR = os.getenv("CUSTOM_TICKERS", "")
if SCAN_MODE == "CUSTOM" and CUSTOM_TICKERS_STR:
    TICKERS = [t.strip() for t in CUSTOM_TICKERS_STR.split(',')]
else:
    TICKERS = ALL_BIST_STOCKS

CHECK_EVERY_MIN = int(os.getenv("CHECK_EVERY_MIN", "15"))
TIMEFRAMES = [t.strip() for t in os.getenv("TIMEFRAMES", "").split(',') if t.strip()] or ["1h", "4h", "1d"]
MIN_PRICE = float(os.getenv("MIN_PRICE", "1.0"))
MIN_VOLUME_TRY = float(os.getenv("MIN_VOLUME_TRY", "1000000"))
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "2.0"))
RSI_LEN = int(os.getenv("RSI_LEN", "22"))
RSI_EMA_LEN = int(os.getenv("RSI_EMA", "66"))
PIVOT_PERIOD = int(os.getenv("PIVOT_PERIOD", "10"))
MIN_SIGNAL_SCORE = float(os.getenv("MIN_SIGNAL_SCORE", "5.0"))

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
    strength_score: float
    timestamp: str
    trend_break: Optional[str] = None

# ----------------------- CAKMAUSTAD RSI STRATEJİ FONKSİYONLARI -----------------------
def rma(series: pd.Series, length: int) -> pd.Series:
    alpha = 1.0 / length
    return series.ewm(alpha=alpha, adjust=False).mean()

def rsi_pine(close: pd.Series, length: int = 14) -> pd.Series:
    delta = close.diff()
    up = delta.where(delta > 0, 0.0)
    down = (-delta).where(delta < 0, 0.0)
    up_rma = rma(up, length)
    down_rma = rma(down, length)
    rs = up_rma / down_rma.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50)

def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()

def detect_trend_breakouts(rsi_series: pd.Series, pivot_period: int = 10) -> Dict[str, bool]:
    """Basitleştirilmiş trend kırılım tespiti"""
    if len(rsi_series) < pivot_period * 2:
        return {'bull_break': False, 'bear_break': False}
    
    bull_break = False
    bear_break = False
    
    # Son 20 mum için yüksek ve düşük değerler
    recent_rsi = rsi_series.tail(20)
    
    # Düşen trend (yüksekler düşüyor) - Bearish trend break
    if len(recent_rsi) >= 10:
        first_half = recent_rsi.iloc[:10].max()
        second_half = recent_rsi.iloc[10:].max()
        
        if first_half > second_half and recent_rsi.iloc[-1] > second_half:
            bull_break = True
    
    # Yükselen trend (düşükler yükseliyor) - Bullish trend break
    if len(recent_rsi) >= 10:
        first_half = recent_rsi.iloc[:10].min()
        second_half = recent_rsi.iloc[10:].min()
        
        if first_half < second_half and recent_rsi.iloc[-1] < second_half:
            bear_break = True
    
    return {'bull_break': bull_break, 'bear_break': bear_break}

def calculate_signal_strength(signal: SignalInfo) -> float:
    """CakmaUstad sinyal gücü hesaplama"""
    score = 0.0
    
    # RSI seviyesi
    if signal.direction == "BULLISH":
        if signal.rsi < 30:
            score += 4.0
        elif signal.rsi < 40:
            score += 3.0
        elif signal.rsi < 50:
            score += 2.0
    else:
        if signal.rsi > 70:
            score += 4.0
        elif signal.rsi > 60:
            score += 3.0
        elif signal.rsi > 50:
            score += 2.0
    
    # RSI-EMA pozisyonu
    if signal.direction == "BULLISH" and signal.rsi > signal.rsi_ema:
        score += 3.0
    elif signal.direction == "BEARISH" and signal.rsi < signal.rsi_ema:
        score += 3.0
    
    # Trend kırılımı
    if signal.trend_break:
        score += 3.0
    
    # Hacim gücü
    if signal.volume_ratio > 3.0:
        score += 2.0
    elif signal.volume_ratio > 2.0:
        score += 1.0
    
    # Zaman dilimi ağırlığı
    if signal.timeframe == '1h':
        score += 1.0
    elif signal.timeframe == '4h':
        score += 2.0
    elif signal.timeframe == '1d':
        score += 3.0
    
    return min(10.0, max(0.0, score))

# ----------------------- TRADINGVIEW VERİ ÇEKME FONKSİYONLARI -----------------------
def get_tradingview_interval(timeframe: str) -> Interval:
    """TradingView zaman dilimini Interval nesnesine dönüştürür"""
    if timeframe == "1h":
        return Interval.INTERVAL_1_HOUR
    elif timeframe == "4h":
        return Interval.INTERVAL_4_HOURS
    elif timeframe == "1d":
        return Interval.INTERVAL_1_DAY
    elif timeframe == "15m":
        return Interval.INTERVAL_15_MINUTES
    elif timeframe == "5m":
        return Interval.INTERVAL_5_MINUTES
    else:
        return Interval.INTERVAL_1_HOUR

async def fetch_tradingview_data(symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
    """
    TradingView'dan hisse senedi verisi çeker.
    """
    try:
        interval = get_tradingview_interval(timeframe)
        
        handler = TA_Handler(
            symbol=symbol,
            exchange="BIST",
            screener="turkey",
            interval=interval,
            timeout=10
        )
        
        analysis = handler.get_analysis()
        
        # TradingView verilerini işle
        indicators = analysis.indicators
        
        result = {
            'Open': indicators.get('open'),
            'High': indicators.get('high'),
            'Low': indicators.get('low'),
            'Close': indicators.get('close'),
            'Volume': indicators.get('volume'),
            'RSI': indicators.get('RSI'),
            'EMA': indicators.get('EMA50'),  # 50 periyotluk EMA
            'MACD.macd': indicators.get('MACD.macd'),
            'MACD.signal': indicators.get('MACD.signal'),
            'summary': analysis.summary
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Hata: TradingView'dan {symbol} için veri çekilemedi - {e}")
        return None

# ----------------------- ANA ANALİZ FONKSİYONU -----------------------
async def fetch_and_analyze_data(session: aiohttp.ClientSession, symbol: str, timeframe: str) -> Tuple[Optional["SignalInfo"], Optional[pd.DataFrame], Optional[Dict[str, Any]]]:
    """
    Hisse senedi için TradingView'dan veri çeker, analiz eder ve sinyal üretir.
    """
    try:
        # TradingView'dan veriyi çek
        data = await fetch_tradingview_data(symbol, timeframe)

        if data is None:
            return None, None, None

        # DataFrame oluştur
        df = pd.DataFrame([{
            'Open': data['Open'],
            'High': data['High'],
            'Low': data['Low'],
            'Close': data['Close'],
            'Volume': data['Volume']
        }])

        # Temel filtreler
        last_close = float(data['Close'])
        last_volume = float(data['Volume'])
        
        if last_close < MIN_PRICE or last_close * last_volume < MIN_VOLUME_TRY:
            return None, df, None

        # RSI değerlerini al
        rsi_value = float(data['RSI']) if data['RSI'] else 50
        ema_value = float(data['EMA']) if data['EMA'] else last_close
        
        # Hacim oranı hesaplama (basit bir yaklaşım)
        volume_ratio = 1.0  # TradingView'dan hacim oranı alamadığımız için varsayılan değer

        # Sinyal yönünü belirle
        direction = None
        if rsi_value < 40 and last_close > ema_value:
            direction = "BULLISH"
        elif rsi_value > 60 and last_close < ema_value:
            direction = "BEARISH"

        if direction is None:
            return None, df, None

        # Trend kırılımını tespit et
        trend_break = None
        if direction == "BULLISH" and rsi_value > 30 and rsi_value < 40:
            trend_break = "BULLISH_TREND_BREAK"
        elif direction == "BEARISH" and rsi_value > 60 and rsi_value < 70:
            trend_break = "BEARISH_TREND_BREAK"

        # Sinyal nesnesini oluştur
        signal = SignalInfo(
            symbol=symbol,
            timeframe=timeframe,
            direction=direction,
            price=last_close,
            rsi=rsi_value,
            rsi_ema=ema_value,  # EMA'yı RSI-EMA olarak kullanıyoruz
            volume_ratio=volume_ratio,
            volume_try=last_close * last_volume,
            strength_score=0.0,
            timestamp=str(dt.datetime.now(IST_TZ)),
            trend_break=trend_break
        )

        signal.strength_score = calculate_signal_strength(signal)
        
        if signal.strength_score < MIN_SIGNAL_SCORE:
            return None, df, None

        # İndikatör verilerini hazırla
        indicators = {
            'rsi': pd.Series([rsi_value]),
            'rsi_ema': pd.Series([ema_value])
        }

        return signal, df, indicators
    
    except Exception as e:
        logger.error(f"{symbol} analiz hatası: {e}")
        return None, None, None

# ----------------------- TELEGRAM FONKSİYONLARI -----------------------
async def send_telegram(text: str):
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{TELEGRAM_API_URL}/sendMessage", json=payload, timeout=15) as r:
                if r.status != 200:
                    logger.warning(f"Telegram failed: {r.status} - {await r.text()}")
    except Exception as e:
        logger.warning(f"Telegram error: {e}")

async def send_chart_to_telegram(token: str, chat_id: str, title: str, df: pd.DataFrame, ind: Dict[str, Any]):
    try:
        matplotlib.use("Agg")
        fig, (ax_price, ax_rsi) = plt.subplots(2, 1, figsize=(12, 8), gridspec_kw={'height_ratios': [3, 1]})

        # Fiyat grafiği
        xs = np.arange(len(df))
        closes = df['Close'].astype(float).values
        ax_price.plot(xs, closes, color='blue', linewidth=2, label='Fiyat')
        ax_price.set_title(f"{title}")
        ax_price.grid(True, alpha=0.3)
        ax_price.legend()

        # RSI grafiği
        if 'rsi' in ind and 'rsi_ema' in ind:
            rsi_vals = ind['rsi'].values
            rsi_ema_vals = ind['rsi_ema'].values
            
            ax_rsi.plot(xs, rsi_vals, color='orange', linewidth=2, label='RSI')
            ax_rsi.plot(xs, rsi_ema_vals, color='purple', linewidth=2, label='EMA')
            ax_rsi.axhline(70, color='red', linestyle='--', alpha=0.7, label='Aşırı Alım')
            ax_rsi.axhline(30, color='green', linestyle='--', alpha=0.7, label='Aşırı Satım')
            ax_rsi.set_ylim(0, 100)
            ax_rsi.grid(True, alpha=0.3)
            ax_rsi.legend()

        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)

        url = f"https://api.telegram.org/bot{token}/sendPhoto"
        async with aiohttp.ClientSession() as session:
            form = aiohttp.FormData()
            form.add_field('chat_id', chat_id)
            form.add_field('photo', buf, filename='chart.png', content_type='image/png')
            await session.post(url, data=form)

    except Exception as e:
        logger.error(f"Grafik gönderme hatası: {e}")

async def send_signal_with_chart(sig: SignalInfo, df: pd.DataFrame, ind: Dict[str, Any]):
    try:
        message = f"<b>🎯 CAKMAUSTAD SİNYALİ - {sig.timeframe}</b>\n\n"
        message += f"<b>{sig.symbol}.IS</b>\n"
        message += f"<b>Yön:</b> {sig.direction}\n"
        message += f"<b>Güç:</b> {sig.strength_score:.1f}/10\n"
        message += f"<b>Fiyat:</b> {sig.price:.2f} TL\n"
        message += f"<b>RSI:</b> {sig.rsi:.2f}\n"
        message += f"<b>EMA:</b> {sig.rsi_ema:.2f}\n"
        message += f"<b>Hacim:</b> {sig.volume_try / 1_000_000:.2f} Milyon TL\n"
        if sig.trend_break:
            message += f"<b>Trend Kırılımı:</b> {sig.trend_break}\n"
        message += f"\n<b>Zaman:</b> {sig.timestamp}"
        
        await send_telegram(message)
        await send_chart_to_telegram(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, sig.symbol, df, ind)
    except Exception as e:
        logger.error(f"Sinyal gönderme hatası: {e}")

# ----------------------- ANA DÖNGÜ -----------------------
async def scan_and_report():
    """
    Tüm hisseleri ve zaman dilimlerini tarar, sinyal üretir ve raporlar.
    """
    global LAST_SCAN_TIME, DAILY_SIGNALS
    
    now_ist = dt.datetime.now(IST_TZ)
    logger.info("⏳ Taramaya başlanıyor...")

    if not is_market_hours():
        logger.info("❌ Borsa kapalı, tarama yapılmadı.")
        DAILY_SIGNALS = {} # Günlük sinyalleri sıfırla
        return

    async with aiohttp.ClientSession() as session:
        tasks = []
        found_signals: Set[str] = set()

        for symbol in TICKERS:
            for tf in TIMEFRAMES:
                tasks.append(
                    asyncio.create_task(
                        fetch_and_analyze_data(session, symbol, tf)
                    )
                )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Görev hatası: {result}")
                continue

            signal, df, ind = result
            if signal and f"{signal.symbol}_{signal.timeframe}" not in found_signals:
                found_signals.add(f"{signal.symbol}_{signal.timeframe}")
                symbol_key = f"{signal.symbol}_{signal.timeframe}"
                if symbol_key not in DAILY_SIGNALS:
                    DAILY_SIGNALS[symbol_key] = asdict(signal)
                    await send_signal_with_chart(signal, df, ind)
                    logger.info(f"🎯 Sinyal: {signal.symbol} - {signal.timeframe} - Güç: {signal.strength_score:.1f}")
    
    logger.info(f"✅ Tarama tamamlandı. {len(found_signals)} sinyal bulundu.")

async def run_scanner_periodically():
    while True:
        try:
            await scan_and_report()
            await asyncio.sleep(CHECK_EVERY_MIN * 60)
        except Exception as e:
            logger.error(f"Tarama hatası: {e}")
            await asyncio.sleep(60)

# ----------------------- SAĞLIK KONTROLÜ -----------------------
class HealthHandler(web.View):
    async def get(self):
        response = {
            "status": "healthy",
            "service": "cakmaustad-scanner",
            "market_status": "AÇIK" if is_market_hours() else "KAPALI",
            "scanned_stocks": len(TICKERS),
            "timestamp": dt.datetime.now(IST_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            "daily_signals": len(DAILY_SIGNALS)
        }
        return web.json_response(response)

async def start_server():
    app = web.Application()
    app.router.add_get('/health', HealthHandler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', HEALTH_CHECK_PORT)
    await site.start()
    logger.info(f"✅ Sağlık kontrolü {HEALTH_CHECK_PORT} portunda başlatıldı.")

async def main():
    await asyncio.gather(start_server(), run_scanner_periodically())

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("CakmaUstad botu kapatılıyor.")
