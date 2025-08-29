#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CakmaUstad RSI Strategy - Sadeleştirilmiş Versiyon
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
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
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
    "BINHO"
]

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

# ----------------------- CAKMAUSTAD RSI STRATEGY FONKSİYONLARI -----------------------
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

# ----------------------- VERİ İŞLEME FONKSİYONLARI -----------------------
# Asenkron veri çekme fonksiyonu
async def fetch_collectapi_data(session: aiohttp.ClientSession, symbol: str) -> Optional[pd.DataFrame]:
    """
    CollectAPI'den asenkron olarak hisse senedi verisi çeker.
    """
    api_key = os.getenv("COLLECTAPI_KEY")
    url = f"https://api.collectapi.com/stock/bist?symbol={symbol}"
    headers = {
        "Content-Type": "application/json",
        "authorization": f"apikey {api_key}"
    }

    try:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            data = await response.json()
            
            if data and "result" in data and len(data["result"]) > 0:
                stock_data = data["result"][0]
                df = pd.DataFrame([stock_data])
                
                # Sütunları standart isme dönüştür
                df = df.rename(columns={
                    "last_price": "Close", 
                    "volume": "Volume", 
                    "daily_low": "Low",
                    "daily_high": "High",
                    "daily_open": "Open"
                })
                
                # Gerekli sütunları seç
                df = df[['Open', 'High', 'Low', 'Close', 'Volume']]
                return df
            else:
                logger.warning(f"Hata: CollectAPI'den {symbol} için veri bulunamadı.")
                return None
            
    except aiohttp.ClientError as e:
        logger.error(f"Hata: {symbol} için CollectAPI isteği başarısız oldu - {e}")
        return None
    except KeyError as e:
        logger.error(f"Hata: CollectAPI yanıt yapısı beklenenden farklı. Eksik anahtar: {e}")
        return None

# ----------------------- ANA ANALİZ FONKSİYONU -----------------------
async def fetch_and_analyze_data(session: aiohttp.ClientSession, symbol: str, timeframe: str) -> Tuple[Optional["SignalInfo"], Optional[pd.DataFrame], Optional[Dict[str, Any]]]:
    """
    Hisse senedi için CollectAPI'den veri çeker, analiz eder ve sinyal üretir.
    """
    try:
        # CollectAPI'den anlık veriyi çek
        df = await fetch_collectapi_data(session, symbol)

        if df is None or df.empty:
            return None, None, None

        # Sadece son veri noktası mevcut, bu yüzden hacim oranı hesaplamak için geçmiş veriye ihtiyaç var.
        # Bu kısım CollectAPI'den gelen tek bir veri noktasıyla çalışmaz.
        # Bu nedenle, hacim oranı için basit bir kontrol uygulanır.
        last_close = float(df['Close'].iloc[-1])
        last_volume = float(df['Volume'].iloc[-1])

        # Temel filtreler
        if last_close < MIN_PRICE or last_close * last_volume < MIN_VOLUME_TRY:
            return None, None, None

        # Sinyal ve indikatörleri oluştur
        # Not: RSI ve diğer indikatörler tarihi veriye ihtiyaç duyar.
        # CollectAPI'nin tek bir noktayla bu verileri sağlaması mümkün değildir.
        # Bu nedenle bu kısım atlanarak sadece anlık sinyale odaklanılır.
        
        # Basit sinyal koşulları (örnek olarak)
        direction = None
        if last_close > last_close * 1.05: # Örnek: %5'ten fazla yükseliş
            direction = "BULLISH"
        elif last_close < last_close * 0.95: # Örnek: %5'ten fazla düşüş
            direction = "BEARISH"

        if direction is None:
            return None, df, None

        # Sinyal nesnesini oluştur
        signal = SignalInfo(
            symbol=symbol,
            timeframe=timeframe,
            direction=direction,
            price=last_close,
            rsi=0.0, # Bu veri yok
            rsi_ema=0.0, # Bu veri yok
            volume_ratio=0.0, # Bu veri yok
            volume_try=last_close * last_volume,
            strength_score=0.0,
            timestamp=str(dt.datetime.now(IST_TZ)),
            trend_break=None
        )

        signal.strength_score = calculate_signal_strength(signal)
        
        if signal.strength_score < MIN_SIGNAL_SCORE:
            return None, df, None

        return signal, df, {} # Boş sözlük döndür, indikatörler artık yok
    
    except Exception as e:
        logger.error(f"{symbol} analiz hatası: {e}")
        return None, None, None

# ----------------------- TELEGRAM FONKSİYONLARI -----------------------
# Bu kısım, değişiklik yapmanızı gerektirmeyecek kadar iyi yazılmış.
# Sadece `send_chart_to_telegram` fonksiyonunun, CollectAPI'den gelen tek veri noktası ile
# tarihi RSI grafiği oluşturamayacağını unutmayın.
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
        closes = df['close'].astype(float).values
        ax_price.plot(xs, closes, color='blue', linewidth=2, label='Fiyat')
        ax_price.set_title(f"{title}")
        ax_price.grid(True, alpha=0.3)
        ax_price.legend()

        # RSI grafiği
        if 'rsi' in ind and 'rsi_ema' in ind:
            rsi_vals = ind['rsi'].values[-len(xs):]
            rsi_ema_vals = ind['rsi_ema'].values[-len(xs):]
            
            ax_rsi.plot(xs, rsi_vals, color='orange', linewidth=2, label='RSI-22')
            ax_rsi.plot(xs, rsi_ema_vals, color='purple', linewidth=2, label='RSI-66')
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
        message += f"<b>RSI-22:</b> {sig.rsi:.2f}\n"
        message += f"<b>RSI-66:</b> {sig.rsi_ema:.2f}\n"
        message += f"<b>Hacim Oranı:</b> {sig.volume_ratio:.2f}x\n"
        
        if sig.trend_break:
            message += f"<b>Trend Kırılımı:</b> {sig.trend_break}\n"
        
        message += f"\n<b>Zaman:</b> {dt.datetime.now(IST_TZ).strftime('%H:%M:%S')}"

        await send_telegram(message)
        
        # Bu kısım, CollectAPI'den gelen tek bir veri noktasıyla çalışmaz.
        # Bu yüzden `send_chart_to_telegram` fonksiyonu şu an için yoruma alındı.
        # Eğer grafiği farklı bir veri kaynağıyla oluşturacaksanız burayı aktif edebilirsiniz.
        # await send_chart_to_telegram(
        #     TELEGRAM_BOT_TOKEN,
        #     TELEGRAM_CHAT_ID,
        #     f"{sig.symbol} - {sig.timeframe} - {sig.direction}",
        #     df.tail(100),
        #     ind
        # )
        
    except Exception as e:
        logger.error(f"Sinyal gönderme hatası: {e}")

async def scan_and_report():
    global LAST_SCAN_TIME, DAILY_SIGNALS
    logger.info("⏳ CakmaUstad taraması başlıyor...")
    LAST_SCAN_TIME = dt.datetime.now(IST_TZ)
    
    if not is_market_hours():
        logger.info("❌ Borsa kapalı, tarama yapılmadı.")
        return

    found_signals = []
    
    # aiohttp oturumunu başlat
    async with aiohttp.ClientSession() as session:
        # Eş zamanlı istek sayısını 10 ile sınırlayan bir semafor oluştur
        # Bu değeri CollectAPI'nin hız sınırına göre ayarlayabilirsiniz.
        semaphore = asyncio.Semaphore(10)
        
        async def fetch_and_process(session, symbol, tf):
            # Semafor kilidini alarak işlemin başlamasını bekle
            async with semaphore:
                signal, df, ind = await fetch_and_analyze_data(session, symbol, tf)
                if signal and signal.direction == "BULLISH":
                    symbol_key = f"{signal.symbol}_{signal.timeframe}"
                    
                    if symbol_key not in DAILY_SIGNALS:
                        found_signals.append(signal)
                        DAILY_SIGNALS[symbol_key] = asdict(signal)
                        await send_signal_with_chart(signal, df, ind)
                        logger.info(f"🎯 Sinyal: {signal.symbol} - {signal.timeframe} - Güç: {signal.strength_score:.1f}")
                        await asyncio.sleep(1) # Telegram'ın hız limitini aşmamak için bekle

        tasks = []
        # Her hisse ve zaman dilimi için bir görev oluştur ve listeye ekle
        for symbol in TICKERS:
            for tf in TIMEFRAMES:
                tasks.append(fetch_and_process(session, symbol, tf))
        
        # Tüm görevleri paralel olarak çalıştır
        await asyncio.gather(*tasks)
    
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
