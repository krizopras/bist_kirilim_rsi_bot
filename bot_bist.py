#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CakmaUstad RSI Strategy - Yahoo Finance Entegrasyonlu (Tam Düzeltilmiş)
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

# Telegram API URL - DÜZELTME: Bu eksikti
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

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
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Rate limiting için semaphore
CONCURRENT_REQUESTS = int(os.getenv("CONCURRENT_REQUESTS", "10"))
request_semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

# --- ZAMAN DİLİMİ AYARI ---
IST_TZ = pytz.timezone('Europe/Istanbul')

# ----------------------- TARAMA AYARLARI -----------------------
# BIST Seans Saatleri
MARKET_OPEN = dt.time(9, 30)
MARKET_CLOSE = dt.time(18, 5) 
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

# Hisse listesi (kısaltıldı)
ALL_BIST_STOCKS = [
    "ADEL.IS", "ADESE.IS", "ADGYO.IS", "AFYON.IS", "AGHOL.IS", "AGESA.IS", "AGROT.IS", "AHSGY.IS", "AHGAZ.IS",
    "AKBNK.IS", "AKCNS.IS", "AKYHO.IS", "AKENR.IS", "AKFGY.IS", "AKFYE.IS", "ATEKS.IS", "AKSGY.IS",
    "AKSA.IS", "AKSEN.IS", "AKGRT.IS", "AKSUE.IS", "ALCAR.IS", "ALGYO.IS", "ALARK.IS", "ALBRK.IS", "ALKIM.IS", "ALKA.IS",
    "ALVES.IS", "ANSGR.IS", "AEFES.IS", "ANHYT.IS", "ASUZU.IS", "ANGEN.IS", "ANELE.IS", "ARCLK.IS", "ARDYZ.IS", "ARENA.IS",
    "ARMGD.IS", "ARSAN.IS", "ARTMS.IS", "ARZUM.IS", "ASGYO.IS", "ASELS.IS", "ASTOR.IS", "ATAGY.IS", "ATAKP.IS", "AGYO.IS",
    "ATSYH.IS", "ATLAS.IS", "ATATP.IS", "AVOD.IS", "AVGYO.IS", "AVTUR.IS", "AVHOL.IS", "AVPGY.IS", "AYDEM.IS", "AYEN.IS",
    "AYES.IS", "AYGAZ.IS", "AZTEK.IS", "A1CAP.IS", "BAGFS.IS", "BAHKM.IS", "BAKAB.IS", "BALAT.IS",
    "BALSU.IS", "BNTAS.IS", "BANVT.IS", "BARMA.IS", "BSRFK.IS", "BASGZ.IS", "BASCM.IS", "BEGYO.IS", "BTCIM.IS", "BSOKE.IS",
    "BYDNR.IS", "BAYRK.IS", "BERA.IS", "BRKT.IS", "BRKSN.IS", "BJKAS.IS", "BEYAZ.IS", "BIENY.IS", "BLCYT.IS",
    "BIMAS.IS", "BINBN.IS", "BIOEN.IS", "BRKVY.IS", "BRKO.IS", "BIGEN.IS", "BRLSM.IS", "BRMEN.IS", "BIZIM.IS",
    "BMSTL.IS", "BMSCH.IS", "BNPPI.IS", "BOBET.IS", "BORSK.IS", "BORLS.IS", "BRSAN.IS", "BRYAT.IS", "BFREN.IS", "BOSSA.IS",
    "BRISA.IS", "BURCE.IS", "BURVA.IS", "BUCIM.IS", "BVSAN.IS", "BIGCH.IS", "CRFSA.IS", "CASA.IS", "CEOEM.IS",
    "CCOLA.IS", "CONSE.IS", "COSMO.IS", "CRDFA.IS", "CVKMD.IS", "CWENE.IS",
    "CAGFA.IS", "CANTE.IS", "CATES.IS", "CLEBI.IS", "CELHA.IS", "CLKMT.IS", "CEMAS.IS", "CEMTS.IS", "CMBTN.IS",
    "CMENT.IS", "CIMSA.IS", "CUSAN.IS", "DVRLK.IS", "DAGI.IS", "DAPGM.IS", "DARDL.IS", "DGATE.IS", "DCTTR.IS",
    "DGRVK.IS", "DMSAS.IS", "DENGE.IS", "DZGYO.IS", "DENIZ.IS", 
    "DERIM.IS", "DERHL.IS", "DESA.IS", "DEVA.IS", "DNISI.IS", "DIRIT.IS",
    "DMRGD.IS", "DOCO.IS", "DOFER.IS", "DOBUR.IS", "DOHOL.IS", "DGNMO.IS", "ARASE.IS", "DOGUB.IS", "DGGYO.IS",
    "DOAS.IS",  "DOKTA.IS", "DURDO.IS",  "DYOBY.IS", "EBEBK.IS", "ECZYT.IS", "EDATA.IS",
    "EDIP.IS", "EFORC.IS", "EGEEN.IS", "EGGUB.IS", "EGPRO.IS", "EGSER.IS", "EPLAS.IS", "EGEGY.IS", "ECILC.IS", "EKER.IS",
    "EKIZ.IS", "EKOS.IS",  "EKSUN.IS", "ELITE.IS", "EMKEL.IS", "EMNIS.IS", 
    "DMLKT.IS", "EKGYO.IS", "ENJSA.IS", "ENERY.IS", "ENKAI.IS", "ENSRI.IS", "ERBOS.IS", "ERCB.IS",
    "EREGL.IS", "KIMMR.IS", "ERSU.IS", "ESCAR.IS", "ESCOM.IS", "ESEN.IS", "ETILR.IS", "EUKYO.IS", "EUYO.IS", "ETYAT.IS",
    "EUHOL.IS", "TEZOL.IS", "EUREN.IS", "EUPWR.IS", "EYGYO.IS", "FADE.IS", "FMIZP.IS", "FENER.IS",
    "FBBNK.IS", "FLAP.IS", "FONET.IS", "FROTO.IS", "FORMT.IS", "FORTE.IS", "FRIGO.IS", "FZLGY.IS", "GWIND.IS",
    "GSRAY.IS", "GARFA.IS", "GARFL.IS", "GRNYO.IS", "GEDIK.IS", "GEDZA.IS", "GLCVY.IS", "GENIL.IS", "GENTS.IS", "GEREL.IS",
    "GZNMI.IS", "GIPTA.IS", "GMTAS.IS", "GESAN.IS", "GLBMD.IS", "GLYHO.IS",  "GOODY.IS",
    "GOKNR.IS", "GOLTS.IS", "GOZDE.IS", "GRTHO.IS", "GSDDE.IS", "GSDHO.IS", "GUBRF.IS", "GLRYH.IS", "GLRMK.IS", "GUNDG.IS",
    "GRSEL.IS", "SAHOL.IS", "HALKF.IS", "HLGYO.IS", "HLVKS.IS", "HALKI.IS", "HRKET.IS", "HATEK.IS", "HATSN.IS",
    "HDFGS.IS", "HEDEF.IS", "HEKTS.IS",
    "HTTBT.IS", "HOROZ.IS", "HUBVC.IS", "HUNER.IS", "HUZFA.IS", "HURGZ.IS", "ENTRA.IS", "ICBCT.IS",
    "INGRM.IS", "INVEO.IS", "INVAZ.IS", "INVES.IS", "ISKPL.IS", "IEYHO.IS", "IDGYO.IS", "IHEVA.IS", "IHLGM.IS",
    "IHGZT.IS", "IHAAS.IS", "IHLAS.IS", "IHYAY.IS", "IMASM.IS", "INDES.IS", "INFO.IS", "INTEK.IS",
    "INTEM.IS", "IPEKE.IS", "ISDMR.IS", "ISTFK.IS", "ISFAK.IS", "ISFIN.IS", "ISGYO.IS", "ISGSY.IS", "ISMEN.IS",
    "ISYAT.IS", "ISBIR.IS", "ISSEN.IS", "IZINV.IS", "IZENR.IS", "IZMDC.IS", "IZFAS.IS", "JANTS.IS", "KFEIN.IS", "KLKIM.IS",
    "KLSER.IS",  "KAPLM.IS", "KRDMA.IS", "KRDMB.IS", "KRDMD.IS", "KAREL.IS", "KARSN.IS",
    "KRTEK.IS", "KARTN.IS",  "KTLEV.IS", "KATMR.IS", "KAYSE.IS", "KENT.IS", "KRVGD.IS", "KERVN.IS",
    "KZBGY.IS", "KLGYO.IS", "KLRHO.IS", "KMPUR.IS", "KLMSN.IS", "KCAER.IS",  "KOCFN.IS", "KCHOL.IS",
    "KOCMT.IS", "KLSYN.IS", "KNFRT.IS", "KONTR.IS", "KONYA.IS", "KONKA.IS", "KGYO.IS", "KORDS.IS", "KRPLS.IS",
    "KOTON.IS", "KOZAL.IS", "KOZAA.IS", "KOPOL.IS", "KRGYO.IS", "KRSTL.IS", "KRONT.IS", "KSTUR.IS",
    "KUVVA.IS", "KUYAS.IS", "KBORU.IS", "KZGYO.IS", "KUTPO.IS", "KTSKR.IS", "LIDER.IS", "LIDFA.IS", "LILAK.IS",
    "LMKDC.IS", "LINK.IS", "LOGO.IS", "LKMNH.IS", "LRSHO.IS", "LUKSK.IS", "LYDHO.IS", "LYDYE.IS", "MACKO.IS", "MAKIM.IS",
    "MAKTK.IS", "MANAS.IS", "MRBAS.IS", "MAGEN.IS", "MRMAG.IS", "MARKA.IS", "MAALT.IS", "MRSHL.IS", "MRGYO.IS",
    "MARTI.IS", "MTRKS.IS", "MAVI.IS", "MZHLD.IS", "MEDTR.IS", "MEGMT.IS", "MEGAP.IS", "MEKAG.IS", "MEKMD.IS", 
    "MNDRS.IS", "MEPET.IS", "MERCN.IS", "MERIT.IS", "MERKO.IS", "METUR.IS", "METRO.IS", "MTRYO.IS",
    "MHRGY.IS", "MIATK.IS", "MGROS.IS", "MSGYO.IS", "MSYBN.IS", "MPARK.IS", "MMCAS.IS", "MNGFA.IS", "MOBTL.IS",
    "MOGAN.IS", "MNDTR.IS", "MOPAS.IS", "EGEPO.IS", "NATEN.IS", "NTGAZ.IS", "NTHOL.IS", "NETAS.IS", "NIBAS.IS", "NUHCM.IS",
    "NUGYO.IS", "NRHOL.IS", "OBAMS.IS", "OBASE.IS", "ODAS.IS", "ODINE.IS",
    "OFSYM.IS", "ONCSM.IS", "ONRYT.IS", "OPET.IS", "ORCAY.IS", "ORFIN.IS", "ORGE.IS", "ORMA.IS", "OSMEN.IS",
    "OSTIM.IS", "OTKAR.IS", "OTTO.IS", "OYAKC.IS", "OYYAT.IS", "OYAYO.IS", "OYLUM.IS",
    "OZKGY.IS", "OZATD.IS", "OZGYO.IS", "OZRDN.IS", "OZSUB.IS", "OZYSR.IS", "PAMEL.IS", "PNLSN.IS", "PAGYO.IS", "PAPIL.IS",
    "PRFFK.IS", "PRDGS.IS", "PRKME.IS", "PASEU.IS", "PSGYO.IS", "PATEK.IS", "PCILT.IS",
    "PGSUS.IS", "PEKGY.IS", "PENGD.IS", "PENTA.IS", "PSDTC.IS", "PETKM.IS", "PKENT.IS", "PETUN.IS", "PINSU.IS", "PNSUT.IS",
    "PKART.IS", "PLTUR.IS", "POLHO.IS", "POLTK.IS", "PRZMA.IS",
    "QNBTR.IS", "QUAGR.IS", "QUFIN.IS", "RNPOL.IS", "RALYH.IS", "RAYSG.IS", "REEDR.IS",
    "RYGYO.IS", "RYSAS.IS", "RODRG.IS", "ROYAL.IS", "RGYAS.IS", "RTALB.IS", "RUBNS.IS", "RUZYE.IS", "SAFKR.IS", "SANEL.IS",
    "SNICA.IS", "SANFM.IS", "SANKO.IS", "SAMAT.IS", "SARKY.IS", "SARTN.IS", "SASA.IS", "SAYAS.IS", "SDTTR.IS", "SEGMN.IS",
    "SEKUR.IS", "SELEC.IS", "SELGD.IS", "SELVA.IS", "SNKRN.IS", "SRNT.IS", "SRVGY.IS", "SEYKM.IS", "SILVR.IS", "SNGYO.IS",
    "SKYLP.IS", "SMRTG.IS", "SMART.IS", "SODSN.IS", "SOKE.IS", "SKTAS.IS", "SONME.IS", "SNPAM.IS", "SUMAS.IS", "SUNTK.IS",
    "SURGY.IS", "SUWEN.IS", "SMRFA.IS", "SMRVA.IS", "SEKFK.IS", "SEGYO.IS", "SKYMD.IS", "SEK.IS",
    "SKBNK.IS", "SOKM.IS", "TABGD.IS",  "TNZTP.IS", "TARKM.IS", "TATGD.IS", "TATEN.IS",
    "TAVHL.IS", "DRPHN.IS", "TEBFA.IS", "TEKTU.IS", "TKFEN.IS", "TKNSA.IS", "TMPOL.IS", "TRFFA.IS", "DAGHL.IS",
    "TERA.IS", "TEHOL.IS", "TGSAS.IS",  "TOASO.IS",
    "TRGYO.IS", "TLMAN.IS", "TSPOR.IS", "TDGYO.IS", "TRMEN.IS", "TSGYO.IS", "TUCLK.IS", "TUKAS.IS", "TRCAS.IS",
    "TUREX.IS", "MARBL.IS", "TRKFN.IS", "TRILC.IS", "TCELL.IS", "TMSN.IS", "TUPRS.IS",
    "THYAO.IS", "PRKAB.IS", "TTKOM.IS", "TTRAK.IS", "TBORG.IS", "TURGG.IS", "GARAN.IS", "HALKB.IS",
    "EXIMB.IS", "ISCTR.IS", "ISKUR.IS", "KLNMA.IS", "TSKB.IS",
    "TURSG.IS", "SISE.IS", "VAKBN.IS", "UFUK.IS", "ULAS.IS", "ULUFA.IS", "ULUSE.IS", "ULUUN.IS",
    "UMPAS.IS", "USAK.IS", "ULKER.IS", "UNLU.IS", "VAKFN.IS", "VKGYO.IS", "VKFYO.IS", "VAKKO.IS",
    "VANGD.IS", "VBTYZ.IS",  "VRGYO.IS", "VERUS.IS", "VERTU.IS", "VESBE.IS", "VESTL.IS", "VKING.IS",
    "VSNMD.IS",  "YKBNK.IS", "YAPRK.IS", "YATAS.IS",
    "YFMEN.IS", "YATVK.IS", "YYLGD.IS", "YAYLA.IS", "YGGYO.IS", "YEOTK.IS", "YGYO.IS", "YYAPI.IS", "YESIL.IS",
    "YBTAS.IS", "YIGIT.IS", "YONGA.IS", "YKSLN.IS", "YUNSA.IS", "ZEDUR.IS", "ZRGYO.IS", "ZKBVK.IS", "ZKBVR.IS", "ZOREN.IS",
    "BINHO.IS"
]

if SCAN_MODE.upper() == "CUSTOM" and CUSTOM_TICKERS_STR:
    TICKERS = [t.strip() for t in CUSTOM_TICKERS_STR.split(',')]
else:
    TICKERS = ALL_BIST_STOCKS

CHECK_EVERY_MIN = int(os.getenv("CHECK_EVERY_MIN", "30"))
TIMEFRAMES = [t.strip() for t in os.getenv("TIMEFRAMES", "").split(',') if t.strip()] or ["1h", "1d"]
MIN_PRICE = float(os.getenv("MIN_PRICE", "1.0"))
MIN_VOLUME_TRY = float(os.getenv("MIN_VOLUME_TRY", "1000000"))
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "1.5"))
RSI_LEN = int(os.getenv("RSI_LEN", "14"))
RSI_EMA_LEN = int(os.getenv("RSI_EMA", "20"))
PIVOT_PERIOD = int(os.getenv("PIVOT_PERIOD", "10"))
MIN_SIGNAL_SCORE = float(os.getenv("MIN_SIGNAL_SCORE", "3.0"))
MAX_RETRY_ATTEMPTS = int(os.getenv("MAX_RETRY_ATTEMPTS", "2"))

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
        if value is None or value == '' or str(value).lower() in ['nan', 'none', 'null']:
            return default
        if pd.isna(value):
            return default
        return float(value)
    except (ValueError, TypeError):
        return default

# DÜZELTME: Bu fonksiyon eksikti
def calculate_volume_metrics(last_volume: float, symbol: str) -> Tuple[float, float]:
    """Hacim metriklerini hesapla - basit implementasyon"""
    try:
        # Gerçek bir botda, geçmiş hacim verisi kullanılmalı
        # Burada basit bir tahmin kullanıyoruz
        estimated_avg_volume = last_volume * 0.8 if last_volume > 0 else 1
        volume_ratio = last_volume / estimated_avg_volume if estimated_avg_volume > 0 else 1.0
        return max(0.1, volume_ratio), estimated_avg_volume
    except Exception as e:
        logger.debug(f"Hacim hesaplama hatası {symbol}: {e}")
        return 1.0, last_volume

# ----------------------- YAHOO FINANCE FONKSİYONLARI -----------------------
def get_yfinance_ticker(symbol: str) -> str:
    """BIST sembolünü Yahoo Finance formatına dönüştür"""
    if not symbol.endswith(".IS"):
        return f"{symbol}.IS"
    return symbol

# DÜZELTME: Timeframe mapping düzeltildi
def get_yfinance_interval(timeframe: str) -> str:
    """TradingView timeframe'ini yfinance interval'ına dönüştür"""
    mapping = {
        '15m': '15m',
        '1h': '1h',
        '4h': '1h',  # 4h yok, 1h kullan ve sonra 4h simüle et
        '1d': '1d',
        '1w': '1wk',
        '1M': '1mo'
    }
    return mapping.get(timeframe, '1h')

def calculate_rsi(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """RSI hesapla"""
    try:
        if len(df) < period + 1:
            return pd.Series([50.0] * len(df), index=df.index)
        
        delta = df['Close'].diff()
        gain = delta.where(delta > 0, 0.0)
        loss = (-delta).where(delta < 0, 0.0)
        
        # İlk değerler için simple mean
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi.fillna(50.0)
    except Exception as e:
        logger.warning(f"RSI hesaplama hatası: {e}")
        return pd.Series([50.0] * len(df), index=df.index)

def calculate_ema(series: pd.Series, period: int = 20) -> pd.Series:
    """EMA hesapla"""
    try:
        return series.ewm(span=period, adjust=False).mean()
    except Exception as e:
        logger.warning(f"EMA hesaplama hatası: {e}")
        return series

# Hafifletilmiş: Bellek dostu yfinance fetch
async def fetch_yfinance_data(symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
    """Yahoo Finance'dan sadece son barı çek (hafifletilmiş sürüm)"""
    yf_symbol = get_yfinance_ticker(symbol)
    yf_interval = get_yfinance_interval(timeframe)
    
    try:
        logger.debug(f"YF veri çekiliyor: {yf_symbol} ({yf_interval})")

        def fetch_data():
            ticker = yf.Ticker(yf_symbol)
            # Daha kısa period seç -> bellek tasarrufu
            period = "3mo" if timeframe == "1d" else "5d"
            hist = ticker.history(period=period, interval=yf_interval, timeout=10)

            # Fallback: eğer boş dönerse
            if hist.empty:
                logger.warning(f"Fallback: {yf_symbol} için 1mo/1d denenecek")
                hist = ticker.history(period="1mo", interval="1d", timeout=10)
            
            return hist

        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=2) as executor:
            history = await loop.run_in_executor(executor, fetch_data)

        # Veri kontrolü
        if history.empty or len(history) < 2:
            logger.debug(f"Yetersiz veri: {yf_symbol}")
            return None

        # Sadece son satırı al
        latest = history.iloc[-1]

        # RSI ve EMA hesaplamak için çok bar lazım -> küçük dilim
        sub_hist = history.tail(RSI_LEN + RSI_EMA_LEN + 5).copy()
        sub_hist['RSI'] = calculate_rsi(sub_hist, RSI_LEN)
        sub_hist['EMA'] = calculate_ema(sub_hist['Close'], RSI_EMA_LEN)

        latest_rsi = safe_float_convert(sub_hist['RSI'].iloc[-1], 50.0)
        latest_ema = safe_float_convert(sub_hist['EMA'].iloc[-1], latest['Close'])

        result = {
            'Close': safe_float_convert(latest['Close']),
            'Volume': safe_float_convert(latest['Volume']),
            'High': safe_float_convert(latest['High']),
            'Low': safe_float_convert(latest['Low']),
            'Open': safe_float_convert(latest['Open']),
            'RSI': latest_rsi,
            'EMA': latest_ema
        }

        # Validasyon
        if result['Close'] <= 0 or result['Volume'] < 0:
            return None

        return result

    except Exception as e:
        logger.debug(f"YF fetch hata {yf_symbol}: {str(e)[:100]}")
        return None


def validate_data(data: Dict[str, Any], symbol: str) -> bool:
    """Veri doğrulama"""
    try:
        required = ['Close', 'Volume', 'RSI']
        for field in required:
            if field not in data or data[field] is None:
                return False
            
            val = float(data[field])
            if field == 'Close' and (val <= 0 or val > 50000):
                return False
            elif field == 'Volume' and val < 0:
                return False
            elif field == 'RSI' and (val < 0 or val > 100):
                return False
                
        return True
    except:
        return False

# ----------------------- SİNYAL ANALİZİ -----------------------
def calculate_signal_strength(signal: SignalInfo) -> float:
    """Sinyal gücü hesapla"""
    score = 0.0
    
    # RSI seviye skoru
    if signal.direction == "BULLISH":
        if signal.rsi <= 30:
            score += 4.0
        elif signal.rsi <= 40:
            score += 3.0
        elif signal.rsi <= 45:
            score += 2.0
        elif signal.rsi <= 50:
            score += 1.0
    else:  # BEARISH
        if signal.rsi >= 70:
            score += 4.0
        elif signal.rsi >= 60:
            score += 3.0
        elif signal.rsi >= 55:
            score += 2.0
        elif signal.rsi >= 50:
            score += 1.0
    
    # Hacim bonusu
    if signal.volume_ratio >= 3.0:
        score += 2.0
    elif signal.volume_ratio >= 2.0:
        score += 1.5
    elif signal.volume_ratio >= 1.5:
        score += 1.0
    
    # Timeframe bonusu
    if signal.timeframe == "1d":
        score += 1.0
    
    # Fiyat kontrolü
    if signal.price >= 10.0:
        score += 0.5
    
    return min(10.0, max(0.0, score))

def calculate_confidence_level(signal: SignalInfo) -> float:
    """Güven seviyesi hesapla"""
    confidence = 0.5  # Base confidence
    
    if signal.strength_score >= 6.0:
        confidence += 0.3
    elif signal.strength_score >= 4.0:
        confidence += 0.2
    
    if signal.volume_ratio >= 2.0:
        confidence += 0.1
        
    if signal.timeframe == "1d":
        confidence += 0.1
    
    return min(1.0, confidence)

# ----------------------- ANA ANALİZ FONKSİYONU -----------------------
async def fetch_and_analyze_data(session: aiohttp.ClientSession, symbol: str, timeframe: str) -> Tuple[Optional[SignalInfo], Optional[pd.DataFrame], Optional[Dict[str, Any]]]:
    """Ana analiz fonksiyonu"""
    try:
        # Başarısız sembolleri atla
        symbol_key = f"{symbol}_{timeframe}"
        if symbol_key in FAILED_SYMBOLS:
            return None, None, None

        # Veri çek
        data = await fetch_yfinance_data(symbol, timeframe)
        if not data or not validate_data(data, symbol):
            FAILED_SYMBOLS.add(symbol_key)
            return None, None, None

        # Temel filtreler
        price = data['Close']
        volume = data['Volume']
        rsi = data['RSI']
        ema = data['EMA']
        
        # Minimum filtreler
        if price < MIN_PRICE:
            return None, None, None
            
        volume_try = price * volume
        if volume_try < MIN_VOLUME_TRY:
            return None, None, None

        # Hacim oranını hesapla
        volume_ratio, _ = calculate_volume_metrics(volume, symbol)
        if volume_ratio < MIN_VOLUME_RATIO:
            return None, None, None

        # Sinyal belirleme - Basitleştirilmiş mantık
        direction = None
        if rsi < 55 and price > ema:
            direction = "BULLISH"
        elif rsi > 75 and price < ema:
            direction = "BEARISH"

        if not direction:
            return None, None, None

        # Sinyal oluştur
        signal = SignalInfo(
            symbol=symbol,
            timeframe=timeframe,
            direction=direction,
            price=price,
            rsi=rsi,
            rsi_ema=ema,
            volume_ratio=volume_ratio,
            volume_try=volume_try,
            strength_score=0.0,
            timestamp=dt.datetime.now(IST_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            confidence=0.0
        )

        # Skor hesapla
        signal.strength_score = calculate_signal_strength(signal)
        signal.confidence = calculate_confidence_level(signal)
        
        # Minimum skor kontrolü
        if signal.strength_score < MIN_SIGNAL_SCORE:
            return None, None, None

        # DataFrame oluştur
        df_data = {
            'Open': [data['Open']],
            'High': [data['High']],
            'Low': [data['Low']],
            'Close': [data['Close']],
            'Volume': [data['Volume']]
        }
        df = pd.DataFrame(df_data)
        
        indicators = {
            'rsi': rsi,
            'rsi_ema': ema,
            'volume_ratio': volume_ratio
        }

        logger.info(f"✅ Sinyal: {symbol} {direction} RSI:{rsi:.1f} Güç:{signal.strength_score:.1f}")
        return signal, df, indicators
        
    except Exception as e:
        logger.error(f"Analiz hatası {symbol}: {e}")
        FAILED_SYMBOLS.add(f"{symbol}_{timeframe}")
        return None, None, None

# ----------------------- TELEGRAM FONKSİYONLARI -----------------------
async def send_telegram_with_retry(text: str, max_retries: int = 2) -> bool:
    """Telegram mesajı gönder"""
    for attempt in range(max_retries):
        try:
            await send_telegram(text)
            return True
        except Exception as e:
            logger.warning(f"Telegram deneme {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
    return False

async def send_telegram(text: str):
    """Telegram API çağrısı"""
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(f"{TELEGRAM_API_URL}/sendMessage", json=payload) as response:
            if response.status != 200:
                error_text = await response.text()
                raise Exception(f"HTTP {response.status}: {error_text[:100]}")

async def send_chart_to_telegram(token: str, chat_id: str, title: str, df: pd.DataFrame, ind: Dict[str, Any]):
    """Grafik gönder"""
    buf = None
    try:
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))
        
        # Fiyat grafiği
        ax1.plot([0], [df['Close'].iloc[0]], 'o-', color='cyan', markersize=10)
        ax1.set_title(f"{title} - Fiyat: {df['Close'].iloc[0]:.3f} TL")
        ax1.grid(True, alpha=0.3)
        
        # RSI grafiği
        rsi_val = ind['rsi']
        ax2.plot([0], [rsi_val], 'o-', color='orange', markersize=10)
        ax2.axhline(70, color='red', linestyle='--', alpha=0.7)
        ax2.axhline(30, color='green', linestyle='--', alpha=0.7)
        ax2.set_ylim(0, 100)
        ax2.set_title(f"RSI: {rsi_val:.1f}")
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
        plt.close()
        buf.seek(0)

        url = f"https://api.telegram.org/bot{token}/sendPhoto"
        
        async with aiohttp.ClientSession() as session:
            form = aiohttp.FormData()
            form.add_field('chat_id', chat_id)
            form.add_field('photo', buf, filename=f'{title}.png')
            
            async with session.post(url, data=form) as response:
                if response.status != 200:
                    logger.error(f"Grafik gönderme hatası: {response.status}")

    except Exception as e:
        logger.error(f"Grafik hatası: {e}")
    finally:
        if buf:
            buf.close()
        plt.clf()

async def send_signal_with_chart(sig: SignalInfo, df: pd.DataFrame, ind: Dict[str, Any]):
    """Sinyal mesajı gönder"""
    try:
        direction_emoji = "🚀" if sig.direction == "BULLISH" else "📉"
        
        message = f"<b>{direction_emoji} {sig.direction} SİNYAL</b>\n\n"
        message += f"<b>📊 {sig.symbol}</b>\n"
        message += f"<b>💰 Fiyat:</b> {sig.price:.3f} TL\n"
        message += f"<b>📈 RSI:</b> {sig.rsi:.1f}\n"
        message += f"<b>💪 Güç:</b> {sig.strength_score:.1f}/10\n"
        message += f"<b>⭐ Güven:</b> {sig.confidence*100:.0f}%\n"
        message += f"<b>📊 Hacim:</b> {sig.volume_try/1000000:.1f}M TL\n"
        message += f"<b>🕐 {sig.timestamp}</b>"
        
        # Mesajı gönder
        await send_telegram_with_retry(message)
        
        # Grafiği gönder
        if df is not None:
            await send_chart_to_telegram(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, sig.symbol, df, ind)
            
    except Exception as e:
        logger.error(f"Sinyal gönderme hatası: {e}")

# ----------------------- TARAMA DÖNGÜSÜ -----------------------
async def scan_and_report():
    """Ana tarama fonksiyonu"""
    global LAST_SCAN_TIME
    
    start_time = time.time()
    logger.info(f"🔍 Tarama başlıyor: {len(TICKERS)} hisse")
    
    cleanup_old_signals()
    
    # NOT: is_market_hours() kontrolü kaldırıldı.
    # Bot artık her zaman tarama yapacak.

    processed = 0
    errors = 0
    signals_found = 0

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=20),
        connector=aiohttp.TCPConnector(limit=10)
    ) as session:
        
        tasks = []
        for symbol in TICKERS:
            for tf in TIMEFRAMES:
                # Her istek arasında 0.5 saniye bekleme ekleyin
                await asyncio.sleep(0.5)
                tasks.append(asyncio.create_task(
                    fetch_and_analyze_data(session, symbol, tf)
                ))

        # Küçük batch'ler halinde işle
        batch_size = 5
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            
            try:
                results = await asyncio.gather(*batch, return_exceptions=True)
                
                for result in results:
                    processed += 1
                    
                    if isinstance(result, Exception):
                        errors += 1
                        continue
                        
                    signal, df, ind = result
                    if signal:
                        signal_key = f"{signal.symbol}_{signal.timeframe}"
                        
                        # Sinyal bulunduğunda yapılacak işlemler
                        
                        await send_signal_with_chart(signal, df, ind)
                        signals_found += 1
                
                # Batch arası bekleme
                if i + batch_size < len(tasks):
                    await asyncio.sleep(0.5)
                    
            except Exception as e:
                logger.error(f"Batch hatası: {e}")
                errors += batch_size

    # Sonuç raporu
    elapsed = time.time() - start_time
    success_rate = ((processed - errors) / max(processed, 1)) * 100
    
    summary = (
        f"✅ Tarama tamamlandı\n"
        f"📊 İşlenen: {processed}\n"
        f"🎯 Sinyal: {signals_found}\n"
        f"❌ Hata: {errors}\n"
        f"📈 Başarı: {success_rate:.1f}%\n"
        f"⏱️ Süre: {elapsed:.1f}s"
    )
    
    logger.info(summary.replace('\n', ' | '))
    
    if signals_found > 0:
        await send_telegram_with_retry(f"📋 <b>Tarama Özeti</b>\n{summary}")
    
    LAST_SCAN_TIME = dt.datetime.now(IST_TZ)

async def run_scanner_periodically():
    """Periyodik tarama"""
    logger.info(f"🤖 Bot başlatıldı! Her {CHECK_EVERY_MIN} dakikada tarama")
    
    start_msg = (
        f"🤖 <b>CakmaUstad Bot Başladı</b>\n\n"
        f"📊 Hisse: {len(TICKERS)}\n"
        f"⏰ Aralık: {CHECK_EVERY_MIN}dk\n"
        f"📈 Zaman: {', '.join(TIMEFRAMES)}\n"
        f"💪 Min güç: {MIN_SIGNAL_SCORE}\n"
        f"💰 Min fiyat: {MIN_PRICE} TL\n"
        f"🚀 Hazır!"
    )
    
    await send_telegram_with_retry(start_msg)
    
    while True:
        try:
            await scan_and_report()
            
            logger.info(f"💤 {CHECK_EVERY_MIN} dakika bekleniyor...")
            await asyncio.sleep(CHECK_EVERY_MIN * 60)
            
        except KeyboardInterrupt:
            logger.info("Bot durduruluyor...")
            break
        except Exception as e:
            logger.error(f"Döngü hatası: {e}")
            await send_telegram_with_retry(f"⚠️ <b>Hata!</b>\n{str(e)[:200]}")
            await asyncio.sleep(60)

    # Kapanış mesajı
    await send_telegram_with_retry("👋 <b>Bot Kapatıldı</b>")

# ----------------------- WEB SERVER -----------------------
class HealthHandler(web.View):
    async def get(self):
        uptime = int(time.time() - START_TIME)
        
        return web.json_response({
            "status": "healthy",
            "uptime_seconds": uptime,
            "market_open": is_market_hours(),
            "daily_signals": len(DAILY_SIGNALS),
            "failed_symbols": len(FAILED_SYMBOLS),
            "last_scan": LAST_SCAN_TIME.isoformat() if LAST_SCAN_TIME else None,
            "settings": {
                "tickers": len(TICKERS),
                "timeframes": TIMEFRAMES,
                "check_interval": CHECK_EVERY_MIN,
                "min_score": MIN_SIGNAL_SCORE
            }
        })

class SignalsHandler(web.View):
    async def get(self):
        signals = list(DAILY_SIGNALS.values())
        signals.sort(key=lambda x: x['strength_score'], reverse=True)
        
        return web.json_response({
            "signals": signals,
            "total_count": len(signals),
            "timestamp": dt.datetime.now(IST_TZ).isoformat()
        })

class StatsHandler(web.View):
    async def get(self):
        total_signals = len(DAILY_SIGNALS)
        if total_signals > 0:
            bullish = sum(1 for s in DAILY_SIGNALS.values() if s['direction'] == 'BULLISH')
            bearish = total_signals - bullish
            avg_strength = sum(s['strength_score'] for s in DAILY_SIGNALS.values()) / total_signals
        else:
            bullish = bearish = 0
            avg_strength = 0
        
        return web.json_response({
            "total_signals": total_signals,
            "bullish": bullish,
            "bearish": bearish,
            "average_strength": round(avg_strength, 2),
            "failed_symbols": len(FAILED_SYMBOLS),
            "last_scan": LAST_SCAN_TIME.isoformat() if LAST_SCAN_TIME else None
        })

class ResetHandler(web.View):
    async def post(self):
        global DAILY_SIGNALS, FAILED_SYMBOLS
        
        cleared_signals = len(DAILY_SIGNALS)
        cleared_failed = len(FAILED_SYMBOLS)
        
        DAILY_SIGNALS.clear()
        FAILED_SYMBOLS.clear()
        
        return web.json_response({
            "status": "success",
            "cleared_signals": cleared_signals,
            "cleared_failed": cleared_failed,
            "timestamp": dt.datetime.now(IST_TZ).isoformat()
        })

async def start_web_server():
    """Web server başlat"""
    app = web.Application()
    
    # Route'lar
    app.router.add_get('/health', HealthHandler)
    app.router.add_get('/signals', SignalsHandler)
    app.router.add_get('/stats', StatsHandler)
    app.router.add_post('/reset', ResetHandler)
    
    # CORS middleware
    async def cors_handler(request, handler):
        response = await handler(request)
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
        return response
    
    app.middlewares.append(cors_handler)
    
    # Server başlat
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, '0.0.0.0', HEALTH_CHECK_PORT)
    await site.start()
    
    logger.info(f"Web server başlatıldı: http://0.0.0.0:{HEALTH_CHECK_PORT}")
    logger.info("Endpoints: /health /signals /stats /reset")

# ----------------------- ANA FONKSİYON -----------------------
async def main():
    """Ana uygulama"""
    try:
        logger.info("CakmaUstad Bot başlatılıyor...")
        
        # Ayarları logla
        logger.info(f"Ayarlar:")
        logger.info(f"  Hisse: {len(TICKERS)}")
        logger.info(f"  Timeframes: {TIMEFRAMES}")
        logger.info(f"  Aralık: {CHECK_EVERY_MIN}dk")
        logger.info(f"  Min skor: {MIN_SIGNAL_SCORE}")
        logger.info(f"  Min fiyat: {MIN_PRICE} TL")
        logger.info(f"  Min hacim: {MIN_VOLUME_TRY:,} TL")
        
        # Bağlantı testi
        test_data = await fetch_yfinance_data("AKBNK", "1d")
        if test_data:
            logger.info(f"Yahoo Finance test: AKBNK = {test_data['Close']:.3f} TL")
        else:
            logger.warning("Yahoo Finance test başarısız!")
        
        # Web server ve tarayıcı eşzamanlı başlat
        await asyncio.gather(
            start_web_server(),
            run_scanner_periodically()
        )
        
    except KeyboardInterrupt:
        logger.info("Uygulama durduruldu")
    except Exception as e:
        logger.error(f"Kritik hata: {e}")
        # Acil durum mesajı
        try:
            await send_telegram(f"🚨 <b>KRİTİK HATA!</b>\n\n{str(e)[:200]}")
        except:
            pass
        raise
    finally:
        logger.info("Bot kapatıldı")

if __name__ == '__main__':
    try:
        # Windows uyumluluğu
        if hasattr(asyncio, 'set_event_loop_policy') and os.name == 'nt':
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
        # Ana uygulamayı çalıştır
        asyncio.run(main())
        
    except KeyboardInterrupt:
        print("\nBot kapatılıyor...")
    except Exception as e:
        print(f"\nBaşlatma hatası: {e}")
        sys.exit(1)
