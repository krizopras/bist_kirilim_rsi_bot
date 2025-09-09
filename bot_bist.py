# -*- coding: utf-8 -*-
"""
CakmaUstad BIST Scanner
- TradingView kullan, 429 olursa otomatik bekle ve tekrar dene
- TradingView başarısız olursa yfinance fallback
- Telegram'a sinyal gönder
- Zamanlanmış tarama döngüsü

Gerekli paketler:
pip install tradingview_ta yfinance aiohttp python-dotenv pandas numpy pytz

Dosyalar:
- env.txt (aynı klasörde)
"""

import os
import time
import asyncio
import logging
import random
import json
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional

import pandas as pd
import numpy as np
import pytz
import yfinance as yf
from tradingview_ta import TA_Handler, Interval
from dotenv import dotenv_values
import aiohttp

# ------------------------------------------------------------
# Konfigürasyon
# ------------------------------------------------------------
ENV = dotenv_values("env.txt") if os.path.exists("env.txt") else {}

def getenv(name: str, default: str = "") -> str:
    return os.getenv(name, ENV.get(name, default))

LOG_LEVEL = getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("cakmaustad_scanner")

IST_TZ = pytz.timezone("Europe/Istanbul")

TELEGRAM_BOT_TOKEN = getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = getenv("TELEGRAM_CHAT_ID")

SCAN_MODE = getenv("SCAN_MODE", "ALL").upper()          # ALL | CUSTOM
CUSTOM_TICKERS = [s.strip().upper() for s in getenv("CUSTOM_TICKERS", "GARAN,ASELS,THYAO").split(",") if s.strip()]

TIMEFRAMES = [s.strip() for s in getenv("TIMEFRAMES", "1d,4h,1h,15m").split(",") if s.strip()]

CHECK_EVERY_MIN = int(getenv("CHECK_EVERY_MIN", "15"))
MIN_PRICE = float(getenv("MIN_PRICE", "1.0"))
MIN_VOLUME_TRY = float(getenv("MIN_VOLUME_TRY", "1000000"))
MIN_SIGNAL_SCORE = float(getenv("MIN_SIGNAL_SCORE", "1.15"))

CONCURRENT_REQUESTS = int(getenv("CONCURRENT_REQUESTS", "1"))
SEM = asyncio.Semaphore(CONCURRENT_REQUESTS)

# Rate limit ayarları
MIN_TRADINGVIEW_INTERVAL = 15  # saniye
_last_tv_ts = 0.0
MAX_RETRY_ATTEMPTS = 3

# Batch ayarı (tek seferde kontrol edilen sembol adedi)
BATCH_SIZE = 20

# "ALL" modu için örnek BIST sembolleri (kısa liste; kendi listenle büyütebilirsin)
DEFAULT_BIST_TICKERS = [
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

# ------------------------------------------------------------
# Veri yapıları ve yardımcı fonksiyonlar
# ------------------------------------------------------------
@dataclass
class SignalInfo:
    symbol: str
    timeframe: str
    price: float
    rsi: float
    ema: Optional[float]
    vol_ratio: float
    vol_try: float
    score: float
    direction: str
    timestamp: str

def now_istanbul_str() -> str:
    return pd.Timestamp.now(tz=IST_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")

def safe_float(x, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def calc_rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = (-delta).clip(lower=0)
    roll_up = up.ewm(alpha=1/length, adjust=False).mean()
    roll_down = down.ewm(alpha=1/length, adjust=False).mean()
    rs = roll_up / roll_down
    rsi = 100 - (100 / (1 + rs))
    return rsi

async def _respect_tv_rate_limit():
    global _last_tv_ts
    now = time.time()
    if now - _last_tv_ts < MIN_TRADINGVIEW_INTERVAL:
        await asyncio.sleep(MIN_TRADINGVIEW_INTERVAL - (now - _last_tv_ts))
    _last_tv_ts = time.time()

def _tv_interval_map(tf: str) -> str:
    return {
        "1d": Interval.INTERVAL_1_DAY,
        "4h": Interval.INTERVAL_4_HOURS,
        "1h": Interval.INTERVAL_1_HOUR,
        "15m": Interval.INTERVAL_15_MINUTES
    }.get(tf, Interval.INTERVAL_1_DAY)

# ------------------------------------------------------------
# TradingView'den tek sembol veri çek
# ------------------------------------------------------------
async def fetch_tradingview(symbol: str, timeframe: str) -> Optional[Dict]:
    formats = [f"BIST:{symbol}", f"{symbol}.IS", symbol]
    async with SEM:
        await _respect_tv_rate_limit()
        for fmt in formats:
            try:
                handler = TA_Handler(
                    symbol=fmt,
                    screener="turkey",
                    exchange="BIST",   # bazı formatlarda önemsiz, ama bırakıyoruz
                    interval=_tv_interval_map(timeframe),
                    timeout=20
                )
                a = handler.get_analysis()
                if not a or not a.indicators:
                    continue

                close = safe_float(a.indicators.get("close"))
                if not close:
                    continue

                # TV indikatörleri: varsa al, yoksa None bırak
                rsi = a.indicators.get("RSI")
                ema = a.indicators.get("EMA66") or a.indicators.get("EMA50") or None
                vol = a.indicators.get("volume")

                return {
                    "Close": safe_float(close),
                    "RSI": safe_float(rsi) if rsi is not None else None,
                    "EMA": safe_float(ema) if ema is not None else None,
                    "Volume": safe_float(vol) if vol is not None else None
                }
            except Exception as e:
                msg = str(e).lower()
                if "429" in msg or "too many requests" in msg:
                    logger.warning(f"TradingView 429: {symbol} için 30sn bekleniyor...")
                    await asyncio.sleep(30)
                else:
                    logger.debug(f"TV format çalışmadı ({fmt}): {e}")
        return None

# ------------------------------------------------------------
# yfinance fallback
# ------------------------------------------------------------
def _yf_period_interval(tf: str):
    # yfinance 4h interval'ı bazı durumlarda sınırlı döner; mümkün olanı kullanıyoruz
    period = {"1d": "6mo", "4h": "60d", "1h": "30d", "15m": "5d"}.get(tf, "3mo")
    interval = {"1d": "1d", "4h": "1h", "1h": "1h", "15m": "15m"}.get(tf, "1d")
    return period, interval

async def fetch_yfinance(symbol: str, timeframe: str) -> Optional[Dict]:
    try:
        yf_symbol = f"{symbol}.IS"
        period, interval = _yf_period_interval(timeframe)
        df = yf.download(yf_symbol, period=period, interval=interval, progress=False)
        if df is None or df.empty:
            return None
        close = df["Close"]
        last = df.iloc[-1]
        rsi_series = calc_rsi(close, 14)
        ema_series = close.ewm(span=66, adjust=False).mean()

        return {
            "Close": safe_float(last["Close"]),
            "RSI": safe_float(rsi_series.iloc[-1]),
            "EMA": safe_float(ema_series.iloc[-1]),
            "Volume": safe_float(last.get("Volume", np.nan))
        }
    except Exception as e:
        logger.error(f"yfinance hata ({symbol}, {timeframe}): {e}")
        return None

# ------------------------------------------------------------
# Akıllı çekme: önce TV, sonra fallback yfinance
# ------------------------------------------------------------
async def fetch_market_data(symbol: str, timeframe: str) -> Optional[Dict]:
    # Birkaç deneme: TV -> yfinance -> bekle & tekrar
    for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
        data = await fetch_tradingview(symbol, timeframe)
        if data:
            return data

        logger.warning(f"TV başarısız ({symbol}, {timeframe}), yfinance fallback denemesi (try={attempt})...")
        yfd = await fetch_yfinance(symbol, timeframe)
        if yfd:
            return yfd

        await asyncio.sleep(5 * attempt)  # artan bekleme
    return None
# ------------------------------------------------------------
# Sinyal skoru ve filtreler
# ------------------------------------------------------------
def compute_score(price: float, rsi: Optional[float], ema: Optional[float],
                  volume: Optional[float]) -> Dict[str, float]:
    """
    Basit bir skor:
    - RSI 30 altı => güçlü pozitif, 70 üstü => negatif
    - Fiyat EMA66 üstü => +, altı => -
    - Hacim katsayısı (normalize yoksa 1.0)
    """
    score = 1.0
    direction = "NEUTRAL"
    vol_ratio = 1.0

    if volume is not None and volume > 0:
        # çok kaba normalize: log ölçeği
        vol_ratio = max(0.5, min(2.0, np.log10(volume + 10) / 6))

    if rsi is not None:
        if rsi < 30:
            score *= 1.25
            direction = "BULLISH"
        elif rsi > 70:
            score *= 0.85
            direction = "BEARISH"
        else:
            score *= 1.0
            direction = "NEUTRAL"

    if ema is not None and price is not None:
        if price > ema:
            score *= 1.10
            if direction == "NEUTRAL":
                direction = "BULLISH"
        elif price < ema:
            score *= 0.92
            if direction == "NEUTRAL":
                direction = "BEARISH"

    score *= vol_ratio
    return {"score": float(score), "direction": direction, "vol_ratio": float(vol_ratio)}

def passes_filters(price: float, vol_try: float) -> bool:
    if price is None or price < MIN_PRICE:
        return False
    if vol_try < MIN_VOLUME_TRY:
        return False
    return True

# ------------------------------------------------------------
# Telegram
# ------------------------------------------------------------
async def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram env değişkenleri boş; mesaj gönderilmeyecek.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=payload, timeout=20) as resp:
                if resp.status != 200:
                    txt = await resp.text()
                    logger.error(f"Telegram hata: {resp.status} - {txt}")
        except Exception as e:
            logger.error(f"Telegram gönderim hatası: {e}")

def fmt_signal_message(s: SignalInfo) -> str:
    arrow = "🟢" if s.direction == "BULLISH" else ("🔴" if s.direction == "BEARISH" else "⚪")
    return (
        f"{arrow} <b>{s.symbol}</b>  <code>{s.timeframe}</code>\n"
        f"Fiyat: <b>{s.price:.2f}</b>  |  RSI: <b>{s.rsi:.2f}</b>  |  EMA66: <b>{'—' if s.ema is None else f'{s.ema:.2f}'}</b>\n"
        f"Hacim Oranı: <b>{s.vol_ratio:.2f}</b>  |  İşlem Tutarı(≈): <b>{s.vol_try:,.0f} ₺</b>\n"
        f"Skor: <b>{s.score:.2f}</b>  |  Yön: <b>{s.direction}</b>\n"
        f"<i>{s.timestamp}</i>"
    )

# ------------------------------------------------------------
# Tek sembol + timeframe analiz
# ------------------------------------------------------------
async def analyze_symbol_timeframe(symbol: str, timeframe: str) -> Optional[SignalInfo]:
    data = await fetch_market_data(symbol, timeframe)
    if not data:
        return None

    price = safe_float(data.get("Close"), np.nan)
    rsi = data.get("RSI")
    ema = data.get("EMA")
    vol = data.get("Volume")

    # yfinance'da Volume lot adedi; TRY tahmini için fiyata çarpıyoruz (yaklaşık)
    vol_try = float(price) * float(vol) if (not pd.isna(price) and vol is not None and not pd.isna(vol)) else 0.0

    sc = compute_score(price, rsi, ema, vol)
    score = sc["score"]
    direction = sc["direction"]
    vol_ratio = sc["vol_ratio"]

    if not passes_filters(price, vol_try):
        return None
    if score < MIN_SIGNAL_SCORE:
        return None

    return SignalInfo(
        symbol=symbol,
        timeframe=timeframe,
        price=float(price),
        rsi=float(rsi) if rsi is not None and not pd.isna(rsi) else float("nan"),
        ema=float(ema) if ema is not None and not pd.isna(ema) else None,
        vol_ratio=vol_ratio,
        vol_try=vol_try,
        score=score,
        direction=direction,
        timestamp=now_istanbul_str()
    )

# ------------------------------------------------------------
# Batch tarama
# ------------------------------------------------------------
async def scan_batch(symbols: List[str], timeframes: List[str]) -> List[SignalInfo]:
    tasks = []
    for sym in symbols:
        for tf in timeframes:
            tasks.append(analyze_symbol_timeframe(sym, tf))

    out: List[SignalInfo] = []
    for chunk_start in range(0, len(tasks), BATCH_SIZE):
        chunk = tasks[chunk_start:chunk_start + BATCH_SIZE]
        results = await asyncio.gather(*chunk, return_exceptions=True)
        for res in results:
            if isinstance(res, SignalInfo):
                out.append(res)
            elif isinstance(res, Exception):
                logger.debug(f"Analyze exception: {res}")
            elif res:
                # SignalInfo veya Exception değilse (None değilse)
                out.append(res)
        # TV rate-limit açısından küçük ara
        await asyncio.sleep(1.0)
    return out
# ------------------------------------------------------------
# Sembol listesi seçimi
# ------------------------------------------------------------
def get_symbol_universe() -> List[str]:
    if SCAN_MODE == "CUSTOM" and CUSTOM_TICKERS:
        return CUSTOM_TICKERS
    # ALL modu: örnek kısa liste; kendi evrenini ekleyebilirsin
    return DEFAULT_BIST_TICKERS

# ------------------------------------------------------------
# Periyodik tarama döngüsü
# ------------------------------------------------------------
async def periodic_scan():
    logger.info("CakmaUstad scanner başlıyor...")
    while True:
        try:
            symbols = get_symbol_universe()
            random.shuffle(symbols)  # hep aynı sırayı sorgulamayalım

            logger.info(f"Tarama başlıyor | Sembol sayısı: {len(symbols)} | TF: {','.join(TIMEFRAMES)}")
            signals = await scan_batch(symbols, TIMEFRAMES)
            if signals:
                # Skora göre sırala, en yüksekleri yolla
                signals.sort(key=lambda s: s.score, reverse=True)
                top = signals[:10]  # spam olmasın diye en iyi 10
                summary = [fmt_signal_message(s) for s in top]
                text = "📈 <b>BIST Tarama Sinyalleri</b>\n\n" + "\n\n".join(summary)
                await send_telegram(text)
                logger.info(f"{len(top)} sinyal gönderildi.")
            else:
                logger.info("Bu turda sinyal yok.")

        except Exception as e:
            logger.exception(f"Tarama hatası: {e}")

        # Sonraki tura kadar bekle
        await asyncio.sleep(CHECK_EVERY_MIN * 60)

# ------------------------------------------------------------
# Basit healthcheck (opsiyonel)
# ------------------------------------------------------------
async def run_health_server():
    from aiohttp import web
    async def health(_):
        return web.json_response({"status": "ok", "time": now_istanbul_str()})
    app = web.Application()
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()
    logger.info("Health server 0.0.0.0:8080/health")

# ------------------------------------------------------------
# main
# ------------------------------------------------------------
async def main():
    # Health server ve taramayı birlikte çalıştır
    await asyncio.gather(
        run_health_server(),
        periodic_scan()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Durduruldu.")
