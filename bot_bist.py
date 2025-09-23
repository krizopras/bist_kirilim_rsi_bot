#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BIST AKILLI TARAYICI BOT - Güvenilir Veri Kaynağı ile RSI + EMA Analizi
- Yahoo Finance fallback implemented (you had it)
- Multi-timeframe analiz desteği
- Gelişmiş sinyal filtreleme
- Cooldown sistemi ve performans takibi
- Telegram bildirimleri
"""
from __future__ import annotations
import os
import io
import json
import time
import math
import asyncio
import logging
import signal
import datetime as dt
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any
import sys
import locale

import numpy as np
import pandas as pd
import aiohttp
from aiohttp import web
import pytz

# Matplotlib backend ayarı
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Karakter kodlama düzeltmesi
try:
    locale.setlocale(locale.LC_ALL, 'tr_TR.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_ALL, 'Turkish_Turkey.1254')
    except locale.Error:
        pass

# -------------------- LOGGING --------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").strip().upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler('bist_bot.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("bist_akilli_tarayici")

# -------------------- PERSIST DOSYA --------------------
SIGNALS_LOG_PATH = os.getenv("SIGNALS_LOG_PATH", "bist_signals_log.jsonl")
os.makedirs(os.path.dirname(SIGNALS_LOG_PATH) if os.path.dirname(SIGNALS_LOG_PATH) else ".", exist_ok=True)

def _append_jsonl(path: str, obj: dict):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def _safe_read_jsonl(path: str) -> List[dict]:
    if not os.path.exists(path):
        return []
    rows = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except Exception:
        return []
    return rows

# -------------------- KONFİGÜRASYON --------------------
@dataclass
class BistBotConfig:
    telegram_token: Optional[str]
    telegram_chat_id: Optional[str]
    min_volume_try: float = 1_000_000
    max_symbols: int = 100

    min_signal_score: float = 3.0       # Sinyal puanını biraz artırabiliriz
    weight_ema: float = 0.0             # Bunu artık filtre olarak kullanıyoruz
    weight_volume: float = 4.0          # Hacim çok önemli
    weight_breakout: float = 3.5        # Fiyat kırılımı da çok önemli
    weight_rsi: float = 2.0             # RSI standart bir destekleyici sinyal
    weight_momentum: float = 1.5        # Trendin hızlanması için daha düşük bir ağırlık
    
    timeframes: Tuple[str, ...] = ("1h", "4h", "1d")
    http_timeout: int = 15
    rate_limit_per_min: int = 600
    concurrency: int = 5

    signal_cooldown_h: int = 6
    min_price: float = 1.0

    # BIST seans saatleri
    market_open: dt.time = dt.time(9, 30)
    market_close: dt.time = dt.time(18, 5)
    lunch_start: dt.time = dt.time(13, 0)
    lunch_end: dt.time = dt.time(14, 0)

    @staticmethod
    def from_env() -> "BistBotConfig":
        return BistBotConfig(
            telegram_token=os.getenv("TELEGRAM_BOT_TOKEN"),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID"),
            min_volume_try=float(os.getenv("MIN_VOLUME_TRY", 1_000_000)),
            max_symbols=int(os.getenv("MAX_SYMBOLS", 100)),
            min_signal_score=float(os.getenv("MIN_SIGNAL_SCORE", 2.5)),
            concurrency=int(os.getenv("CONCURRENCY", 5)),
            signal_cooldown_h=int(os.getenv("SIGNAL_COOLDOWN_H", 6)),
            min_price=float(os.getenv("MIN_PRICE", 1.0))
        )

CONFIG = BistBotConfig.from_env()

# Zaman dilimi ayarı
IST_TZ = pytz.timezone('Europe/Istanbul')

# -------------------- BIST HISSE LISTESI --------------------
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
   

# Test modu için farklı sembol seçimi
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"
SELECTED_SYMBOLS = ALL_BIST_STOCKS[:20] if TEST_MODE else ALL_BIST_STOCKS

# -------------------- RATE LIMITER --------------------
class BistRateLimiter:
    def __init__(self, max_requests_per_min: int):
        self.max_requests = max_requests_per_min
        self.window = 60.0
        self.requests = 0
        self.window_start = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            if now - self.window_start >= self.window:
                self.requests = 0
                self.window_start = now

            if self.requests >= self.max_requests:
                sleep_time = self.window - (now - self.window_start)
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                    self.requests = 0
                    self.window_start = time.monotonic()

            self.requests += 1

RATE_LIMITER = BistRateLimiter(CONFIG.rate_limit_per_min)

# -------------------- MARKET HOURS CHECK --------------------
def is_market_hours() -> bool:
    """Borsa İstanbul işlem saatleri kontrolü"""
    if TEST_MODE:
        return True

    now_ist = dt.datetime.now(IST_TZ)

    # Hafta sonu kontrolü
    if now_ist.weekday() >= 5:
        return False

    current_time = now_ist.time()

    # Öğle molası kontrolü
    if CONFIG.lunch_start <= current_time < CONFIG.lunch_end:
        return False

    return CONFIG.market_open <= current_time <= CONFIG.market_close

# -------------------- COOLDOWN YÖNETİMİ --------------------
async def is_in_cooldown(symbol: str) -> bool:
    lookback = dt.datetime.utcnow() - dt.timedelta(hours=CONFIG.signal_cooldown_h)
    
    # Not: Büyük dosyalarda performans sorununu önlemek için
    # sadece son birkaç satırı okumak daha verimli olabilir.
    recent_signals = _safe_read_jsonl(SIGNALS_LOG_PATH)[-1000:]

    for rec in reversed(recent_signals):
        if rec.get("symbol") != symbol:
            continue
        try:
            ts = dt.datetime.fromisoformat(rec["ts"])
            if ts >= lookback:
                return True
            # Tarih eski ise döngüden çık
            if ts < lookback:
                break
        except (ValueError, KeyError):
            continue
    return False

async def mark_signal_sent(symbol: str, entry: float, score: float, rsi: float, signal_type: str):
    ts = dt.datetime.utcnow().isoformat()
    rec = {
        "ts": ts,
        "symbol": symbol,
        "entry": float(entry),
        "score": float(score),
        "rsi": float(rsi),
        "signal_type": signal_type,
    }
    _append_jsonl(SIGNALS_LOG_PATH, rec)

# -------------------- VERİ KAYNAĞI --------------------
class BistDataClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def _fetch_with_fallback(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        """Fallback veri kaynakları ile veri çekme"""
        # 1. Öncelik: Yahoo Finance
        try:
            return await self._fetch_yahoo(symbol, timeframe)
        except Exception as e:
            logger.debug(f"Yahoo Finance başarısız {symbol}: {e}")

        # 2. Son Çare: Mock data (sadece test modunda)
        if TEST_MODE:
            return self._generate_mock_data(symbol)
        return None

    async def _fetch_yahoo(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        """Yahoo Finance'den veri çek"""
        await RATE_LIMITER.acquire()
        yf_symbol = f"{symbol.replace('.', '')}.IS"  # Düzeltildi: noktaları kaldır
        interval_map = {"15m": "15m", "1h": "1h", "4h": "1h", "1d": "1d"}
        interval = interval_map.get(timeframe, "1h")
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yf_symbol}"
        params = {
            "interval": interval,
            "period1": int((dt.datetime.now() - dt.timedelta(days=90)).timestamp()),
            "period2": int(dt.datetime.now().timestamp()),
            "includePrePost": "false"
        }
        try:
            async with self.session.get(url, params=params, timeout=CONFIG.http_timeout) as response:
                if response.status != 200:
                    raise ValueError(f"HTTP {response.status}")
                data = await response.json()
                result = data.get("chart", {}).get("result", [])
                if not result:
                    raise ValueError("Boş sonuç")
                quote = result[0]
                timestamps = quote.get("timestamp", [])
                indicators = quote.get("indicators", {})
                quote_data = indicators.get("quote", [{}])[0]
                if not timestamps or not quote_data:
                    raise ValueError("Veri eksik")
                closes = quote_data.get("close", [])
                volumes = quote_data.get("volume", [])
                highs = quote_data.get("high", [])
                lows = quote_data.get("low", [])
                opens = quote_data.get("open", [])
                if not closes or len(closes) < 50:
                    raise ValueError("Yetersiz veri")
                df_data = {
                    "timestamp": timestamps[-100:], "open": opens[-100:],
                    "high": highs[-100:], "low": lows[-100:],
                    "close": closes[-100:], "volume": volumes[-100:]
                }
                df = pd.DataFrame(df_data)
                df = df.dropna()
                if len(df) < 20:
                    raise ValueError("Temizleme sonrası yetersiz veri")
                df = self._calculate_indicators(df)
                return {
                    "df": df,
                    "current": {
                        "price": float(df["close"].iloc[-1]),
                        "volume": float(df["volume"].iloc[-1]),
                        "rsi": float(df["rsi"].iloc[-1]),
                        "ema13": float(df["ema13"].iloc[-1]),
                        "ema55": float(
