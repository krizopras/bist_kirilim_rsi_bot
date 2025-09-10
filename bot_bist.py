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

# -------------------- KONFIGÜRASYON --------------------
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

# -------------------- COOLDOWN YÖNETIMİ --------------------
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
        # Alpha Vantage için API anahtarını çevre değişkeninden al
        self.av_api_key = os.getenv("ALPHA_VANTAGE_API_KEY")

    async def _fetch_with_fallback(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        """Fallback veri kaynakları ile veri çekme"""
        # 1. Öncelik: Alpha Vantage
        if self.av_api_key:
            try:
                return await self._fetch_alpha_vantage(symbol, timeframe)
            except Exception as e:
                logger.debug(f"Alpha Vantage başarısız {symbol}: {e}")

        # 2. Öncelik: Yahoo Finance
        try:
            return await self._fetch_yahoo(symbol, timeframe)
        except Exception as e:
            logger.debug(f"Yahoo Finance başarısız {symbol}: {e}")

        # 3. Son Çare: Mock data
        return self._generate_mock_data(symbol)

    async def _fetch_yahoo(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        """Yahoo Finance'den veri çek"""
        await RATE_LIMITER.acquire()
        yf_symbol = f"{symbol}.IS"
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
                        "ema55": float(df["ema55"].iloc[-1])
                    }
                }
        except aiohttp.ClientError as e:
            raise RuntimeError(f"Aiohttp veri hatası: {e}")
        except (ValueError, KeyError) as e:
            raise ValueError(f"Yahoo Finance veri formatı hatası: {e}")

    async def _fetch_alpha_vantage(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        """Alpha Vantage'dan veri çekme fonksiyonu"""
        if not self.av_api_key:
            raise ValueError("Alpha Vantage API anahtarı ayarlanmamış.")

        function = 'TIME_SERIES_DAILY_ADJUSTED'
        interval = None
        if timeframe in ['1h', '4h', '15m']:
            function = 'TIME_SERIES_INTRADAY'
            if timeframe == '4h':
                # Alpha Vantage 4h aralığı sağlamaz, 60min kullanacağız.
                interval = '60min'
            else:
                interval = timeframe.replace('h', 'min')

        url = f"https://www.alphavantage.co/query?function={function}&symbol={symbol}.IS&outputsize=full&apikey={self.av_api_key}"
        if interval:
            url += f"&interval={interval}"

        await RATE_LIMITER.acquire()
        
        async with self.session.get(url, timeout=CONFIG.http_timeout) as response:
            if response.status != 200:
                raise ValueError(f"HTTP {response.status}")
            data = await response.json()
            
            if 'Error Message' in data:
                raise ValueError(f"Alpha Vantage hatası: {data['Error Message']}")
            
            time_series_key = "Time Series (Daily)" if function == 'TIME_SERIES_DAILY_ADJUSTED' else f"Time Series ({interval})"
            if time_series_key not in data:
                raise ValueError(f"Alpha Vantage'dan veri alınamadı: {data}")

            df = pd.DataFrame.from_dict(data[time_series_key], orient='index').astype(float)
            df.index = pd.to_datetime(df.index)
            df = df.sort_index()
            
            df.columns = ['open', 'high', 'low', 'close', 'adjusted close', 'volume', 'dividend amount', 'split coefficient'] if function == 'TIME_SERIES_DAILY_ADJUSTED' else ['open', 'high', 'low', 'close', 'volume']
            df['close'] = df['adjusted close'] if 'adjusted close' in df.columns else df['close']
            df = df[['open', 'high', 'low', 'close', 'volume']]
            
            if len(df) < 50:
                raise ValueError("Yetersiz veri")
            
            df = self._calculate_indicators(df)

            return {
                "df": df,
                "current": {
                    "price": float(df["close"].iloc[-1]),
                    "volume": float(df["volume"].iloc[-1]),
                    "rsi": float(df["rsi"].iloc[-1]),
                    "ema13": float(df["ema13"].iloc[-1]),
                    "ema55": float(df["ema55"].iloc[-1])
                }
            }
    def _calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Teknik göstergeleri hesapla"""
        delta = df["close"].diff()
        gain = delta.where(delta > 0, 0.0)
        loss = (-delta).where(delta < 0, 0.0)
        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()
        rs = avg_gain / avg_loss.replace(0, 1e-10)
        df["rsi"] = 100 - (100 / (1 + rs))
        df["rsi"] = df["rsi"].fillna(50)
        df["ema13"] = df["close"].ewm(span=13).mean()
        df["ema55"] = df["close"].ewm(span=55).mean()
        df["ema233"] = df["close"].ewm(span=min(233, max(3, len(df)//2))).mean()
        return df

# -------------------- SİNYAL ANALİZİ --------------------
class BistSignalAnalyzer:
    def __init__(self, config: BistBotConfig):
        self.config = config

    def analyze_symbol(self, symbol: str, timeframe_data: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Sembol analizi"""
        if not timeframe_data:
            return None
        main_tf = "1h" if "1h" in timeframe_data else list(timeframe_data.keys())[0]
        main_data = timeframe_data[main_tf]
        current = main_data["current"]
        df = main_data["df"]
        if current["price"] < self.config.min_price:
            return None
        volume_try = current["price"] * current["volume"]
        if volume_try < self.config.min_volume_try:
            return None
        
        # Sinyal yönünü belirlemek için ana filtreyi çalıştır
        direction = self._determine_direction(current)
        if direction == "NEUTRAL":
            return None

        # Sadece ana filtreyi geçen hisseler için detaylı sinyalleri hesapla
        signals = self._calculate_signals(df, current, direction)
        total_score = self._calculate_total_score(signals)
        
        # Puan kontrolü
        if total_score < self.config.min_signal_score:
            return None

        # Multi-timeframe tutarlılık kontrolü
        if not self._check_timeframe_consistency(timeframe_data, direction):
            return None
        
        return {
            "symbol": symbol,
            "direction": direction,
            "score": round(total_score, 2),
            "price": current["price"],
            "rsi": current["rsi"],
            "volume_try": volume_try,
            "signals": signals,
            "timeframe_scores": {tf: self._calculate_total_score(
                self._calculate_signals(data["df"], data["current"], direction)
            ) for tf, data in timeframe_data.items()}
        }

    def _calculate_signals(self, df: pd.DataFrame, current: Dict[str, Any], direction: str) -> Dict[str, float]:
        """Sinyal skorları hesapla"""
        signals = {
            "rsi_momentum": 0.0,
            "price_breakout": 0.0,
            "trend_acceleration": 0.0,
            "volume_confirmation": 0.0
        }

        # Sadece BUY sinyali için RSI momentumunu hesapla
        if direction == "BUY":
            rsi = current["rsi"]
            if rsi < 50:
                signals["rsi_momentum"] = 1.0
                if rsi < 40:
                    signals["rsi_momentum"] = 1.5
                if rsi < 30:
                    signals["rsi_momentum"] = 2.0

        # Fiyat Kırılımı Sinyali
        try:
            price = current["price"]
            ema13 = current["ema13"]
            distance_to_ema = abs(price - ema13) / ema13 * 100
            if distance_to_ema < 1.0:
                signals["price_breakout"] = 1.5
            elif distance_to_ema < 2.0:
                signals["price_breakout"] = 1.0
        except (ZeroDivisionError, KeyError):
            pass

        # Trend Hızlanması Sinyali
        if len(df) >= 5:
            recent_ema13 = df["ema13"].iloc[-5:].diff().mean()
            if recent_ema13 > 0:
                signals["trend_acceleration"] = 1.0

        # Hacim Onayı Sinyali
        if len(df) >= 20:
            avg_volume = df["volume"].iloc[-20:].mean()
            if current["volume"] > avg_volume * 1.5:
                signals["volume_confirmation"] = 1.0
        
        return signals

    def _calculate_total_score(self, signals: Dict[str, float]) -> float:
        """Toplam skor hesapla"""
        weighted_sum = (
            signals.get("rsi_momentum", 0) * self.config.weight_rsi +
            signals.get("price_breakout", 0) * self.config.weight_breakout +
            signals.get("trend_acceleration", 0) * self.config.weight_momentum +
            signals.get("volume_confirmation", 0) * self.config.weight_volume
        )
        total_weights = (
            self.config.weight_rsi + self.config.weight_breakout +
            self.config.weight_momentum + self.config.weight_volume
        )
        if total_weights == 0:
            return 0
        return weighted_sum / total_weights

    def _determine_direction(self, current: Dict[str, Any]) -> str:
        """Sinyal yönünü belirle (ana filtre)"""
        price = current["price"]
        ema13 = current["ema13"]
        ema55 = current["ema55"]
        
        if price > ema13 > ema55:
            return "BUY"
        if price < ema13 < ema55:
            return "SELL"
            
        return "NEUTRAL"

    def _check_timeframe_consistency(self, timeframe_data: Dict[str, Dict[str, Any]], main_direction: str) -> bool:
        """Multi-timeframe tutarlılık kontrolü"""
        if len(timeframe_data) <= 1:
            return True
        consistent_count = 0
        for tf, data in timeframe_data.items():
            # Yeni mantığa göre sadece yön kontrolü yap
            tf_direction = self._determine_direction(data["current"])
            if tf_direction == main_direction:
                consistent_count += 1
        return consistent_count >= max(1, len(timeframe_data) // 2)
# -------------------- TELEGRAM --------------------
class BistTelegramNotifier:
    def __init__(self, token: str, chat_id: str, session: aiohttp.ClientSession):
        self.base = f"https://api.telegram.org/bot{token}"
        self.chat_id = chat_id
        self.session = session

    async def send_message(self, text: str):
        try:
            url = f"{self.base}/sendMessage"
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
            async with self.session.post(url, json=payload, timeout=CONFIG.http_timeout) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    logger.warning(f"Telegram hata {resp.status}: {body}")
                else:
                    logger.info("Telegram mesajı gönderildi")
        except aiohttp.ClientError as e:
            logger.error(f"Telegram gönderim hatası (Aiohttp): {e}")
        except Exception as e:
            logger.error(f"Telegram gönderim hatası: {e}")

    async def send_chart(self, symbol: str, df: pd.DataFrame, signal_info: Dict[str, Any]):
        """Grafik oluşturup Telegram'a gönder"""
        try:
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), gridspec_kw={'height_ratios': [3, 1]})
            recent_data = df.tail(50)
            dates = list(range(len(recent_data)))
            ax1.plot(dates, recent_data["close"], label="Fiyat", color='dodgerblue')
            ax1.plot(dates, recent_data["ema13"], label="EMA13", color='orange', linestyle='--')
            ax1.plot(dates, recent_data["ema55"], label="EMA55", color='red', linestyle='--')
            if len(recent_data) > 2:
                for i in range(1, len(recent_data)):
                    if (recent_data['ema13'].iloc[i-1] < recent_data['ema55'].iloc[i-1] and recent_data['ema13'].iloc[i] > recent_data['ema55'].iloc[i]):
                        ax1.axvline(x=dates[i], color='green', linestyle=':', linewidth=2, label='Golden Cross')
                    elif (recent_data['ema13'].iloc[i-1] > recent_data['ema55'].iloc[i-1] and recent_data['ema13'].iloc[i] < recent_data['ema55'].iloc[i]):
                        ax1.axvline(x=dates[i], color='purple', linestyle=':', linewidth=2, label='Death Cross')
            ax1.set_title(f"{symbol} - {signal_info['direction']} Sinyali", fontsize=14, fontweight='bold')
            ax1.legend()
            ax1.grid(True, alpha=0.3)
            ax2.plot(dates, recent_data["rsi"], label="RSI", color='green')
            ax2.axhline(70, linestyle='--', color='red', alpha=0.5, label='Aşırı Alım')
            ax2.axhline(30, linestyle='--', color='green', alpha=0.5, label='Aşırı Satım')
            ax2.axhline(50, linestyle='-', color='gray', alpha=0.3)
            ax2.set_ylim(0, 100)
            ax2.set_title(f"RSI: {signal_info['rsi']:.1f}", fontsize=12)
            ax2.legend()
            ax2.grid(True, alpha=0.3)
            plt.tight_layout()
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
            plt.close()
            buf.seek(0)
            url = f"{self.base}/sendPhoto"
            form = aiohttp.FormData()
            form.add_field('chat_id', self.chat_id)
            form.add_field('photo', buf, filename=f'{symbol}.png', content_type='image/png')
            async with self.session.post(url, data=form, timeout=CONFIG.http_timeout) as response:
                if response.status != 200:
                    body = await response.text()
                    logger.error(f"Grafik gönderme hatası: {response.status} {body}")
            buf.close()
        except aiohttp.ClientError as e:
            logger.error(f"Grafik gönderme hatası (Aiohttp): {e}")
        except Exception as e:
            logger.error(f"Grafik oluşturma hatası: {e}")

# -------------------- SİNYAL BİLDİRİMLERİ --------------------
async def send_signal_notification(telegram: BistTelegramNotifier, signal_info: Dict[str, Any],
                                   timeframe_data: Dict[str, Dict[str, Any]]):
    """Sinyali formatlayıp Telegram'a gönder"""
    try:
        symbol = signal_info["symbol"]
        dir_ = signal_info["direction"]
        score = signal_info["score"]
        price = signal_info["price"]
        rsi = signal_info["rsi"]
        msg_lines = [
            f"📣 <b>{symbol}</b> — <b>{dir_}</b>",
            f"Skor: <b>{score}</b>",
            f"Fiyat: <b>{price:.2f} TRY</b>",
            f"RSI: <b>{rsi:.1f}</b>",
            f"Hacim (TRY): <b>{signal_info['volume_try']:.0f}</b>",
            "",
            "<b>Detaylar (zaman dilimleri):</b>"
        ]
        for tf, sc in signal_info.get("timeframe_scores", {}).items():
            msg_lines.append(f"- {tf}: {sc:.2f}")
        msg_lines.append("")
        msg_lines.append("<b>Sinyal bileşenleri:</b>")
        for k, v in signal_info.get("signals", {}).items():
            msg_lines.append(f"- {k}: {v}")
        message = "\n".join(msg_lines)
        await telegram.send_message(message)
        main_tf = "1h" if "1h" in timeframe_data else next(iter(timeframe_data))
        main_df = timeframe_data[main_tf]["df"]
        await telegram.send_chart(symbol, main_df, signal_info)
    except Exception as e:
        logger.exception(f"Sinyal bildirimi gönderilemedi: {e}")

async def send_scan_summary(telegram: BistTelegramNotifier, results: List[Dict[str, Any]], elapsed: float):
    """Tarama özeti gönder"""
    try:
        top_n = min(8, len(results))
        lines = [
            f"🔎 Tarama tamamlandı — {len(results)} sinyal bulundu",
            f"Süre: {elapsed:.1f}s",
            "",
            "<b>En iyi sinyaller:</b>"
        ]
        for r in results[:top_n]:
            lines.append(f"{r['symbol']} — {r['direction']} — Skor: {r['score']:.2f} — Fiyat: {r['price']:.2f} TRY — RSI: {r['rsi']:.1f}")
        await telegram.send_message("\n".join(lines))
    except Exception as e:
        logger.exception(f"Özet gönderilemedi: {e}")

# -------------------- ANA TARAMA FONKSİYONU --------------------
async def scan_symbols(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """Ana tarama fonksiyonu"""
    start_time = time.time()
    logger.info(f"🔍 BIST taraması başlıyor: {len(SELECTED_SYMBOLS)} sembol")
    data_client = BistDataClient(session)
    analyzer = BistSignalAnalyzer(CONFIG)
    telegram = None
    if CONFIG.telegram_token and CONFIG.telegram_chat_id:
        telegram = BistTelegramNotifier(CONFIG.telegram_token, CONFIG.telegram_chat_id, session)
    results = []
    semaphore = asyncio.Semaphore(CONFIG.concurrency)
    async def analyze_single_symbol(symbol: str) -> Optional[Dict[str, Any]]:
        async with semaphore:
            try:
                if await is_in_cooldown(symbol):
                    return None
                timeframe_data = {}
                for tf in CONFIG.timeframes:
                    try:
                        data = await data_client._fetch_with_fallback(symbol, tf)
                        if data:
                            timeframe_data[tf] = data
                    except (ValueError, RuntimeError, KeyError) as e:
                        logger.debug(f"{symbol} {tf} veri hatası: {e}")
                        continue
                if not timeframe_data:
                    return None
                signal_info = analyzer.analyze_symbol(symbol, timeframe_data)
                if signal_info:
                    if telegram:
                        await send_signal_notification(telegram, signal_info, timeframe_data)
                    await mark_signal_sent(
                        signal_info["symbol"],
                        signal_info["price"],
                        signal_info["score"],
                        signal_info["rsi"],
                        signal_info["direction"]
                    )
                    logger.info(f"✅ {signal_info['direction']} SİNYAL: {symbol} - Skor: {signal_info['score']:.2f}")
                    return signal_info
                return None
            except Exception as e:
                logger.warning(f"{symbol} analiz hatası: {e}")
                return None
    tasks = [analyze_single_symbol(symbol) for symbol in SELECTED_SYMBOLS[:CONFIG.max_symbols]]
    completed = await asyncio.gather(*tasks, return_exceptions=True)
    for result in completed:
        if isinstance(result, dict):
            results.append(result)
    results.sort(key=lambda x: x["score"], reverse=True)
    elapsed = time.time() - start_time
    logger.info(f"✅ Tarama tamamlandı: {elapsed:.1f}s - {len(results)} sinyal bulundu")
    if telegram and results:
        await send_scan_summary(telegram, results, elapsed)
    return results

# -------------------- HTTP SAĞLIK KONTROLÜ --------------------
async def health_handler(request):
    return web.Response(text="OK")

# -------------------- GLOBAL FLAGS --------------------
running = True

# -------------------- SIGNAL HANDLER --------------------
def _signal_handler(sig, frame):
    global running
    logger.info(f"Signal received: {sig}. Shutting down...")
    running = False

# -------------------- PERIODIC TASK --------------------
async def periodic_runner(app):
    """Periyodik tarayıcı; arka planda çalışır."""
    session: aiohttp.ClientSession = app["session"]
    interval_sec = int(os.getenv("SCAN_INTERVAL_SEC", "900"))
    while running:
        try:
            if is_market_hours() or TEST_MODE:
                await scan_symbols(session)
            else:
                logger.debug("Market kapalı; tarama atlandı.")
        except Exception as e:
            logger.exception(f"Periyodik tarama hatası: {e}")
        for _ in range(max(1, interval_sec // 1)):
            if not running:
                break
            await asyncio.sleep(1)
    logger.info("Periodic runner stopped.")

# -------------------- BACKGROUND TASKS --------------------
async def start_background_tasks(app):
    app["bg_task"] = asyncio.create_task(periodic_runner(app))

async def cleanup_background_tasks(app):
    task = app.get("bg_task")
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

# -------------------- APP INITIALIZATION --------------------
async def init_app():
    app = web.Application()
    app.add_routes([web.get("/health", health_handler)])
    timeout = aiohttp.ClientTimeout(total=CONFIG.http_timeout)
    session = aiohttp.ClientSession(timeout=timeout)
    app["session"] = session
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)
    return app

# -------------------- MAIN ENTRY --------------------
async def main():
    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, _signal_handler, s, None)
        except (NotImplementedError, ValueError):
            pass
    app = await init_app()
    runner = web.AppRunner(app)
    await runner.setup()
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8080"))
    site = web.TCPSite(runner, host=host, port=port)
    logger.info(f"Başlatılıyor — host={host} port={port} TEST_MODE={TEST_MODE}")
    await site.start()
    try:
        while running:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("Kapatılıyor...")
        await runner.cleanup()
        await app["session"].close()
        logger.info("Çıkış tamamlandı.")

# -------------------- PROGRAM START --------------------
if __name__ == "__main__":
    asyncio.run(main())
