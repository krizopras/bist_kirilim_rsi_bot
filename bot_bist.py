#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BIST AKILLI TARAYICI BOT - Strateji Odaklı Teknik Analiz
- Strateji notlarına uygun timeframe konfigürasyonu
- RSI, EMA ve MACD kombinasyonu
- Kategori bazlı puanlama sistemi
- Gerçekçi sinyal filtreleme
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
import datetime as dt
from datetime import timezone
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
    min_volume_try: float = 500_000  # Daha düşük hacim limiti
    max_symbols: int = 100

    min_signal_score: float = 1.5       # Daha düşük minimum puan
    signal_cooldown_h: int = 4          # Daha kısa cooldown
    min_price: float = 0.5              # Daha düşük minimum fiyat
    min_volume_ratio: float = 1.2       # Minimum hacim oranı
    min_active_signals: int = 2         # Minimum aktif sinyal sayısı
    
    timeframes: Tuple[str, ...] = ("15m", "1h", "4h")
    http_timeout: int = 15
    rate_limit_per_min: int = 600
    concurrency: int = 5

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
            min_volume_try=float(os.getenv("MIN_VOLUME_TRY", 500_000)),
            max_symbols=int(os.getenv("MAX_SYMBOLS", 100)),
            min_signal_score=float(os.getenv("MIN_SIGNAL_SCORE", 1.5)),
            concurrency=int(os.getenv("CONCURRENCY", 5)),
            signal_cooldown_h=int(os.getenv("SIGNAL_COOLDOWN_H", 4)),
            min_price=float(os.getenv("MIN_PRICE", 0.5))
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
    "DOAS.IS", "DOKTA.IS", "DURDO.IS", "DYOBY.IS", "EBEBK.IS", "ECZYT.IS", "EDATA.IS",
    "EDIP.IS", "EFORC.IS", "EGEEN.IS", "EGGUB.IS", "EGPRO.IS", "EGSER.IS", "EPLAS.IS", "EGEGY.IS", "ECILC.IS", "EKER.IS",
    "EKIZ.IS", "EKOS.IS", "EKSUN.IS", "ELITE.IS", "EMKEL.IS", "EMNIS.IS", 
    "DMLKT.IS", "EKGYO.IS", "ENJSA.IS", "ENERY.IS", "ENKAI.IS", "ENSRI.IS", "ERBOS.IS", "ERCB.IS",
    "EREGL.IS", "KIMMR.IS", "ERSU.IS", "ESCAR.IS", "ESCOM.IS", "ESEN.IS", "ETILR.IS", "EUKYO.IS", "EUYO.IS", "ETYAT.IS",
    "EUHOL.IS", "TEZOL.IS", "EUREN.IS", "EUPWR.IS", "EYGYO.IS", "FADE.IS", "FMIZP.IS", "FENER.IS",
    "FBBNK.IS", "FLAP.IS", "FONET.IS", "FROTO.IS", "FORMT.IS", "FORTE.IS", "FRIGO.IS", "FZLGY.IS", "GWIND.IS",
    "GSRAY.IS", "GARFA.IS", "GARFL.IS", "GRNYO.IS", "GEDIK.IS", "GEDZA.IS", "GLCVY.IS", "GENIL.IS", "GENTS.IS", "GEREL.IS",
    "GZNMI.IS", "GIPTA.IS", "GMTAS.IS", "GESAN.IS", "GLBMD.IS", "GLYHO.IS", "GOODY.IS",
    "GOKNR.IS", "GOLTS.IS", "GOZDE.IS", "GRTHO.IS", "GSDDE.IS", "GSDHO.IS", "GUBRF.IS", "GLRYH.IS", "GLRMK.IS", "GUNDG.IS",
    "GRSEL.IS", "SAHOL.IS", "HALKF.IS", "HLGYO.IS", "HLVKS.IS", "HALKI.IS", "HRKET.IS", "HATEK.IS", "HATSN.IS",
    "HDFGS.IS", "HEDEF.IS", "HEKTS.IS",
    "HTTBT.IS", "HOROZ.IS", "HUBVC.IS", "HUNER.IS", "HUZFA.IS", "HURGZ.IS", "ENTRA.IS", "ICBCT.IS"
]

# Test modu için farklı sembol seçimi
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"
SELECTED_SYMBOLS = ALL_BIST_STOCKS[:30] if TEST_MODE else ALL_BIST_STOCKS

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

# Cooldown fonksiyonu düzeltmesi
async def is_in_cooldown(symbol: str) -> bool:
    # datetime.datetime.utcnow() yerine datetime.now(timezone.utc) kullan
    lookback = dt.datetime.now(timezone.utc) - dt.timedelta(hours=CONFIG.signal_cooldown_h)
    
    recent_signals = _safe_read_jsonl(SIGNALS_LOG_PATH)[-1000:]

    for rec in reversed(recent_signals):
        if rec.get("symbol") != symbol:
            continue
        try:
            # ISO format string'i timezone-aware datetime'a çevir
            ts_str = rec["ts"]
            if ts_str.endswith('Z'):
                ts = dt.datetime.fromisoformat(ts_str[:-1]).replace(tzinfo=timezone.utc)
            elif '+' in ts_str or ts_str.endswith('+00:00'):
                ts = dt.datetime.fromisoformat(ts_str)
            else:
                # Timezone bilgisi yoksa UTC kabul et
                ts = dt.datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
                
            if ts >= lookback:
                return True
            if ts < lookback:
                break
        except (ValueError, KeyError):
            continue
    return False

# Mark signal fonksiyonu düzeltmesi  
async def mark_signal_sent(symbol: str, entry: float, score: float, rsi: float, signal_type: str):
    # UTC timezone ile timestamp oluştur
    ts = dt.datetime.now(timezone.utc).isoformat()
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
        try:
            return await self._fetch_yahoo(symbol, timeframe)
        except Exception as e:
            logger.debug(f"Yahoo Finance başarısız {symbol}: {e}")

        if TEST_MODE:
            return self._generate_mock_data(symbol)
        return None

    async def _fetch_yahoo(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
    """Yahoo Finance'den veri çek - Düzeltilmiş versiyon"""
    await RATE_LIMITER.acquire()
    
    # BIST sembol formatını doğru şekilde düzelt
    # Örnek: AGROT.IS -> AGROT.IS (zaten doğru)
    if symbol.endswith('.IS'):
        yf_symbol = symbol
    else:
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
            
            # Veri kontrolü ve temizleme
            closes = quote_data.get("close", [])
            volumes = quote_data.get("volume", [])
            highs = quote_data.get("high", [])
            lows = quote_data.get("low", [])
            opens = quote_data.get("open", [])
            
            # None değerleri filtrele
            valid_indices = []
            for i in range(len(timestamps)):
                if (i < len(closes) and closes[i] is not None and 
                    i < len(volumes) and volumes[i] is not None and
                    i < len(highs) and highs[i] is not None and
                    i < len(lows) and lows[i] is not None and
                    i < len(opens) and opens[i] is not None):
                    valid_indices.append(i)
            
            if len(valid_indices) < 50:
                raise ValueError("Yetersiz geçerli veri")
            
            # Sadece geçerli verileri al
            df_data = {
                "timestamp": [timestamps[i] for i in valid_indices[-100:]],
                "open": [opens[i] for i in valid_indices[-100:]],
                "high": [highs[i] for i in valid_indices[-100:]],
                "low": [lows[i] for i in valid_indices[-100:]],
                "close": [closes[i] for i in valid_indices[-100:]],
                "volume": [volumes[i] for i in valid_indices[-100:]]
            }
            
            df = pd.DataFrame(df_data)
            
            # Veri doğrulama
            if len(df) < 20:
                raise ValueError("Temizleme sonrası yetersiz veri")
            
            # Anormal fiyat kontrolü
            current_price = df.iloc[-1]["close"]
            if current_price <= 0 or current_price > 10000:  # BIST için makul fiyat aralığı
                logger.warning(f"{symbol} anormal fiyat: {current_price}")
            
            # Debug için fiyat logla
            logger.debug(f"{symbol} mevcut fiyat: {current_price:.2f}")
            
            df = self._calculate_all_indicators(df, timeframe)
            current = df.iloc[-1]
            
            return {
                "df": df,
                "current": {
                    "price": float(current["close"]),
                    "volume": float(current["volume"]),
                    "rsi": float(current.get("rsi", 50)),
                    "ema_9": float(current.get("ema_9", current["close"])),
                    "ema_21": float(current.get("ema_21", current["close"])),
                    "ema_50": float(current.get("ema_50", current["close"])),
                    "macd": float(current.get("macd", 0)),
                    "macd_signal": float(current.get("macd_signal", 0)),
                    "volume_ratio": float(current.get("volume_ratio", 1))
                }
            }
            
    except aiohttp.ClientError as e:
        raise RuntimeError(f"Aiohttp veri hatası: {e}")
    except (ValueError, KeyError) as e:
        raise ValueError(f"Yahoo Finance veri formatı hatası: {e}")
    except Exception as e:
        logger.error(f"{symbol} beklenmeyen hata: {e}")
        raise

    def _get_strategy_config(self, timeframe: str) -> Dict:
        """Strateji notlarına göre timeframe konfigürasyonu"""
        configs = {
            "15m": {
                "ema_periods": [9, 21],
                "rsi_period": 5,
                "macd_params": (6, 13, 4),
                "volume_period": 5,
                "min_rsi": 70,
                "rsi_oversold": 30,
                "rsi_bullish": 40
            },
            "1h": {
                "ema_periods": [21, 50],
                "rsi_period": 14,
                "macd_params": (12, 26, 9),
                "volume_period": 5,
                "min_rsi": 60,
                "rsi_oversold": 30,
                "rsi_bullish": 55
            },
            "4h": {
                "ema_periods": [13, 21, 34],
                "rsi_period": 14,
                "macd_params": (12, 26, 9),
                "volume_period": 20,
                "min_rsi": 55,
                "rsi_oversold": 30,
                "rsi_bullish": 50
            },
            "1d": {
                "ema_periods": [9, 21, 50],
                "rsi_period": 14,
                "macd_params": (12, 26, 9),
                "volume_period": 20,
                "min_rsi": 55,
                "rsi_oversold": 30,
                "rsi_bullish": 50
            }
        }
        return configs.get(timeframe, configs["1h"])

    def _calculate_all_indicators(self, df: pd.DataFrame, timeframe: str = "1h") -> pd.DataFrame:
        """Tüm teknik göstergeleri hesapla"""
        if len(df) < 20:
            return df
            
        try:
            close = df['close']
            config = self._get_strategy_config(timeframe)
            
            # Strateji notlarına göre EMA'lar
            for period in config["ema_periods"]:
                if len(df) >= period:
                    df[f'ema_{period}'] = close.ewm(span=period, adjust=False).mean()
                else:
                    df[f'ema_{period}'] = close
            
            # Ek önemli EMA'lar
            df['ema_20'] = close.ewm(span=20, adjust=False).mean()
            
            # Hacim analizleri
            volume_period = config["volume_period"]
            df[f'volume_sma_{volume_period}'] = df['volume'].rolling(volume_period, min_periods=1).mean()
            df['volume_sma_20'] = df['volume'].rolling(20, min_periods=1).mean()
            
            # Hacim oranı
            df['volume_ratio'] = df['volume'] / df[f'volume_sma_{volume_period}']
            df['volume_ratio'] = df['volume_ratio'].fillna(1.0)
            
            # RSI
            rsi_period = config["rsi_period"]
            if len(df) >= rsi_period:
                delta = close.diff()
                gain = delta.where(delta > 0, 0.0)
                loss = (-delta).where(delta < 0, 0.0)
                avg_gain = gain.rolling(window=rsi_period).mean()
                avg_loss = loss.rolling(window=rsi_period).mean()
                rs = avg_gain / avg_loss.replace(0, 1e-10)
                df['rsi'] = 100 - (100 / (1 + rs))
                df['rsi'] = df['rsi'].fillna(50)
            else:
                df['rsi'] = 50.0
            
            # MACD
            if len(df) >= max(config["macd_params"]):
                fast, slow, signal = config["macd_params"]
                ema_fast = close.ewm(span=fast, adjust=False).mean()
                ema_slow = close.ewm(span=slow, adjust=False).mean()
                df['macd'] = ema_fast - ema_slow
                df['macd_signal'] = df['macd'].ewm(span=signal, adjust=False).mean()
                df['macd_histogram'] = df['macd'] - df['macd_signal']
            else:
                df['macd'] = 0.0
                df['macd_signal'] = 0.0
                df['macd_histogram'] = 0.0
            
            # Support/Resistance
            df['support_level'] = df['low'].rolling(10, min_periods=1).min()
            df['resistance_level'] = df['high'].rolling(10, min_periods=1).max()
            
            return df.replace([np.inf, -np.inf], np.nan).ffill()
            
        except Exception as e:
            logger.error(f"Gösterge hesaplama hatası: {e}")
            return df

    def _generate_mock_data(self, symbol: str) -> Dict[str, Any]:
        """Test modu için mock data"""
        if not TEST_MODE:
            return None
            
        # Basit mock data üret
        dates = pd.date_range(start='2024-01-01', periods=100, freq='1h')
        np.random.seed(hash(symbol) % 2**32)
        
        prices = 100 + np.cumsum(np.random.randn(100) * 0.5)
        volumes = np.random.randint(1000, 10000, 100)
        
        df = pd.DataFrame({
            'timestamp': dates,
            'open': prices * (1 + np.random.randn(100) * 0.001),
            'high': prices * (1 + abs(np.random.randn(100)) * 0.005),
            'low': prices * (1 - abs(np.random.randn(100)) * 0.005),
            'close': prices,
            'volume': volumes
        })
        
        df = self._calculate_all_indicators(df, "1h")
        current = df.iloc[-1]
        
        return {
            "df": df,
            "current": {
                "price": float(current["close"]),
                "volume": float(current["volume"]),
                "rsi": float(current.get("rsi", 50)),
                "ema_9": float(current.get("ema_9", current["close"])),
                "ema_21": float(current.get("ema_21", current["close"])),
                "ema_50": float(current.get("ema_50", current["close"])),
                "macd": float(current.get("macd", 0)),
                "macd_signal": float(current.get("macd_signal", 0)),
                "volume_ratio": float(current.get("volume_ratio", 1))
            }
        }

# -------------------- STRATEJİ ODAKLI SİNYAL ANALİZİ --------------------
class StrategyAlignedBuyAnalyzer:
    def __init__(self, config: BistBotConfig):
        self.config = config

    def analyze_symbol(self, df: pd.DataFrame, timeframe: str = "1h") -> Dict[str, Any]:
        """Sembol analizi - Strateji odaklı"""
        if len(df) < 30:
            return {'score': 0.0, 'signals': {}, 'current_values': {}}

        try:
            current = df.iloc[-1]
            previous = df.iloc[-2] if len(df) > 1 else current
            
            if pd.isna(current['close']) or current['close'] <= 0:
                return {'score': 0.0, 'signals': {}, 'current_values': {}}
            
            # Kategori bazlı puanlama - her kategori için maksimum sınır
            kategori_puanlari = {
                'rsi': 0.0,        # Maks 2.0 puan
                'macd': 0.0,       # Maks 1.5 puan  
                'ema': 0.0,        # Maks 2.0 puan
                'hacim': 0.0,      # Maks 1.5 puan
                'destek': 0.0,     # Maks 1.0 puan
                'momentum': 0.0    # Maks 0.5 puan (bonus)
            }
            
            signals = {}
            
            # RSI Analizi
            rsi_puan, rsi_sinyaller = self._analyze_rsi_strategy(df, current, previous, timeframe)
            kategori_puanlari['rsi'] = min(rsi_puan, 2.0)
            signals.update(rsi_sinyaller)
            
            # MACD Analizi
            macd_puan, macd_sinyaller = self._analyze_macd_strategy(df, current, previous, timeframe)
            kategori_puanlari['macd'] = min(macd_puan, 1.5)
            signals.update(macd_sinyaller)
            
            # EMA Trend Analizi
            ema_puan, ema_sinyaller = self._analyze_ema_strategy(df, current, previous, timeframe)
            kategori_puanlari['ema'] = min(ema_puan, 2.0)
            signals.update(ema_sinyaller)
            
            # Hacim Analizi
            hacim_puan, hacim_sinyaller = self._analyze_volume_strategy(df, current, previous, timeframe)
            kategori_puanlari['hacim'] = min(hacim_puan, 1.5)
            signals.update(hacim_sinyaller)
            
            # Destek/Direnç
            destek_puan, destek_sinyaller = self._analyze_support_resistance(df, current, previous, timeframe)
            kategori_puanlari['destek'] = min(destek_puan, 1.0)
            signals.update(destek_sinyaller)
            
            # Momentum Bonusu
            momentum_puan, momentum_sinyaller = self._detect_momentum(df, current, previous, timeframe)
            kategori_puanlari['momentum'] = min(momentum_puan * 0.15, 0.5)
            signals.update(momentum_sinyaller)
            
            # Temel puan hesaplama
            temel_puan = sum(kategori_puanlari.values())
            
            # Kalite kontrolleri ve cezalar
            cezalar = 0.0
            bonuslar = 0.0
            
            # Hacim oranı kontrolü - KRİTİK
            volume_ratio = current.get('volume_ratio', 1.0)
            if volume_ratio < self.config.min_volume_ratio:
                cezalar += 1.0  # Düşük hacim için ceza
                signals['dusuk_hacim_cezasi'] = True
            
            # Fiyat kontrolü
            if current['close'] < self.config.min_price:
                return {'score': 0.0, 'signals': {}, 'current_values': {}}
            
            # Aktif sinyal sayısı
            aktif_sinyal_sayisi = sum(1 for sig in signals.values() if sig is True)
            
            if aktif_sinyal_sayisi < self.config.min_active_signals:
                ceza = (self.config.min_active_signals - aktif_sinyal_sayisi) * 0.3
                cezalar += ceza
                signals['min_sinyal_cezasi'] = ceza
            
            # Zaman dilimi güvenilirlik çarpanı
            zaman_dilimi_carpanlari = {
                "15m": 0.85,  # Scalping daha az güvenilir
                "1h": 1.0,    # Ana strateji
                "4h": 1.1,    # Çok güvenilir
                "1d": 1.15    # En güvenilir ama yavaş
            }
            
            guvenilirlik_carpani = zaman_dilimi_carpanlari.get(timeframe, 1.0)
            
            # Son puan hesaplama
            ayarlanmis_puan = (temel_puan * guvenilirlik_carpani) - cezalar + bonuslar
            final_puan = max(0.0, min(ayarlanmis_puan, 6.0))  # Üst sınır 6.0
            
            # Çok düşük RSI durumunda bonus ver (gerçek fırsat olabilir)
            current_rsi = current.get('rsi', 50)
            if current_rsi < 25 and volume_ratio > 2.0:
                bonuslar += 0.3
                signals['asiri_satis_firsati'] = True
                final_puan = min(final_puan + 0.3, 6.0)
            
            result = {
                'score': round(final_puan, 2),
                'signals': signals,
                'current_values': {
                    'price': float(current['close']),
                    'rsi': float(current.get('rsi', 50)),
                    'volume_ratio': round(float(volume_ratio), 2),
                    'active_signals_count': aktif_sinyal_sayisi,
                    'timeframe': timeframe,
                    'kategori_dagilimi': {k: round(v, 2) for k, v in kategori_puanlari.items()},
                    'guvenilirlik_carpani': guvenilirlik_carpani,
                    'toplam_ceza': round(cezalar, 2),
                    'macd': float(current.get('macd', 0)),
                    'macd_signal': float(current.get('macd_signal', 0))
                }
            }
            
            return result

        except Exception as e:
            logger.error(f"Sembol analiz hatası: {e}")
            return {'score': 0.0, 'signals': {}, 'current_values': {}}

    def _analyze_rsi_strategy(self, df: pd.DataFrame, current: pd.Series, previous: pd.Series, timeframe: str) -> Tuple[float, Dict]:
        """RSI analizi - Strateji odaklı"""
        score = 0.0
        signals = {}
        
        try:
            current_rsi = current.get('rsi', 50)
            prev_rsi = previous.get('rsi', 50)
            
            if pd.isna(current_rsi) or pd.isna(prev_rsi):
                return 0.0, {}
            
            # RSI 30 altından yukarı kesişim (en güçlü sinyal)
            if prev_rsi <= 30 and current_rsi > 30:
                score += 1.5
                signals['rsi_30_yukari_kesisim'] = True
            
            # RSI aşırı satım bölgesinden çıkış
            elif prev_rsi < 35 and current_rsi >= 35:
                score += 1.0
                signals['rsi_asiri_satim_cikis'] = True
            
            # RSI boğa bölgesinde (50+)
            if current_rsi >= 50:
                score += 0.5
                signals['rsi_boga_bolgesi'] = True
            
            # RSI momentum artışı
            if current_rsi > prev_rsi + 3:
                score += 0.3
                signals['rsi_momentum_artisi'] = True
            
            # Aşırı alım kontrolü (ceza)
            if current_rsi > 80:
                score -= 0.5
                signals['rsi_asiri_alim_ceza'] = True
            
            signals['mevcut_rsi'] = round(current_rsi, 2)
            return min(score, 2.0), signals
            
        except Exception:
            return 0.0, {}

    def _analyze_macd_strategy(self, df: pd.DataFrame, current: pd.Series, previous: pd.Series, timeframe: str) -> Tuple[float, Dict]:
        """MACD analizi"""
        score = 0.0
        signals = {}
        
        try:
            current_macd = current.get('macd', 0)
            current_signal = current.get('macd_signal', 0)
            prev_macd = previous.get('macd', 0)
            prev_signal = previous.get('macd_signal', 0)
            
            if any(pd.isna(x) for x in [current_macd, current_signal, prev_macd, prev_signal]):
                return 0.0, {}
            
            # MACD sıfır üstü
            if current_macd > 0:
                score += 0.4
                signals['macd_sifir_ustu'] = True
            
            # MACD pozitif kesişim (en önemli)
            if (prev_macd <= prev_signal and current_macd > current_signal):
                if current_macd > 0:
                    score += 1.0
                    signals['macd_boga_kesisim_sifir_ustu'] = True
                else:
                    score += 0.8
                    signals['macd_boga_kesisim_sifir_alti'] = True
            
            # MACD histogram yükselişi
            current_hist = current_macd - current_signal
            prev_hist = prev_macd - prev_signal
            if current_hist > prev_hist:
                score += 0.3
                signals['macd_histogram_yukselis'] = True
            
            return min(score, 1.5), signals
            
        except Exception:
            return 0.0, {}

    def _analyze_ema_strategy(self, df: pd.DataFrame, current: pd.Series, previous: pd.Series, timeframe: str) -> Tuple[float, Dict]:
        """EMA trend analizi"""
        score = 0.0
        signals = {}
        
        try:
            price = current['close']
            prev_price = previous['close']
            
            # Timeframe'e göre EMA analizleri
            if timeframe == "15m":
                ema9 = current.get('ema_9', price)
                ema21 = current.get('ema_21', price)
                
                if not pd.isna(ema9) and not pd.isna(ema21):
                    # Fiyat EMA9 üstü
                    if price > ema9:
                        score += 0.6
                        signals['fiyat_ema9_ustu'] = True
                    
                    # EMA9 > EMA21 (boğa trendi)
                    if ema9 > ema21:
                        score += 0.8
                        signals['ema9_ema21_ustu'] = True
                    
                    # EMA kesişimi
                    prev_ema9 = previous.get('ema_9', ema9)
                    prev_ema21 = previous.get('ema_21', ema21)
                    if prev_ema9 <= prev_ema21 and ema9 > ema21:
                        score += 1.2
                        signals['golden_cross_9_21'] = True
                        
            elif timeframe == "1h":
                ema21 = current.get('ema_21', price)
                ema50 = current.get('ema_50', price)
                
                if not pd.isna(ema21) and not pd.isna(ema50):
                    # Fiyat her iki EMA üstü
                    if price > ema21 and price > ema50:
                        score += 0.7
                        signals['fiyat_ana_ema_ustu'] = True
                    
                    # EMA21 > EMA50
                    if ema21 > ema50:
                        score += 0.6
                        signals['ema_boga_siralama'] = True
                    
                    # Golden Cross
                    prev_ema21 = previous.get('ema_21', ema21)
                    prev_ema50 = previous.get('ema_50', ema50)
                    if prev_ema21 <= prev_ema50 and ema21 > ema50:
                        score += 1.0
                        signals['golden_cross_21_50'] = True
            
            elif timeframe == "4h":
                ema13 = current.get('ema_13', price)
                ema21 = current.get('ema_21', price)
                ema34 = current.get('ema_34', price)
                
                if all(not pd.isna(x) for x in [ema13, ema21, ema34]):
                    # Fibonacci EMA sıralaması
                    if ema13 > ema21 > ema34 and price > ema13:
                        score += 1.2
                        signals['fibonacci_ema_siralama'] = True
                    elif price > ema21:
                        score += 0.5
                        signals['fiyat_ema21_ustu'] = True
            
            # 20 EMA kırılımı (tüm timeframelerde)
            ema20 = current.get('ema_20', price)
            if not pd.isna(ema20):
                prev_ema20 = previous.get('ema_20', ema20)
                if prev_price <= prev_ema20 and price > ema20:
                    score += 0.6
                    signals['ema20_kirilim'] = True
            
            return min(score, 2.0), signals
            
        except Exception:
            return 0.0, {}

    def _analyze_volume_strategy(self, df: pd.DataFrame, current: pd.Series, previous: pd.Series, timeframe: str) -> Tuple[float, Dict]:
        """Hacim analizi"""
        score = 0.0
        signals = {}
        
        try:
            current_volume = current['volume']
            volume_ratio = current.get('volume_ratio', 1.0)
            
            if pd.isna(current_volume) or current_volume <= 0:
                return 0.0, {}
            
            # Hacim artışı seviyeleri
            if volume_ratio >= 1.5:
                score += 0.4
                signals['hacim_150pct_ustu'] = True
                
                if volume_ratio >= 2.0:
                    score += 0.6
                    signals['hacim_spike_2x'] = True
                    
                    if volume_ratio >= 3.0:
                        score += 0.5
                        signals['hacim_spike_3x'] = True
            
            # Timeframe'e göre hacim değerlendirmesi
            if timeframe in ['15m', '1h']:
                # Kısa vadeli hacim patlaması daha önemli
                if volume_ratio >= 1.8:
                    score += 0.3
                    signals['kisa_vade_hacim_patlama'] = True
            else:
                # Uzun vadeli sürdürülebilir hacim
                if volume_ratio >= 1.3:
                    score += 0.2
                    signals['surdurulebilir_hacim'] = True
            
            signals['hacim_orani'] = round(volume_ratio, 2)
            return min(score, 1.5), signals
            
        except Exception:
            return 0.0, {}

    def _analyze_support_resistance(self, df: pd.DataFrame, current: pd.Series, previous: pd.Series, timeframe: str) -> Tuple[float, Dict]:
        """Destek-direnç analizi"""
        score = 0.0
        signals = {}
        
        try:
            price = current['close']
            support = current.get('support_level', 0)
            resistance = current.get('resistance_level', 0)
            
            if pd.isna(support) or support <= 0:
                return 0.0, {}
            
            # Destek seviyesine yakınlık
            distance_from_support = (price - support) / support
            if 0 <= distance_from_support <= 0.03:
                score += 0.8
                signals['destek_yakin'] = True
                signals['destek_mesafe_yuzde'] = round(distance_from_support * 100, 2)
                
                # Çok yakınsa bonus
                if distance_from_support <= 0.01:
                    score += 0.2
                    signals['destek_cok_yakin'] = True
            
            # Direnç seviyesine uzaklık - potansiyel
            if not pd.isna(resistance) and resistance > 0:
                distance_to_resistance = (resistance - price) / price
                if distance_to_resistance >= 0.05:
                    score += 0.2
                    signals['yukari_potansiyel'] = True
            
            return min(score, 1.0), signals
            
        except Exception:
            return 0.0, {}

    def _detect_momentum(self, df: pd.DataFrame, current: pd.Series, previous: pd.Series, timeframe: str) -> Tuple[float, Dict]:
        """Momentum tespiti"""
        score = 0.0
        signals = {}
        
        try:
            if len(df) < 10:
                return 0.0, {}
            
            recent_data = df.tail(10)
            
            # Fiyat momentum
            price_changes = recent_data['close'].pct_change().fillna(0)
            positive_days = (price_changes > 0).sum()
            
            if positive_days >= 6:  # Son 10 günün 6'sında artış
                score += 1.5
                signals['guclu_fiyat_momentum'] = True
            elif positive_days >= 4:
                score += 1.0
                signals['pozitif_fiyat_momentum'] = True
            
            # RSI momentum
            current_rsi = current.get('rsi', 50)
            rsi_3_ago = recent_data['rsi'].iloc[-4] if len(recent_data) >= 4 else current_rsi
            
            if not pd.isna(rsi_3_ago) and current_rsi > rsi_3_ago + 5:
                score += 1.0
                signals['rsi_momentum'] = True
            
            # Hacim momentum
            volume_trend = recent_data['volume'].rolling(3).mean().pct_change().iloc[-1]
            if not pd.isna(volume_trend) and volume_trend > 0.1:
                score += 0.5
                signals['hacim_momentum'] = True
            
            return score, signals
            
        except Exception:
            return 0.0, {}


class BistSignalAnalyzer:
    def __init__(self, config: BistBotConfig):
        self.config = config
        self.analyzer = StrategyAlignedBuyAnalyzer(config)

    def analyze_symbol(self, symbol: str, timeframe_data: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Sembol analizi - Multi-timeframe"""
        if not timeframe_data:
            return None
            
        # Ana timeframe seçimi
        main_tf = "1h" if "1h" in timeframe_data else list(timeframe_data.keys())[0]
        main_data = timeframe_data[main_tf]
        
        current = main_data["current"]
        df = main_data["df"]
        
        # Temel filtreler
        if current["price"] < self.config.min_price:
            return None
            
        volume_try = current["price"] * current["volume"]
        if volume_try < self.config.min_volume_try:
            return None
        
        # Ana analiz
        analysis = self.analyzer.analyze_symbol(df, main_tf)
        
        if analysis['score'] < self.config.min_signal_score:
            return None
        
        # Multi-timeframe tutarlılık kontrolü
        timeframe_scores = {}
        for tf, data in timeframe_data.items():
            tf_analysis = self.analyzer.analyze_symbol(data["df"], tf)
            timeframe_scores[tf] = tf_analysis['score']
        
        # En az 2 timeframe'de pozitif sinyal olmalı
        positive_tf_count = sum(1 for score in timeframe_scores.values() if score > 1.0)
        if positive_tf_count < 2 and len(timeframe_data) > 1:
            return None
        
        return {
            "symbol": symbol,
            "direction": "BUY",
            "score": analysis['score'],
            "price": current["price"],
            "rsi": current["rsi"],
            "volume_try": volume_try,
            "signals": analysis['signals'],
            "current_values": analysis['current_values'],
            "timeframe_scores": timeframe_scores
        }


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
        except Exception as e:
            logger.error(f"Telegram gönderim hatası: {e}")

    async def send_chart(self, symbol: str, df: pd.DataFrame, signal_info: Dict[str, Any]):
        """Grafik oluşturup Telegram'a gönder"""
        try:
            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 10), 
                                              gridspec_kw={'height_ratios': [3, 1, 1]})
            
            recent_data = df.tail(50)
            dates = list(range(len(recent_data)))
            
            # Fiyat ve EMA'lar
            ax1.plot(dates, recent_data["close"], label="Fiyat", color='dodgerblue', linewidth=2)
            
            if 'ema_21' in recent_data.columns:
                ax1.plot(dates, recent_data["ema_21"], label="EMA21", color='orange', linestyle='--')
            if 'ema_50' in recent_data.columns:
                ax1.plot(dates, recent_data["ema_50"], label="EMA50", color='red', linestyle='--')
            
            ax1.set_title(f"{symbol} - BUY Sinyali (Skor: {signal_info['score']})", 
                         fontsize=14, fontweight='bold')
            ax1.legend()
            ax1.grid(True, alpha=0.3)
            
            # RSI
            if 'rsi' in recent_data.columns:
                ax2.plot(dates, recent_data["rsi"], label="RSI", color='green')
                ax2.axhline(70, linestyle='--', color='red', alpha=0.5, label='Aşırı Alım')
                ax2.axhline(30, linestyle='--', color='green', alpha=0.5, label='Aşırı Satım')
                ax2.axhline(50, linestyle='-', color='gray', alpha=0.3)
                ax2.set_ylim(0, 100)
                ax2.set_title(f"RSI: {signal_info['rsi']:.1f}", fontsize=12)
                ax2.legend()
                ax2.grid(True, alpha=0.3)
            
            # MACD
            if all(col in recent_data.columns for col in ['macd', 'macd_signal']):
                ax3.plot(dates, recent_data["macd"], label="MACD", color='blue')
                ax3.plot(dates, recent_data["macd_signal"], label="Signal", color='red')
                ax3.axhline(0, linestyle='-', color='black', alpha=0.3)
                ax3.set_title("MACD", fontsize=12)
                ax3.legend()
                ax3.grid(True, alpha=0.3)
            
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
            
        except Exception as e:
            logger.error(f"Grafik oluşturma hatası: {e}")


# -------------------- SİNYAL BİLDİRİMLERİ --------------------
async def send_signal_notification(telegram: BistTelegramNotifier, signal_info: Dict[str, Any],
                                   timeframe_data: Dict[str, Dict[str, Any]]):
    """Sinyali formatla ve Telegram'a gönder"""
    try:
        symbol = signal_info["symbol"]
        score = signal_info["score"]
        price = signal_info["price"]
        rsi = signal_info["rsi"]
        
        current_values = signal_info.get("current_values", {})
        
        msg_lines = [
            f"🚀 <b>{symbol}</b> — <b>BUY SİNYALİ</b>",
            f"Skor: <b>{score}</b>/6.0",
            f"Fiyat: <b>{price:.2f} TRY</b>",
            f"RSI: <b>{rsi:.1f}</b>",
            f"Hacim Oranı: <b>{current_values.get('volume_ratio', 1.0):.1f}x</b>",
            f"Aktif Sinyal: <b>{current_values.get('active_signals_count', 0)}</b>",
            "",
            "<b>Zaman Dilimleri:</b>"
        ]
        
        for tf, tf_score in signal_info.get("timeframe_scores", {}).items():
            msg_lines.append(f"- {tf}: {tf_score:.2f}")
        
        msg_lines.append("")
        msg_lines.append("<b>Kategori Dağılımı:</b>")
        for kategori, puan in current_values.get("kategori_dagilimi", {}).items():
            if puan > 0:
                msg_lines.append(f"- {kategori}: {puan:.1f}")
        
        # Önemli sinyalleri göster
        msg_lines.append("")
        msg_lines.append("<b>Önemli Sinyaller:</b>")
        important_signals = []
        for sig_name, sig_value in signal_info.get("signals", {}).items():
            if sig_value is True:
                # Sinyal isimlerini Türkçe'ye çevir
                turkish_names = {
                    'rsi_30_yukari_kesisim': 'RSI 30 Yukarı Kesişim',
                    'rsi_asiri_satim_cikis': 'RSI Aşırı Satımdan Çıkış',
                    'macd_boga_kesisim_sifir_ustu': 'MACD Boğa Kesişimi (0 Üstü)',
                    'golden_cross_21_50': 'Golden Cross (21/50)',
                    'hacim_spike_2x': 'Hacim Patlaması (2x)',
                    'destek_yakin': 'Destek Seviyesi Yakın',
                    'guclu_fiyat_momentum': 'Güçlü Fiyat Momentum'
                }
                display_name = turkish_names.get(sig_name, sig_name)
                important_signals.append(display_name)
        
        if important_signals:
            for sig in important_signals[:5]:  # En fazla 5 tane göster
                msg_lines.append(f"✅ {sig}")
        
        message = "\n".join(msg_lines)
        await telegram.send_message(message)
        
        # Grafik gönderimi devre dışı
        # main_tf = "1h" if "1h" in timeframe_data else next(iter(timeframe_data))
        # main_df = timeframe_data[main_tf]["df"]
        # await telegram.send_chart(symbol, main_df, signal_info)
        
    except Exception as e:
        logger.exception(f"Sinyal bildirimi gönderilemedi: {e}")

async def send_scan_summary(telegram: BistTelegramNotifier, results: List[Dict[str, Any]], elapsed: float):
    """Tarama özeti gönder"""
    try:
        top_n = min(5, len(results))
        lines = [
            f"📊 Tarama tamamlandı — {len(results)} sinyal bulundu",
            f"Süre: {elapsed:.1f}s",
            "",
            "<b>En iyi sinyaller:</b>"
        ]
        
        for r in results[:top_n]:
            lines.append(
                f"• {r['symbol']} — Skor: {r['score']:.1f} — "
                f"Fiyat: {r['price']:.2f} TRY — RSI: {r['rsi']:.1f}"
            )
        
        await telegram.send_message("\n".join(lines))
        
    except Exception as e:
        logger.exception(f"Özet gönderilemedi: {e}")


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
                    logger.debug(f"{symbol} cooldown'da")
                    return None
                
                timeframe_data = {}
                for tf in CONFIG.timeframes:
                    try:
                        data = await data_client._fetch_with_fallback(symbol, tf)
                        if data:
                            # Fiyat doğrulaması ekle
                            if not await data_client.validate_price_data(symbol, data):
                                logger.warning(f"{symbol} {tf} fiyat doğrulaması başarısız - veri atlandı")
                                continue
                            timeframe_data[tf] = data
                            
                            # Debug için fiyat bilgilerini logla
                            current_price = data["current"]["price"]
                            logger.debug(f"{symbol} {tf} fiyat: {current_price:.2f} TL")
                            
                    except Exception as e:
                        logger.debug(f"{symbol} {tf} veri hatası: {e}")
                        continue
                
                if not timeframe_data:
                    logger.debug(f"{symbol} - tüm timeframelerde veri alınamadı")
                    return None
                
                signal_info = analyzer.analyze_symbol(symbol, timeframe_data)
                if signal_info:
                    # Telegram bildiriminden önce fiyat kontrolü
                    final_price = signal_info["price"]
                    logger.info(f"✅ {symbol} BUY SİNYAL - Fiyat: {final_price:.2f} TL, Skor: {signal_info['score']:.2f}")
                    
                    if telegram:
                        await send_signal_notification(telegram, signal_info, timeframe_data)
                    
                    await mark_signal_sent(
                        signal_info["symbol"],
                        signal_info["price"],
                        signal_info["score"],
                        signal_info["rsi"],
                        signal_info["direction"]
                    )
                    
                    return signal_info
                
                return None
                
            except Exception as e:
                logger.warning(f"{symbol} analiz hatası: {e}")
                return None
    
    # Sembol analizlerini paralel olarak çalıştır
    tasks = [analyze_single_symbol(symbol) for symbol in SELECTED_SYMBOLS[:CONFIG.max_symbols]]
    completed = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Sonuçları topla
    for result in completed:
        if isinstance(result, dict):
            results.append(result)
    
    # Skora göre sırala
    results.sort(key=lambda x: x["score"], reverse=True)
    elapsed = time.time() - start_time
    
    logger.info(f"✅ Tarama tamamlandı: {elapsed:.1f}s - {len(results)} sinyal bulundu")
    
    # Özet gönder
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
    interval_sec = int(os.getenv("SCAN_INTERVAL_SEC", "600"))  # 10 dakika
    
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
    logger.info(f"Konfigürasyon: min_score={CONFIG.min_signal_score}, min_volume_try={CONFIG.min_volume_try:,.0f}")
    
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
