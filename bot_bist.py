#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import asyncio
import aiohttp
from aiohttp import web
import datetime as dt
import logging
import json
import time
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

# --- 1. AYARLAR VE GÜVENLİK ---
# Ortam değişkenlerinden çekilir.
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
COLLECTAPI_KEY = os.getenv("COLLECTAPI_KEY")

if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, COLLECTAPI_KEY]):
    raise ValueError("Ortam değişkenleri (TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, COLLECTAPI_KEY) ayarlanmalı!")

# Diğer Ayarlar
LOG_FILE = 'trading_bot.log'
DATA_CACHE_DIR = "data_cache"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
HEALTH_CHECK_PORT = int(os.getenv("PORT", 8080))
IST_TZ = pytz.timezone('Europe/Istanbul')

# Loglama ayarları
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
logger = logging.getLogger("bist_scanner")

# ----------------------- 2. BORSA VE TARAMA AYARLARI -----------------------
MARKET_OPEN_HOUR = 9
MARKET_CLOSE_HOUR = 18
MARKET_CLOSE_MINUTE = 30
DAILY_REPORT_HOUR = 18
DAILY_REPORT_MINUTE = 30
CHECK_EVERY_MIN = 15
TIMEFRAMES = ["1d", "4h", "1h", "15m"]
TICKERS = ["THYAO", "SAHOL", "ASTOR", "AKBNK", "SISE", "BIMAS"] # Örnek hisseler
MIN_PRICE = 1.0
MIN_VOLUME_TRY = 1000000
MIN_SIGNAL_SCORE = 8.0 
RSI_LEN, MACD_FAST, MACD_SLOW, MACD_SIGNAL, MA_SHORT, MA_LONG = 22, 12, 26, 9, 50, 200

LAST_SCAN_TIME: Optional[dt.datetime] = None
DAILY_SIGNALS: Dict[str, Dict] = {}

# Sinyal bilgi sınıfı
@dataclass
class SignalInfo:
    symbol: str
    timeframe: str
    direction: str
    price: float
    rsi: float
    volume_ratio: float
    strength_score: float
    timestamp: str
    macd_signal: str

# ----------------------- 3. YARDIMCI VE TEKNİK ANALİZ FONKSİYONLARI -----------------------
def is_market_hours() -> bool:
    """Borsa çalışma saatleri kontrolü"""
    now_ist = dt.datetime.now(IST_TZ)
    if now_ist.weekday() >= 5: return False
    market_open = dt.time(MARKET_OPEN_HOUR, 0)
    market_close = dt.time(MARKET_CLOSE_HOUR, MARKET_CLOSE_MINUTE)
    return market_open <= now_ist.time() <= market_close

async def send_telegram(text: str, photo: Optional[str] = None):
    """Telegram'a mesaj ve/veya fotoğraf gönderir."""
    try:
        async with aiohttp.ClientSession() as session:
            if photo:
                with open(photo, 'rb') as f:
                    data = aiohttp.FormData()
                    data.add_field('chat_id', TELEGRAM_CHAT_ID)
                    data.add_field('caption', text, content_type='text/html')
                    data.add_field('photo', f, filename=photo, content_type='image/png')
                    await session.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto", data=data, timeout=30)
            else:
                payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
                await session.post(f"{TELEGRAM_API_URL}", json=payload, timeout=15)
    except Exception as e:
        logger.warning(f"Telegram error: {e}")

def rsi_from_close(close: pd.Series, length: int) -> pd.Series:
    """RSI hesaplar."""
    delta = close.diff()
    up, down = delta.clip(lower=0), -delta.clip(upper=0)
    up_rma = up.ewm(alpha=1/length, adjust=False).mean()
    down_rma = down.ewm(alpha=1/length, adjust=False).mean()
    rs = up_rma / down_rma.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def sma(series: pd.Series, length: int) -> pd.Series:
    """Basit hareketli ortalama (SMA) hesaplar."""
    return series.rolling(window=length).mean()

def create_and_save_plot(df: pd.DataFrame, signal: SignalInfo, filename: str):
    """Grafik oluşturur ve dosyaya kaydeder."""
    plt.style.use('seaborn-v0_8-darkgrid')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), gridspec_kw={'height_ratios': [3, 1]})
    
    # Mum grafiği
    ax1.plot(df.index, df['close'], label='Fiyat', color='blue')
    ax1.plot(df.index, sma(df['close'], MA_SHORT), label=f'MA({MA_SHORT})', color='green')
    ax1.plot(df.index, sma(df['close'], MA_LONG), label=f'MA({MA_LONG})', color='red')
    ax1.set_title(f"{signal.symbol}.IS - {signal.timeframe} Analiz Grafiği", fontsize=16)
    ax1.set_ylabel("Fiyat (TL)", fontsize=12)
    ax1.legend()
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    signal_point = df.index[-1]
    ax1.plot(signal_point, signal.price, marker='^' if signal.direction == "BULLISH" else 'v', 
             color='green' if signal.direction == "BULLISH" else 'red', markersize=10, zorder=5)
    
    # RSI
    rsi_series = rsi_from_close(df['close'], RSI_LEN)
    ax2.plot(df.index, rsi_series, label=f'RSI({RSI_LEN})', color='purple')
    ax2.axhline(70, linestyle='--', color='red', alpha=0.7)
    ax2.axhline(30, linestyle='--', color='green', alpha=0.7)
    ax2.set_ylabel("RSI", fontsize=12)
    ax2.set_xlabel("Tarih", fontsize=12)
    ax2.legend()
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    plt.tight_layout()
    plt.savefig(filename)
    plt.close(fig)

# ----------------------- 4. VERİ ÇEKME & ÖN BELLEKLEME -----------------------
def get_live_price_from_collectapi(symbol: str) -> Optional[float]:
    """CollectAPI'den anlık fiyatı çeker."""
    try:
        conn = http.client.HTTPSConnection("api.collectapi.com")
        headers = {'content-type': "application/json", 'authorization': f"apikey {COLLECTAPI_KEY}"}
        conn.request("GET", f"/economy/liveBorsa?q={symbol}", headers=headers)
        res = conn.getresponse()
        data = res.read()
        json_data = json.loads(data.decode("utf-8"))
        if json_data.get('success') and json_data.get('result'):
            return float(json_data['result'][0]['pricestr'].replace(',', '.'))
    except Exception as e:
        logger.error(f"CollectAPI anlık fiyat çekme hatası: {e}")
    return None

def fetch_and_update_data(symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
    """Veriyi CSV'den çeker, eksik veriyi yfinance'den tamamlar ve anlık fiyatı günceller."""
    yf_symbol = f"{symbol}.IS"
    cache_path = os.path.join(DATA_CACHE_DIR, f"{yf_symbol}_{timeframe}.csv")
    period_map = {'1d': '730d', '4h': '180d', '1h': '180d', '15m': '60d'}
    interval_map = {'1d': '1d', '4h': '1h', '1h': '1h', '15m': '15m'}

    try:
        if os.path.exists(cache_path):
            df_cache = pd.read_csv(cache_path, index_col=0, parse_dates=True)
            df_cache.index = df_cache.index.tz_localize(IST_TZ)
            last_date = df_cache.index.max()
            new_data_period = f"{int((dt.datetime.now(IST_TZ) - last_date).total_seconds() / 86400) + 1}d"
        else:
            df_cache = pd.DataFrame()
            new_data_period = period_map[timeframe]

        df_new = yf.Ticker(yf_symbol).history(period=new_data_period, interval=interval_map[timeframe], auto_adjust=False)
        if df_new.empty:
            df = df_cache
        else:
            df_new.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'}, inplace=True)
            if df_new.index.tz is None:
                df_new.index = df_new.index.tz_localize('UTC').tz_convert(IST_TZ)
            else:
                df_new.index = df_new.index.tz_convert(IST_TZ)
            df = pd.concat([df_cache, df_new]).drop_duplicates().sort_index()

        if timeframe == '4h':
            agg = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}
            df = df.resample('4H').agg(agg).dropna()
        
        live_price = get_live_price_from_collectapi(symbol)
        if live_price and not df.empty:
            last_index = df.index[-1]
            df.loc[last_index, 'close'] = live_price
            df.loc[last_index, 'high'] = max(df.loc[last_index, 'high'], live_price)
            df.loc[last_index, 'low'] = min(df.loc[last_index, 'low'], live_price)

        df.to_csv(cache_path)
        return df.dropna().tail(max(300, MA_LONG + 10))

    except Exception as e:
        logger.error(f"Veri çekme/güncelleme hatası {symbol} {timeframe}: {e}")
        return None

# ----------------------- 5. SİNYAL ANALİZİ VE PUANLAMA -----------------------
def calculate_signal_strength(signal: SignalInfo) -> float:
    """Sinyal gücünü puanlar."""
    score = 5.0
    if signal.direction == "BULLISH":
        if signal.rsi < 30: score += 3.0
        elif signal.rsi < 50: score += 1.0
    else:
        if signal.rsi > 70: score += 3.0
        elif signal.rsi > 50: score += 1.0
    if signal.volume_ratio > 3.0: score += 2.5
    elif signal.volume_ratio > 2.0: score += 2.0
    elif signal.volume_ratio > 1.5: score += 1.0
    if (signal.direction == "BULLISH" and signal.macd_signal == "BULLISH") or \
       (signal.direction == "BEARISH" and signal.macd_signal == "BEARISH"):
        score += 1.5
    return float(max(0.0, min(10.0, score)))

def analyze_stock(symbol: str, timeframe: str) -> Optional[SignalInfo]:
    """Bir hisse ve zaman dilimi için sinyal analizi yapar."""
    df = fetch_and_update_data(symbol, timeframe)
    if df is None or df.empty or len(df) < max(200, MA_LONG): return None

    last_close, last_vol = float(df['close'].iloc[-1]), float(df['volume'].iloc[-1])
    if last_close < MIN_PRICE or last_close * last_vol < MIN_VOLUME_TRY: return None

    close, volume = df["close"], df["volume"]
    rsi_series = rsi_from_close(close, RSI_LEN)
    volume_ratio = float(volume.iloc[-1] / sma(volume, 20).iloc[-1]) if sma(volume, 20).iloc[-1] > 0 else 1.0
    macd_hist = (close.ewm(span=MACD_FAST).mean() - close.ewm(span=MACD_SLOW).mean()).ewm(span=MACD_SIGNAL).mean()
    
    macd_signal = "NEUTRAL"
    if len(macd_hist) > 1:
        if macd_hist.iloc[-1] > 0 and macd_hist.iloc[-2] <= 0: macd_signal = "BULLISH"
        elif macd_hist.iloc[-1] < 0 and macd_hist.iloc[-2] >= 0: macd_signal = "BEARISH"
    
    is_bull_signal = macd_signal == "BULLISH" and rsi_series.iloc[-1] > 50 and volume_ratio > 1.5
    is_bear_signal = macd_signal == "BEARISH" and rsi_series.iloc[-1] < 50 and volume_ratio > 1.5

    if not (is_bull_signal or is_bear_signal): return None
    
    direction = "BULLISH" if is_bull_signal else "BEARISH"
    base_signal = SignalInfo(
        symbol=symbol, timeframe=timeframe, direction=direction, price=last_close,
        rsi=rsi_series.iloc[-1], volume_ratio=volume_ratio, strength_score=0.0,
        timestamp=df.index[-1].strftime('%Y-%m-%d %H:%M'), macd_signal=macd_signal
    )
    base_signal.strength_score = calculate_signal_strength(base_signal)
    
    return base_signal

# ----------------------- 6. SİNYAL VERİTABANI YÖNETİMİ -----------------------
def get_signals_db_file():
    return f"signals_{dt.datetime.now(IST_TZ).strftime('%Y-%m-%d')}.json"

def load_daily_signals_from_disk():
    global DAILY_SIGNALS
    db_file = get_signals_db_file()
    if os.path.exists(db_file) and os.stat(db_file).st_size > 0:
        try:
            with open(db_file, 'r', encoding='utf-8') as f: DAILY_SIGNALS = json.load(f)
        except Exception as e:
            logger.error(f"Günlük sinyaller yüklenemedi: {e}")
            DAILY_SIGNALS = {}
    else: DAILY_SIGNALS = {}

def save_daily_signals_to_disk():
    db_file = get_signals_db_file()
    try:
        with open(db_file, 'w', encoding='utf-8') as f:
            json.dump(DAILY_SIGNALS, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Günlük sinyaller kaydedilemedi: {e}")

def is_signal_already_sent_today(symbol: str) -> bool:
    today_str = dt.datetime.now(IST_TZ).strftime("%Y-%m-%d")
    return DAILY_SIGNALS.get(symbol, {}).get('date', '') == today_str

def save_daily_signal(signal: SignalInfo):
    today_str = dt.datetime.now(IST_TZ).strftime("%Y-%m-%d")
    DAILY_SIGNALS[signal.symbol] = {
        'date': today_str,
        'symbol': signal.symbol,
        'strength_score': signal.strength_score,
        'sent_time': dt.datetime.now(IST_TZ).isoformat()
    }
    save_daily_signals_to_disk()

def clear_old_signals():
    today_str = dt.datetime.now(IST_TZ).strftime("%Y-%m-%d")
    keys_to_remove = [k for k, v in DAILY_SIGNALS.items() if v.get('date', '') != today_str]
    for key in keys_to_remove: del DAILY_SIGNALS[key]
    save_daily_signals_to_disk()

# ----------------------- 7. SİNYAL GÖNDERME VE RAPORLAMA -----------------------
async def send_combined_alert(signals: List[SignalInfo]):
    """Bir hisse için tüm sinyalleri tek bir Telegram mesajında birleştirir ve grafik ekler."""
    if not signals: return
    symbol = signals[0].symbol
    if is_signal_already_sent_today(symbol):
        logger.info(f"⏳ {symbol} için sinyal bugün zaten gönderilmiş.")
        return
        
    strongest_signal = max(signals, key=lambda s: s.strength_score)
    if strongest_signal.strength_score < MIN_SIGNAL_SCORE:
        logger.info(f"❌ {symbol} için sinyal puanı ({strongest_signal.strength_score:.1f}) eşiğin altında.")
        return

    save_daily_signal(strongest_signal)
    
    combined_message = f"""<b>📊 BİRLEŞİK SİNYAL: {symbol}.IS</b>

💰 <b>Anlık Fiyat:</b> ₺{strongest_signal.price:.2f}
⚡ <b>En Güçlü Sinyal:</b> {strongest_signal.strength_score:.1f}/10
---
"""
    bullish_signals = [s for s in signals if s.direction == "BULLISH"]
    bearish_signals = [s for s in signals if s.direction == "BEARISH"]
    
    if bullish_signals:
        combined_message += "\n📈 <b>YÜKSELİŞ SİNYALLERİ</b>\n"
        for s in bullish_signals:
            combined_message += f"🟢 <b>{s.timeframe.upper()}</b> | Güç: {s.strength_score:.1f}/10\n"
    
    if bearish_signals:
        combined_message += "\n📉 <b>DÜŞÜŞ SİNYALLERİ</b>\n"
        for s in bearish_signals:
            combined_message += f"🔴 <b>{s.timeframe.upper()}</b> | Güç: {s.strength_score:.1f}/10\n"

    graph_filename = f"/tmp/{symbol}_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    df_to_plot = fetch_and_update_data(symbol, strongest_signal.timeframe)
    if df_to_plot is not None and not df_to_plot.empty:
        try:
            create_and_save_plot(df_to_plot, strongest_signal, graph_filename)
            await send_telegram(combined_message, photo=graph_filename)
        except Exception as e:
            logger.error(f"Grafik oluşturma/gönderme hatası: {e}")
            await send_telegram(combined_message)
        finally:
            if os.path.exists(graph_filename):
                os.remove(graph_filename)
    else:
        await send_telegram(combined_message)

    logger.info(f"📤 Birleşik sinyal gönderildi: {symbol}")

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

# ----------------------- 8. TARAMA DÖNGÜSÜ -----------------------
async def run_scan_async():
    """Tüm hisseleri paralel olarak tarar."""
    global LAST_SCAN_TIME
    if not is_market_hours():
        logger.info(f"⏰ Borsa kapalı - Tarama atlanıyor")
        return

    logger.info(f"🔍 TÜM BIST taraması başladı - {len(TICKERS)} hisse")
    clear_old_signals()
    os.makedirs(DATA_CACHE_DIR, exist_ok=True)
    loop = asyncio.get_running_loop()
    signals_by_symbol: Dict[str, List[SignalInfo]] = {}

    with ThreadPoolExecutor(max_workers=30) as executor:
        tasks = [loop.run_in_executor(executor, analyze_stock, symbol, tf)
                 for symbol in TICKERS for tf in TIMEFRAMES]
        
        try:
            completed_tasks = await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Paralel tarama görevleri sırasında hata oluştu: {e}")
            return

    for result in completed_tasks:
        if isinstance(result, SignalInfo):
            symbol = result.symbol
            if symbol not in signals_by_symbol: signals_by_symbol[symbol] = []
            signals_by_symbol[symbol].append(result)

    for symbol, signals in signals_by_symbol.items():
        await send_combined_alert(signals)
    
    LAST_SCAN_TIME = dt.datetime.now(IST_TZ)
    logger.info(f"✅ Tarama tamamlandı. Son tarama: {LAST_SCAN_TIME.strftime('%H:%M:%S')}")

# ----------------------- 9. WEB SUNUCUSU VE ANA DÖNGÜ YÖNETİMİ -----------------------
async def main_loop():
    """Botun ana çalışma döngüsü."""
    load_daily_signals_from_disk()
    report_sent_today = False
    
    while True:
        now_ist = dt.datetime.now(IST_TZ)
        
        if now_ist.time() >= dt.time(DAILY_REPORT_HOUR, DAILY_REPORT_MINUTE) and not report_sent_today:
            await send_daily_report()
            report_sent_today = True
        
        if now_ist.time() < dt.time(MARKET_OPEN_HOUR, 0):
            report_sent_today = False
            
        if is_market_hours():
            await run_scan_async()
            logger.info(f"⏳ Bir sonraki tarama için bekleniyor... {CHECK_EVERY_MIN} dakika")
            await asyncio.sleep(CHECK_EVERY_MIN * 60)
        else:
            logger.info("😴 Borsa kapalı, uyku modunda bekleniyor...")
            await asyncio.sleep(600)

async def start_background_tasks(app):
    """Arka plan görevini başlatır."""
    app['background_task'] = asyncio.create_task(main_loop())

async def cleanup_background_tasks(app):
    """Arka plan görevini sonlandırır."""
    app['background_task'].cancel()
    await app['background_task']

if __name__ == "__main__":
    app = web.Application()
    app.router.add_get('/health', lambda r: web.json_response({"status": "healthy"}))
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)

    web.run_app(app, host='0.0.0.0', port=HEALTH_CHECK_PORT)
