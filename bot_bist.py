import os
import sqlite3
import asyncio
import threading
import logging
import io
import random
import time
from datetime import datetime, timedelta

# 3. Parti
import pandas as pd
import pandas_ta as ta
import yfinance as yf
import mplfinance as mpf
from flask import Flask
from telegram import Bot, InputFile
from telegram.error import RetryAfter

# --- CONFIG ---
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
DB_NAME = "bist_quant.sqlite"
MIN_VOLUME_TL = 20_000_000
BATCH_SIZE = 20  # Her seferde 20 hisse çek (Hız Canavarı)
TIMEFRAMES = ["1h", "1d"]

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("BIST_QUANT")

# --- DYNAMIC SYMBOL MANAGER ---
def get_bist_symbols():
    """
    Burası dinamikleştirilebilir.
    İlerde TradingView veya KAP'tan çeken bir scraper eklenebilir.
    Şimdilik en hacimli ve güvenilir BIST Tüm listesi.
    """
    base_list = [
        "THYAO.IS", "ASELS.IS", "KCHOL.IS", "AKBNK.IS", "EREGL.IS", "SISE.IS", "BIMAS.IS", "TUPRS.IS", 
        "GARAN.IS", "YKBNK.IS", "SAHOL.IS", "PETKM.IS", "HEKTS.IS", "SASA.IS", "KONTR.IS", "ODAS.IS", 
        "EKGYO.IS", "FROTO.IS", "TOASO.IS", "PGSUS.IS", "ASTOR.IS", "EUPWR.IS", "ALARK.IS", "KOZAL.IS",
        "ISCTR.IS", "TCELL.IS", "MGROS.IS", "ENKAI.IS", "VESTL.IS", "GUBRF.IS", "OYAKC.IS", "TTKOM.IS",
        "HALKB.IS", "VAKBN.IS", "TSKB.IS", "SKBNK.IS", "ALBRK.IS", "AEFES.IS", "AGHOL.IS", "AKSA.IS",
        "AKSEN.IS", "ALGYO.IS", "ALKIM.IS", "ANHYT.IS", "ARCLK.IS", "ARDYZ.IS", "AYDEM.IS", "AYGAZ.IS",
        "BERA.IS", "BIOEN.IS", "BRSAN.IS", "BRYAT.IS", "BUCIM.IS", "CANTE.IS", "CCOLA.IS", "CEMTS.IS",
        "CIMSA.IS", "DOAS.IS", "DOHOL.IS", "ECILC.IS", "EGEEN.IS", "ENJSA.IS", "ENERY.IS", "ERBOS.IS",
        "GENIL.IS", "GESAN.IS", "GLYHO.IS", "GOZDE.IS", "GSDHO.IS", "GWIND.IS", "HEDEF.IS", "HLGYO.IS",
        "HUNER.IS", "IHLAS.IS", "INDES.IS", "INVEO.IS", "ISDMR.IS", "ISFIN.IS", "ISGYO.IS", "ISMEN.IS",
        "IZMDC.IS", "KARSN.IS", "KARTN.IS", "KCAER.IS", "KERVT.IS", "KLRHO.IS", "KMPUR.IS", "KORDS.IS",
        "KOZAA.IS", "KRDMD.IS", "KZBGY.IS", "LOGO.IS", "MAVI.IS", "MNDRS.IS", "MPARK.IS", "NTHOL.IS",
        "OTKAR.IS", "OZKGY.IS", "PENTA.IS", "PSGYO.IS", "QUAGR.IS", "RTALB.IS", "RYGYO.IS", "RYSAS.IS",
        "SANKO.IS", "SELEC.IS", "SKTAS.IS", "SMRTG.IS", "SNGYO.IS", "SOKM.IS", "TATGD.IS", "TAVHL.IS",
        "TKFEN.IS", "TKNSA.IS", "TMSN.IS", "TRGYO.IS", "TSPOR.IS", "TTRAK.IS", "TUKAS.IS", "TURSG.IS",
        "ULKER.IS", "UNLU.IS", "VERUS.IS", "VESBE.IS", "YATAS.IS", "YEOAF.IS", "YEOTK.IS", "YYLGD.IS",
        "ZRGYO.IS", "ZOREN.IS"
    ]
    # Listeyi karıştır ki hep aynı sırayla gitmesin
    random.shuffle(base_list)
    return base_list

# --- QUEUE ---
class TelegramQueue:
    def __init__(self, token, chat_id):
        self.bot = Bot(token=token)
        self.chat_id = chat_id
        self.queue = asyncio.Queue()
        self.running = True

    async def add_msg(self, photo_buf, caption):
        await self.queue.put((photo_buf, caption))

    async def worker(self):
        logger.info("📨 Mesaj Kuyruğu Hazır")
        while self.running:
            try:
                photo, caption = await self.queue.get()
                try:
                    await self.bot.send_photo(self.chat_id, photo=InputFile(photo, "chart.png"), caption=caption, parse_mode='HTML')
                except RetryAfter as e:
                    logger.warning(f"Rate Limit: {e.retry_after}s bekleniyor")
                    await asyncio.sleep(e.retry_after)
                except Exception as e:
                    logger.error(f"Mesaj Hatası: {e}")
                self.queue.task_done()
                await asyncio.sleep(3)
            except Exception:
                await asyncio.sleep(1)

# --- DB MANAGER ---
class AsyncDatabase:
    def __init__(self):
        self.lock = asyncio.Lock()
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(DB_NAME)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, timeframe TEXT,
                entry_price REAL, current_price REAL,
                entry_date TEXT, status TEXT DEFAULT 'OPEN',
                score REAL, pnl_percent REAL DEFAULT 0.0
            )
        ''')
        conn.commit()
        conn.close()

    async def write(self, query, params=()):
        async with self.lock:
            with sqlite3.connect(DB_NAME) as conn:
                conn.execute(query, params)
                conn.commit()

    async def fetch(self, query, params=()):
        async with self.lock:
            with sqlite3.connect(DB_NAME) as conn:
                conn.row_factory = sqlite3.Row
                return [dict(r) for r in conn.execute(query, params).fetchall()]

# --- QUANT ANALYZER (BATCH) ---
class QuantAnalyzer:
    @staticmethod
    async def fetch_batch_data(symbols, interval):
        """
        TEK SEFERDE 20 HİSSE ÇEKER (Rate Limit Dostu)
        """
        period = "1mo" if interval == "1h" else "6mo"
        tickers = " ".join(symbols)
        try:
            loop = asyncio.get_running_loop()
            # Threads=True ile daha da hızlanır
            df = await loop.run_in_executor(
                None, 
                lambda: yf.download(tickers, period=period, interval=interval, group_by='ticker', progress=False, threads=True)
            )
            return df
        except Exception as e:
            logger.error(f"Batch Download Hatası: {e}")
            return None

    @staticmethod
    def analyze_symbol(df, symbol):
        """Tek bir sembol için analiz"""
        try:
            # Veri temizliği (Batch indirmede MultiIndex gelir, tek hisse ise gelmeyebilir)
            if df.empty or len(df) < 30: return None
            
            # MultiIndex kontrolü (Close, Volume vs var mı?)
            # Eğer tek hisse indiyse kolonlar düzdür, çoklu indiyse (Ticker, Close) şeklindedir.
            if isinstance(df.columns, pd.MultiIndex):
                # Bu fonksiyon tekil dataframe bekler, dışarıda ayıklanmalı
                pass 

            # Temel Göstergeler
            close = df['Close']
            if close.isna().iloc[-1]: return None # Son veri yoksa geç

            df['EMA_20'] = ta.ema(close, length=20)
            df['EMA_50'] = ta.ema(close, length=50)
            df['RSI'] = ta.rsi(close, length=14)
            
            curr = df.iloc[-1]
            
            # Hacim Filtresi (Hata vermemesi için try-except)
            try:
                vol_tl = curr['Close'] * curr['Volume']
                if vol_tl < MIN_VOLUME_TL: return None
            except: return None

            score = 0
            reasons = []

            # STRATEJİ
            if curr['EMA_20'] > curr['EMA_50']:
                score += 30
                reasons.append("Trend Pozitif")
            
            if 45 <= curr['RSI'] <= 65:
                score += 20
                reasons.append("RSI Makul")
            
            # Hacim Patlaması
            vol_ma = df['Volume'].rolling(20).mean().iloc[-1]
            if curr['Volume'] > vol_ma * 1.3:
                score += 25
                reasons.append("Hacim Artışı")
                
            # Yutan Boğa (Engulfing)
            prev = df.iloc[-2]
            if curr['Close'] > prev['High'] and curr['Open'] < prev['Low']:
                score += 25
                reasons.append("Bullish Engulfing")

            if score >= 75:
                return {
                    "price": curr['Close'],
                    "score": score,
                    "reasons": reasons,
                    "rsi": curr['RSI'],
                    "df": df.tail(60) # Grafik için son 60 mum
                }
        except Exception as e:
            pass
        return None

    @staticmethod
    def generate_chart(df, symbol, tf, info):
        buf = io.BytesIO()
        s = mpf.make_mpf_style(base_mpf_style='nightclouds', rc={'font.size': 7})
        title = f"{symbol} ({tf}) | Skor:{info['score']}"
        
        apds = [
            mpf.make_addplot(df['EMA_20'], color='cyan', width=0.8),
            mpf.make_addplot(df['EMA_50'], color='orange', width=0.8)
        ]
        mpf.plot(df, type='candle', style=s, addplot=apds, title=title, volume=True, savefig=dict(fname=buf, dpi=100, bbox_inches='tight'))
        buf.seek(0)
        return buf

# --- DASHBOARD ---
app = Flask(__name__)
db_inst = AsyncDatabase()

@app.route('/')
def index():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM signals ORDER BY id DESC LIMIT 50").fetchall()
    conn.close()
    
    html = "<html><body style='background:#111;color:#eee;font-family:sans-serif'>"
    html += "<h2>⚡ BIST QUANT SCANNER (Batch Processing)</h2>"
    html += "<table border='1' style='border-collapse:collapse;width:100%'><tr><th>Sym</th><th>Fiyat</th><th>Durum</th><th>PNL</th></tr>"
    for r in rows:
        color = "lime" if r['pnl_percent']>0 else "red"
        html += f"<tr><td>{r['symbol']}</td><td>{r['entry_price']:.2f}</td><td>{r['status']}</td><td style='color:{color}'>{r['pnl_percent']:.2f}%</td></tr>"
    html += "</table></body></html>"
    return html

# --- ENGINE ---
class QuantBot:
    def __init__(self):
        self.db = AsyncDatabase()
        self.tg = TelegramQueue(TELEGRAM_TOKEN, CHAT_ID)

    async def scanner(self):
        await asyncio.sleep(2)
        logger.info("🚀 Quant Motoru Başlatılıyor...")
        
        while True:
            symbols = get_bist_symbols()
            
            # Hisseleri paketle (Batching)
            # Örn: [ [A,B,C...], [D,E,F...] ]
            batches = [symbols[i:i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
            
            for tf in TIMEFRAMES:
                logger.info(f"🔍 Tarama Modu: {tf} | Toplam Paket: {len(batches)}")
                
                for batch in batches:
                    try:
                        # 1. Toplu İndir
                        data = await QuantAnalyzer.fetch_batch_data(batch, tf)
                        
                        if data is None or data.empty:
                            continue

                        # 2. Her hisse için ayıkla ve analiz et
                        for symbol in batch:
                            try:
                                # Cooldown Kontrolü (DB)
                                if await self.db.fetch("SELECT 1 FROM signals WHERE symbol=? AND timeframe=? AND status='OPEN'", (symbol, tf)):
                                    continue

                                # Dataframe Slicing (Pandas Büyüsü)
                                # Yfinance group_by='ticker' formatı: data[Symbol]
                                try:
                                    if len(batch) > 1:
                                        symbol_df = data[symbol].copy()
                                    else:
                                        symbol_df = data.copy() # Tek hisse kaldıysa
                                except KeyError:
                                    continue # Veri gelmediyse geç

                                # 3. Analiz
                                res = QuantAnalyzer.analyze_symbol(symbol_df, symbol)
                                if res:
                                    logger.info(f"✅ Sinyal: {symbol}")
                                    await self.db.write(
                                        "INSERT INTO signals (symbol, timeframe, entry_price, current_price, entry_date, score) VALUES (?, ?, ?, ?, ?, ?)",
                                        (symbol, tf, res['price'], res['price'], datetime.now().strftime('%Y-%m-%d %H:%M'), res['score'])
                                    )
                                    
                                    chart = QuantAnalyzer.generate_chart(res['df'], symbol, tf, res)
                                    caption = f"🤖 <b>ALGO SİNYAL ({tf})</b>\n\n📈 {symbol}\n💰 {res['price']:.2f} TL\n🎯 Skor: {res['score']}\n⚡ {', '.join(res['reasons'])}"
                                    await self.tg.add_msg(chart, caption)

                            except Exception as e:
                                continue # Tekil hisse hatası tüm batch'i durdurmasın

                        # Batch arası kısa mola (Nefes al)
                        await asyncio.sleep(2)

                    except Exception as e:
                        logger.error(f"Batch Hatası: {e}")
                        await asyncio.sleep(5)

                logger.info(f"{tf} turu bitti. Soğutuluyor...")
                await asyncio.sleep(300) # Timeframe arası bekleme

    async def pnl_tracker(self):
        while True:
            # PNL Takibi (Burada da batch yapılabilir ama basit tutalım)
            opens = await self.db.fetch("SELECT * FROM signals WHERE status='OPEN'")
            for row in opens:
                # Burayı da ilerde batch yapabilirsin
                pass 
            await asyncio.sleep(600)

    def run(self):
        t = threading.Thread(target=lambda: app.run(host='0.0.0.0', port=8080))
        t.daemon = True
        t.start()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(self.tg.worker())
        loop.create_task(self.scanner())
        loop.run_forever()

if __name__ == "__main__":
    if TELEGRAM_TOKEN:
        QuantBot().run()
    else:
        print("Token Yok")
