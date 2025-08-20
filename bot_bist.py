import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="investpy")

import investpy
import yfinance as yf
from datetime import datetime, timedelta, UTC
import pandas as pd
import time

# ------------------------------------------------
# AYARLAR
# ------------------------------------------------
BIST_HISSELER = ["THYAO.IS", "GARAN.IS", "ASELS.IS"]  # Borsa İstanbul hisseleri
INTERVAL = "1d"  # '1d', '1h', '15m' vb.
RSI_PERIOD = 14

# ------------------------------------------------
# YARDIMCI FONKSİYONLAR
# ------------------------------------------------
def get_end_date():
    return datetime.now(UTC)  # utcnow() yerine

def get_start_date(days_back=30):
    return get_end_date() - timedelta(days=days_back)

def fetch_data_yfinance(symbol, start, end, interval):
   df = yf.download(symbol, start=start, end=end, interval=interval, auto_adjust=False)
    if not df.empty:
        df.dropna(inplace=True)
    return df

def rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -1 * delta.clip(upper=0)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def analyze_symbol(symbol):
    start_date = get_start_date(90)
    end_date = get_end_date()
    df = fetch_data_yfinance(symbol, start=start_date, end=end_date, interval=INTERVAL)

    if df.empty:
        print(f"[HATA] Veri yok: {symbol}")
        return

    df["RSI"] = rsi(df["Close"], RSI_PERIOD)
    last_rsi = df["RSI"].iloc[-1]
    print(f"{symbol} son RSI: {last_rsi:.2f}")

    if last_rsi > 70:
        print(f"⚠ {symbol} aşırı alım bölgesinde!")
    elif last_rsi < 30:
        print(f"⚠ {symbol} aşırı satım bölgesinde!")

# ------------------------------------------------
# ANA DÖNGÜ
# ------------------------------------------------
if __name__ == "__main__":
    while True:
        print("\n--- BIST RSI BOT ---")
        for hisse in BIST_HISSELER:
            analyze_symbol(hisse)
        print("Bekleniyor...")
        time.sleep(300)  # 5 dakika bekle
