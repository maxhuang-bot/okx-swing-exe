# okx_swing_rsi_adx_atr_20.py
import requests
import pandas as pd
import numpy as np
import time
import os

def calculate_rsi(close, window=14):
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_adx(high, low, close, window=14):
    tr0 = high - low
    tr1 = (high - close.shift(1)).abs()
    tr2 = (low - close.shift(1)).abs()
    tr = pd.concat([tr0, tr1, tr2], axis=1).max(axis=1)
    
    up = high.diff()
    down = -low.diff()
    plus_dm = up.where((up > down) & (up > 0), 0)
    minus_dm = down.where((down > up) & (down > 0), 0)
    
    tr_smooth = tr.ewm(alpha=1/window, min_periods=window).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1/window, min_periods=window).mean() / tr_smooth
    minus_di = 100 * minus_dm.ewm(alpha=1/window, min_periods=window).mean() / tr_smooth
    
    dx = (plus_di - minus_di).abs() / (plus_di + minus_di) * 100
    adx = dx.ewm(alpha=1/window, min_periods=window).mean()
    return adx

def calculate_atr(high, low, close, window=14):
    tr0 = high - low
    tr1 = (high - close.shift(1)).abs()
    tr2 = (low - close.shift(1)).abs()
    tr = pd.concat([tr0, tr1, tr2], axis=1).max(axis=1)
    return tr.rolling(window, min_periods=1).mean()

def is_swing_market(df, rsi_window=14, adx_window=14, atr_window=14, adx_threshold=25, atr_lookback=20):
    if len(df) < max(rsi_window, adx_window, atr_window) + 10:
        return False

    # 条件1: RSI ∈ [40, 60]
    rsi = calculate_rsi(df['close'], rsi_window)
    cond1 = 40 <= rsi.iloc[-1] <= 60

    # 条件2: ADX < 25
    adx = calculate_adx(df['high'], df['low'], df['close'], adx_window)
    cond2 = adx.iloc[-1] < adx_threshold

    # 条件3: 当前 ATR < 最近 atr_lookback 根 ATR 的中位数（默认20）
    atr = calculate_atr(df['high'], df['low'], df['close'], atr_window)
    if len(atr) < atr_lookback:
        cond3 = False
    else:
        recent_atr = atr.tail(atr_lookback)
        atr_median = recent_atr.median()
        atr_current = atr.iloc[-1]
        cond3 = atr_current < atr_median

    return cond1 and cond2 and cond3

# ========== 数据获取与主程序（保持不变）==========
def get_okx_swap_symbols():
    try:
        resp = requests.get("https://www.okx.com/api/v5/public/instruments?instType=SWAP", timeout=10)
        data = resp.json()
        return [item['instId'] for item in data['data']] if data['code'] == '0' else []
    except:
        return []

def fetch_klines(symbol, bar="15m", limit=5000):
    url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar={bar}&limit={limit}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if data['code'] != '0' or not data['data']:
            return None
        df = pd.DataFrame(data['data'], columns=[
            'ts', 'open', 'high', 'low', 'close', 'vol', 'volCcy', 'volCcyQuote', 'confirm'
        ])
        for col in ['open', 'high', 'low', 'close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df.dropna(inplace=True)
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        return df.sort_values('ts').reset_index(drop=True)
    except:
        return None

def calculate_avg_amplitude(df, n=5000):
    if len(df) < n:
        return np.nan
    recent = df.tail(n)
    amp = (recent['high'] - recent['low']) / recent['open']
    return amp.mean()

def main():
    print("OKX 震荡合约扫描器（20根ATR版）")
    print("判定条件：")
    print("  1. RSI(14) ∈ [40, 60]")
    print("  2. ADX(14) < 25")
    print("  3. 当前 ATR(14) < 最近 20 根 ATR 的中位数")
    print("-" * 70)
    
    symbols = get_okx_swap_symbols()
    if not symbols:
        print("❌ 无法获取 OKX 合约列表")
        input("\n按回车退出...")
        return

    results = []
    total = min(80, len(symbols))
    for i, symbol in enumerate(symbols[:total]):
        print(f"\r[{i+1}/{total}] {symbol}...", end='', flush=True)
        df = fetch_klines(symbol, limit=5000)
        if df is None or len(df) < 50:
            continue
        if is_swing_market(df, atr_lookback=20):  # ← 关键参数
            avg_amp = calculate_avg_amplitude(df, 5000)
            if not np.isnan(avg_amp):
                results.append({'symbol': symbol, 'avg_amplitude': avg_amp})
        time.sleep(0.15)

    print("\n\n" + "="*60)
    if results:
        result_df = pd.DataFrame(results).sort_values('avg_amplitude', ascending=False)
        for _, row in result_df.iterrows():
            print(f"{row['symbol']:<20} | 平均振幅: {row['avg_amplitude']:.4%}")
        csv_file = "okx_swing_20atr.csv"
        result_df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        print(f"\n✅ 结果已保存至: {os.path.abspath(csv_file)}")
    else:
        print("⚠️ 未发现符合三重条件的合约")
    input("\n按回车退出...")

if __name__ == "__main__":
    main()