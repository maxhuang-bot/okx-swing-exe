# okx_swing_web_full.py
import os
import time
import threading
import requests
import pandas as pd
import numpy as np
from flask import Flask, render_template_string
from flask_socketio import SocketIO, emit
import logging
import webbrowser

# 关闭 Flask 默认日志
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'okx_swing_secret'
socketio = SocketIO(app)  # ✅ 移除 async_mode，自动选择后端

# 全局状态
all_symbols = []
prices = {}
swing_symbols = {}
scan_index = 0
total_symbols = 0
lock = threading.Lock()

# ========== 指标计算 ==========
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

def is_swing_market(df, atr_lookback=20):
    if len(df) < 50:
        return False
    rsi = calculate_rsi(df['close'], 14)
    adx = calculate_adx(df['high'], df['low'], df['close'], 14)
    atr = calculate_atr(df['high'], df['low'], df['close'], 14)
    cond1 = 40 <= rsi.iloc[-1] <= 60
    cond2 = adx.iloc[-1] < 25
    cond3 = len(atr) >= atr_lookback and atr.iloc[-1] < atr.tail(atr_lookback).median()
    return cond1 and cond2 and cond3

# ========== 数据获取 ==========
def fetch_klines(symbol, limit=100):
    try:
        url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar=15m&limit={limit}"
        resp = requests.get(url, timeout=8)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if data.get('code') != '0' or not data.get('data'):
            return None
        df = pd.DataFrame(data['data'], columns=[
            'ts', 'open', 'high', 'low', 'close', 'vol', 'volCcy', 'volCcyQuote', 'confirm'
        ])
        numeric_cols = ['ts', 'open', 'high', 'low', 'close']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
        df.dropna(inplace=True)
        if len(df) == 0:
            return None
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        return df.sort_values('ts').reset_index(drop=True)
    except Exception:
        return None

def get_okx_swap_symbols():
    try:
        resp = requests.get("https://www.okx.com/api/v5/public/instruments?instType=SWAP", timeout=10)
        data = resp.json()
        if data['code'] == '0':
            return [item['instId'] for item in data['data']]
        else:
            return []
    except Exception:
        return []

# ========== 后台扫描线程 ==========
def background_scanner():
    global all_symbols, prices, swing_symbols, scan_index, total_symbols
    symbols = get_okx_swap_symbols()
    if not symbols:
        print("❌ 无法获取合约列表")
        return

    with lock:
        all_symbols = symbols
        total_symbols = len(symbols)
        for s in symbols:
            prices[s] = {'price': '--', 'change': 0}

    print(f"✅ 获取到 {total_symbols} 个永续合约，开始滚动扫描...")
    
    batch_size = 12
    while True:
        start_idx = scan_index
        end_idx = min(start_idx + batch_size, total_symbols)
        current_batch = all_symbols[start_idx:end_idx]

        new_swing_this_round = {}

        for symbol in current_batch:
            df = fetch_klines(symbol, limit=100)
            if df is None or len(df) < 2:
                continue

            latest = df['close'].iloc[-1]
            prev = df['close'].iloc[-2]
            change = ((latest - prev) / prev) * 100 if prev != 0 else 0

            with lock:
                prices[symbol] = {'price': latest, 'change': change}

            if is_swing_market(df, atr_lookback=20):
                # ✅ 安全计算振幅：防除零、无穷
                amp_series = (df['high'] - df['low']) / df['open']
                amp_series = amp_series.replace([np.inf, -np.inf], np.nan).dropna()
                avg_amp = amp_series.tail(5000).mean() if len(amp_series) > 0 else 0
                new_swing_this_round[symbol] = {
                    'price': latest,
                    'amplitude': avg_amp
                }

            time.sleep(0.12)  # ✅ 使用 time.sleep 而非 socketio.sleep

        # ✅ 线程安全：emit 前加锁复制
        with lock:
            for sym, data in new_swing_this_round.items():
                swing_symbols[sym] = data
            prices_snapshot = prices.copy()
            swings_snapshot = swing_symbols.copy()

        socketio.emit('update_prices', prices_snapshot, namespace='/')
        socketio.emit('update_swings', swings_snapshot, namespace='/')

        scan_index = (scan_index + batch_size) % total_symbols

        if scan_index < batch_size:
            print(f"🔄 已完成一轮全市场扫描（共 {total_symbols} 个合约）")

# ========== 自动打开浏览器（带异常处理）==========
def open_browser():
    try:
        webbrowser.open("http://localhost:5000", new=2)
    except Exception:
        pass  # 忽略浏览器打不开的错误

# ========== Web 路由 ==========
@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

# ========== HTML 模板（含按振幅降序排序）==========
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="zh">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>OKX 全市场震荡扫描器</title>
    <script src="https://cdn.socket.io/4.7.2/socket.io.min.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
        body { background: #0f0f1b; color: #e0e0ff; padding: 20px; }
        .container { max-width: 1400px; margin: 0 auto; display: flex; gap: 20px; flex-wrap: wrap; }
        .panel { background: #1a1a2e; border-radius: 12px; padding: 20px; flex: 1; min-width: 300px; }
        h1 { text-align: center; margin-bottom: 20px; color: #4cc9f0; font-size: 28px; }
        h2 { margin-bottom: 15px; color: #f72585; font-size: 20px; }
        .price-item { 
            display: flex; justify-content: space-between; 
            padding: 8px 0; border-bottom: 1px solid #2d2d44;
        }
        .symbol { font-weight: bold; color: #a9a9ff; }
        .price { font-weight: bold; }
        .change { font-size: 0.9em; }
        .positive { color: #4ade80; }
        .negative { color: #f87171; }
        .swing-list { max-height: 600px; overflow-y: auto; }
        .swing-item { 
            background: #252540; margin-bottom: 10px; padding: 12px; 
            border-left: 4px solid #f72585; border-radius: 8px;
            animation: fadeIn 0.3s;
        }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(5px); } to { opacity: 1; transform: translateY(0); } }
        .status { text-align: center; margin-top: 10px; color: #888; font-size: 0.9em; }
        .header-info { text-align: center; color: #aaa; margin-bottom: 15px; }
        .amplitude { color: #ffd166; font-weight: bold; }
    </style>
</head>
<body>
    <h1>🌐 OKX 全市场震荡扫描器</h1>
    <div class="header-info">正在扫描全部 <span id="totalCount">--</span> 个永续合约 · 实时滚动更新</div>
    <div class="container">
        <div class="panel">
            <h2>📈 实时价格（全市场）</h2>
            <div id="prices"></div>
            <div class="status">加载中...</div>
        </div>
        <div class="panel">
            <h2>🎯 震荡币种（三重过滤｜按振幅降序）</h2>
            <div id="swings" class="swing-list"></div>
            <div class="status" id="swingStatus">暂无符合条件的币种</div>
        </div>
    </div>

    <script>
        const socket = io();
        let prices = {};
        let swings = {};
        let totalCount = 0;

        function formatPrice(price) {
            if (typeof price !== 'number' || isNaN(price)) return '--';
            if (price >= 1) return price.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            return parseFloat(price).toFixed(8).replace(/\.?0+$/, '');
        }

        function updatePrices() {
            const container = document.getElementById('prices');
            const symbols = Object.keys(prices).sort();
            container.innerHTML = '';
            symbols.forEach(symbol => {
                const data = prices[symbol];
                const div = document.createElement('div');
                div.className = 'price-item';
                const changeClass = data.change >= 0 ? 'positive' : 'negative';
                const changeSign = data.change >= 0 ? '+' : '';
                div.innerHTML = `
                    <span class="symbol">${symbol}</span>
                    <div>
                        <span class="price">${formatPrice(data.price)}</span>
                        <span class="change ${changeClass}">${isNaN(data.change) ? '--' : changeSign + data.change.toFixed(2)}%</span>
                    </div>
                `;
                container.appendChild(div);
            });
            document.getElementById('totalCount').textContent = totalCount;
        }

        function updateSwings() {
            const container = document.getElementById('swings');
            const status = document.getElementById('swingStatus');
            container.innerHTML = '';

            // 🔺 按平均振幅从大到小排序
            const sortedSwings = Object.entries(swings)
                .sort((a, b) => b[1].amplitude - a[1].amplitude);

            if (sortedSwings.length === 0) {
                status.textContent = '暂无符合条件的币种';
                return;
            }

            status.textContent = `发现 ${sortedSwings.length} 个震荡币种（按振幅降序）`;

            sortedSwings.forEach(([symbol, data]) => {
                const div = document.createElement('div');
                div.className = 'swing-item';
                div.innerHTML = `
                    <div><strong>${symbol}</strong></div>
                    <div>价格: ${formatPrice(data.price)}</div>
                    <div>平均振幅: <span class="amplitude">${(data.amplitude * 100).toFixed(4)}%</span></div>
                `;
                container.appendChild(div);
            });
        }

        socket.on('connect', () => {
            console.log('Connected to server');
        });

        socket.on('update_prices', (data) => {
            prices = data;
            totalCount = Object.keys(data).length;
            updatePrices();
        });

        socket.on('update_swings', (data) => {
            swings = data;
            updateSwings();
        });
    </script>
</body>
</html>
'''

# ========== 启动 ==========
if __name__ == '__main__':
    scanner_thread = threading.Thread(target=background_scanner, daemon=True)
    scanner_thread.start()
    
    # 启动后 1.5 秒自动打开浏览器
    browser_thread = threading.Thread(target=open_browser)
    browser_thread.daemon = True
    browser_thread.start()
    
    print("🚀 OKX 扫描器已启动，正在打开浏览器...")
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, use_reloader=False)
