# multi_exchange_swing_scanner.py
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
app.config['SECRET_KEY'] = 'multi_exchange_swing_secret'
socketio = SocketIO(app, cors_allowed_origins="*")

# 全局状态
current_exchange = 'binance'  # 默认币安
all_symbols = []
prices = {}
swing_symbols = {}
scan_index = 0
total_symbols = 0
lock = threading.Lock()
scanning = False

# ========== 技术指标计算 ==========
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

# ========== 交易所配置 ==========
EXCHANGES = {
    'binance': {
        'name': 'Binance',
        'color': '#f39c12',
        'symbols_url': 'https://fapi.binance.com/fapi/v1/exchangeInfo',
        'kline_url': 'https://fapi.binance.com/fapi/v1/klines?symbol={}&interval=15m&limit={}',
        'get_symbols': lambda data: sorted([
            item['symbol'] for item in data.get('symbols', [])
            if item.get('contractType') == 'PERPETUAL' and item.get('quoteAsset') == 'USDT'
        ])
    },
    'okx': {
        'name': 'OKX',
        'color': '#4cc9f0',
        'symbols_url': 'https://www.okx.com/api/v5/public/instruments?instType=SWAP',
        'kline_url': 'https://www.okx.com/api/v5/market/candles?instId={}&bar=15m&limit={}',
        'get_symbols': lambda data: [
            item['instId'] for item in data.get('data', []) if item.get('ctType') == 'linear'
        ]
    },
    'bybit': {
        'name': 'Bybit',
        'color': '#ff7326',
        'symbols_url': 'https://api.bybit.com/derivatives/v3/public/instruments-info?category=linear',
        'kline_url': 'https://api.bybit.com/derivatives/v3/public/kline?category=linear&symbol={}&interval=15&limit={}',
        'get_symbols': lambda data: [
            item['symbol'] for item in data.get('result', {}).get('list', [])
            if item.get('status') == 'Trading'
        ]
    },
    'bitget': {
        'name': 'Bitget',
        'color': '#00c1de',
        'symbols_url': 'https://api.bitget.com/api/mix/v1/market/contracts?productType=usdt-futures',
        'kline_url': 'https://api.bitget.com/api/mix/v1/market/candles?symbol={}&granularity=15m&limit={}',
        'get_symbols': lambda data: [
            item['symbol'] for item in data.get('data', []) if item.get('status') == 'online'
        ]
    },
    'kucoin': {
        'name': 'KuCoin Futures',
        'color': '#fd8c3b',
        'symbols_url': 'https://api-futures.kucoin.com/api/v1/contracts/active',
        'kline_url': 'https://api-futures.kucoin.com/api/v1/kline?symbol={}&granularity=15&type=1&limit={}',
        'get_symbols': lambda data: [
            item['symbol'] for item in data.get('data', []) if item.get('status') == 'Open'
        ]
    }
}

# ========== 数据获取 ==========
def fetch_klines(exchange_id, symbol, limit=100):
    try:
        config = EXCHANGES[exchange_id]
        url = config['kline_url'].format(symbol, limit)
        resp = requests.get(url, timeout=8)
        if resp.status_code != 200:
            return None
        
        data = resp.json()
        
        if exchange_id == 'binance':
            if not isinstance(data, list):
                return None
            df = pd.DataFrame(data, columns=[
                'open_time', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
            ])
        elif exchange_id == 'okx':
            if data.get('code') != '0' or not data.get('data'):
                return None
            df = pd.DataFrame(data['data'], columns=[
                'ts', 'open', 'high', 'low', 'close', 'vol', 'volCcy', 'volCcyQuote', 'confirm'
            ])
            # 👇 关键修复：OKX 返回倒序，需反转为正序（最新在最后）
            df = df.iloc[::-1].reset_index(drop=True)
        elif exchange_id == 'bybit':
            if data.get('retCode') != 0 or not data.get('result', {}).get('list'):
                return None
            klines = data['result']['list']
            df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
        elif exchange_id == 'bitget':
            if data.get('code') != '00000' or not data.get('data'):
                return None
            klines = data['data']
            df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        elif exchange_id == 'kucoin':
            if data.get('code') != '200000' or not data.get('data'):
                return None
            klines = data['data']
            df = pd.DataFrame(klines, columns=['time', 'open', 'close', 'high', 'low', 'volume'])
            df = df[['time', 'open', 'high', 'low', 'close', 'volume']]
        else:
            return None

        numeric_cols = ['open', 'high', 'low', 'close']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
        df.dropna(inplace=True)
        if len(df) == 0:
            return None
        return df.reset_index(drop=True)
    except Exception:
        return None

def get_symbols(exchange_id):
    try:
        config = EXCHANGES[exchange_id]
        resp = requests.get(config['symbols_url'], timeout=10)
        data = resp.json()
        symbols = config['get_symbols'](data)
        return sorted(symbols)
    except Exception:
        return []

# ========== 后台扫描线程 ==========
def background_scanner():
    global current_exchange, all_symbols, prices, swing_symbols, scan_index, total_symbols, scanning
    while True:
        with lock:
            exchange = current_exchange
            scanning = True

        symbols = get_symbols(exchange)
        if not symbols:
            print(f"❌ 无法获取 {EXCHANGES[exchange]['name']} 合约列表")
            time.sleep(10)
            continue

        with lock:
            all_symbols = symbols
            total_symbols = len(symbols)
            prices = {s: {'price': None, 'change': 0.0} for s in symbols}
            swing_symbols = {}

        print(f"✅ {EXCHANGES[exchange]['name']}：获取到 {total_symbols} 个合约，开始滚动扫描...")
        
        batch_size = 12
        scan_idx = 0
        while scanning and current_exchange == exchange:
            start_idx = scan_idx
            end_idx = min(start_idx + batch_size, total_symbols)
            current_batch = all_symbols[start_idx:end_idx]

            new_swing_this_round = {}

            for symbol in current_batch:
                df = fetch_klines(exchange, symbol, limit=100)
                if df is None or len(df) < 2:
                    continue

                latest = df['close'].iloc[-1]
                prev = df['close'].iloc[-2]
                change = ((latest - prev) / prev) * 100 if prev != 0 else 0.0

                # 👇 关键修复：转换为原生 Python float，避免 JSON 序列化失败
                safe_price = float(latest) if pd.notna(latest) else None
                safe_change = float(change) if pd.notna(change) else 0.0

                with lock:
                    if symbol in prices:
                        prices[symbol] = {'price': safe_price, 'change': safe_change}

                if is_swing_market(df, atr_lookback=20):
                    amp_series = (df['high'] - df['low']) / df['open']
                    amp_series = amp_series.replace([np.inf, -np.inf], np.nan).dropna()
                    avg_amp = amp_series.tail(5000).mean() if len(amp_series) > 0 else 0.0
                    new_swing_this_round[symbol] = {
                        'price': float(latest) if pd.notna(latest) else None,
                        'amplitude': float(avg_amp) if pd.notna(avg_amp) else 0.0
                    }

                time.sleep(0.12)

            # 👇 安全初始化快照变量（防止 UnboundLocalError）
            prices_snapshot = {}
            swings_snapshot = {}
            exchange_name = EXCHANGES[exchange]['name']
            color = EXCHANGES[exchange]['color']

            with lock:
                if current_exchange == exchange:
                    for sym, data in new_swing_this_round.items():
                        swing_symbols[sym] = data
                    prices_snapshot = prices.copy()
                    swings_snapshot = swing_symbols.copy()

            # 👇 安全 emit（现在所有值都是 JSON serializable）
            socketio.emit('update_prices', prices_snapshot, namespace='/')
            socketio.emit('update_swings', swings_snapshot, namespace='/')
            socketio.emit('update_exchange', {'exchange': exchange_name, 'color': color}, namespace='/')

            scan_idx = (scan_idx + batch_size) % total_symbols

            if scan_idx < batch_size:
                print(f"🔄 {EXCHANGES[exchange]['name']}：已完成一轮全市场扫描")

        scanning = False

# ========== 切换交易所事件 ==========
@socketio.on('switch_exchange')
def handle_switch_exchange(data):
    global current_exchange, scanning
    exchange_id = data.get('exchange')
    if exchange_id in EXCHANGES:
        with lock:
            current_exchange = exchange_id
            scanning = False  # 停止当前扫描
        print(f"🔄 切换至交易所: {EXCHANGES[exchange_id]['name']}")

# ========== 自动打开浏览器 ==========
def open_browser():
    try:
        webbrowser.open("http://localhost:5000", new=2)
    except Exception:
        pass

# ========== HTML 模板 ==========
HTML_TEMPLATE = r'''
<!DOCTYPE html>
<html lang="zh">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>多交易所震荡扫描器</title>
    <script src="https://cdn.socket.io/4.7.2/socket.io.min.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
        body { background: #0f0f1b; color: #e0e0ff; padding: 20px; }
        .header { text-align: center; margin-bottom: 20px; }
        h1 { 
            margin-bottom: 15px; 
            font-size: 28px; 
            color: var(--primary-color, #f39c12);
            transition: color 0.3s;
        }
        .exchange-selector {
            display: inline-block;
            background: #1a1a2e;
            padding: 10px 20px;
            border-radius: 24px;
            margin-bottom: 15px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
        }
        select {
            background: #252540;
            color: #e0e0ff;
            border: 1px solid #444;
            padding: 8px 16px;
            border-radius: 8px;
            font-size: 16px;
            cursor: pointer;
            outline: none;
            appearance: none;
            background-image: url("data:image/svg+xml;charset=UTF-8,%3csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='%23a9a9ff' stroke-width='2'%3e%3cpath d='M6 9l6 6 6-6'/%3e%3c/svg%3e");
            background-repeat: no-repeat;
            background-position: right 10px center;
            background-size: 14px;
            padding-right: 36px;
        }
        .container { max-width: 1400px; margin: 0 auto; display: flex; gap: 20px; flex-wrap: wrap; }
        .panel { background: #1a1a2e; border-radius: 12px; padding: 20px; flex: 1; min-width: 300px; }
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
        .loading { color: #ffaa33; }
    </style>
</head>
<body>
    <div class="header">
        <h1 id="exchangeTitle">多交易所震荡扫描器</h1>
        <div class="exchange-selector">
            <select id="exchangeSelect">
                <option value="binance">Binance</option>
                <option value="okx">OKX</option>
                <option value="bybit">Bybit</option>
                <option value="bitget">Bitget</option>
                <option value="kucoin">KuCoin Futures</option>
            </select>
        </div>
        <div class="header-info">
            正在扫描全部 <span id="totalCount">--</span> 个永续合约 · 
            <span id="statusText" class="loading">加载中...</span>
        </div>
    </div>

    <div class="container">
        <div class="panel">
            <h2>📈 实时价格（全市场）</h2>
            <div id="prices"></div>
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
            // 👇 安全处理 null / NaN / undefined
            if (price == null || typeof price !== 'number' || isNaN(price)) return '--';
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

        function switchExchange(exchangeId) {
            document.getElementById('statusText').textContent = '正在切换...';
            document.getElementById('statusText').className = 'loading';
            socket.emit('switch_exchange', { exchange: exchangeId });
        }

        document.getElementById('exchangeSelect').addEventListener('change', (e) => {
            switchExchange(e.target.value);
        });

        socket.on('connect', () => {
            console.log('✅ 已连接到服务器');
        });

        socket.on('update_prices', (data) => {
            prices = data;
            totalCount = Object.keys(data).length;
            updatePrices();
            document.getElementById('statusText').textContent = '实时更新中';
            document.getElementById('statusText').className = '';
        });

        socket.on('update_swings', (data) => {
            swings = data;
            updateSwings();
        });

        socket.on('update_exchange', (data) => {
            document.documentElement.style.setProperty('--primary-color', data.color);
            document.getElementById('exchangeTitle').textContent = data.exchange + ' 震荡扫描器';
        });
    </script>
</body>
</html>
'''

# 👇 关键路由：解决 404
@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

# ========== 启动主程序 ==========
if __name__ == '__main__':
    scanner_thread = threading.Thread(target=background_scanner, daemon=True)
    scanner_thread.start()
    
    browser_thread = threading.Thread(target=open_browser)
    browser_thread.daemon = True
    browser_thread.start()
    
    print("🚀 多交易所震荡扫描器已启动，正在打开浏览器...")
    print("支持的交易所：Binance, OKX, Bybit, Bitget, KuCoin Futures")
    socketio.run(
        app,
        host='127.0.0.1',
        port=5000,
        debug=False,
        use_reloader=False,
        allow_unsafe_werkzeug=True  # 允许本地运行（新版 Flask-SocketIO 必需）
    )
