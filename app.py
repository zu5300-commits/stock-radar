from flask import Flask, jsonify, request
import yfinance as yf
import os

app = Flask(__name__)

def fetch_stock(code):
    try:
        ticker = yf.Ticker(f"{code}.TW")
        info = ticker.fast_info
        price  = round(float(info.last_price or 0), 2)
        prev   = round(float(info.previous_close or price), 2)
        vol    = int(info.three_month_average_volume or 0) // 1000
        change = round((price - prev) / prev * 100, 2) if prev > 0 else 0
        return {'code': code, 'price': price, 'change': change, 'volume': vol, 'error': False}
    except Exception as e:
        print(f"[WARN] {code}: {e}")
        return {'code': code, 'price': 0, 'change': 0, 'volume': 0, 'error': True}

@app.route('/quote')
def quote():
    codes = request.args.get('codes', '').split(',')
    result = {}
    for code in codes:
        code = code.strip()
        if code:
            result[code] = fetch_stock(code)
    return jsonify({'ok': True, 'data': result})

@app.route('/health')
def health():
    return jsonify({'ok': True})

@app.route('/')
def index():
    base = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(base, 'index.html'), encoding='utf-8') as f:
        return f.read()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
