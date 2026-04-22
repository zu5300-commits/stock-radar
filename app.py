from flask import Flask, jsonify, request
import urllib.request
import json
import ssl
import os

app = Flask(__name__)

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
    'Referer': 'https://finance.yahoo.com',
}

def fetch_stock(code):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{code}.TW?interval=1d&range=5d"
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, context=CTX, timeout=10) as r:
            data = json.loads(r.read())
        meta = data['chart']['result'][0]['meta']
        price = float(meta.get('regularMarketPrice') or 0)
        prev  = float(meta.get('chartPreviousClose') or price)
        vol   = int(meta.get('regularMarketVolume') or 0)
        change = round((price - prev) / prev * 100, 2) if prev > 0 else 0
        return {'code': code, 'price': round(price,2), 'change': change, 'volume': vol // 1000, 'error': False}
    except Exception as e:
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
