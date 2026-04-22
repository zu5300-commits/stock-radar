from flask import Flask, jsonify, request
import requests as req
import os, json
from datetime import datetime, timedelta

app = Flask(__name__)

TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiV2luIiwiZW1haWwiOiJ6dTUzMDBAZ21haWwuY29tIn0.q5_lYazAnsTiNGKFdVNlIReL8Kq_FdwnkMd7IZKcPJI"
BASE = "https://api.finmindtrade.com/api/v4/data"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

def fm(dataset, stock_id=None, start=None):
    p = {"dataset": dataset}
    if stock_id: p["data_id"] = stock_id
    if start: p["start_date"] = start
    r = req.get(BASE, headers=HEADERS, params=p, timeout=15)
    return r.json().get("data", [])

def fetch_stock(code):
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        start_30 = (datetime.now()-timedelta(days=30)).strftime("%Y-%m-%d")
        start_1y = (datetime.now()-timedelta(days=365)).strftime("%Y-%m-%d")

        # 股價
        prices = fm("TaiwanStockPrice", code, start_30)
        if not prices:
            return {'code':code,'price':0,'change':0,'volume':0,'error':True}
        latest = prices[-1]
        price  = float(latest.get('close',0))
        prev   = float(latest.get('open', price))
        change = round((price-prev)/prev*100,2) if prev>0 else 0
        volume = int(latest.get('Trading_Volume',0))//1000

        # 三大法人（外資/投信連買天數）
        inst = fm("TaiwanStockInstitutionalInvestorsBuySell", code, start_30)
        foreign_days = 0
        trust_days   = 0
        for row in reversed(inst):
            name = row.get('name','')
            net  = int(row.get('buy',0)) - int(row.get('sell',0))
            if '外資' in name and '自營' not in name:
                if net > 0: foreign_days += 1
                else: break
            if '投信' in name:
                if net > 0: trust_days += 1
                else: break

        return {
            'code': code, 'price': price, 'change': change,
            'volume': volume, 'foreign_days': foreign_days,
            'trust_days': trust_days, 'error': False
        }
    except Exception as e:
        print(f"[ERR] {code}: {e}")
        return {'code':code,'price':0,'change':0,'volume':0,'foreign_days':0,'trust_days':0,'error':True}

@app.route('/quote')
def quote():
    codes = request.args.get('codes','').split(',')
    result = {}
    for code in codes:
        code = code.strip()
        if code:
            result[code] = fetch_stock(code)
    return jsonify({'ok':True,'data':result})

@app.route('/health')
def health():
    return jsonify({'ok':True})

@app.route('/')
def index():
    base = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(base,'index.html'), encoding='utf-8') as f:
        return f.read()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
