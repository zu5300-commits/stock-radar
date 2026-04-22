from flask import Flask, jsonify, request
import requests as req
import os
from datetime import datetime, timedelta

app = Flask(__name__)

TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiV2luIiwiZW1haWwiOiJ6dTUzMDBAZ21haWwuY29tIn0.q5_lYazAnsTiNGKFdVNlIReL8Kq_FdwnkMd7IZKcPJI"
BASE = "https://api.finmindtrade.com/api/v4/data"

def fm(dataset, stock_id, start):
    r = req.get(BASE, params={
        "dataset": dataset,
        "data_id": stock_id,
        "start_date": start,
        "token": TOKEN
    }, timeout=15)
    return r.json().get("data", [])

def fetch_stock(code):
    try:
        start_30 = (datetime.now()-timedelta(days=30)).strftime("%Y-%m-%d")

        # 股價
        prices = fm("TaiwanStockPrice", code, start_30)
        if not prices:
            return {'code':code,'price':0,'change':0,'volume':0,'foreign_days':0,'trust_days':0,'error':True}
        latest = prices[-1]
        price  = float(latest.get('close') or 0)
        prev   = float(prices[-2].get('close') or price) if len(prices)>=2 else price
        change = round((price-prev)/prev*100, 2) if prev>0 else 0
        volume = int(latest.get('Trading_Volume') or 0) // 1000

        # 三大法人（外資/投信連買天數）
        inst = fm("TaiwanStockInstitutionalInvestorsBuySell", code, start_30)
        
        # 整理成 {日期: {外資net, 投信net}}
        from collections import defaultdict
        daily = defaultdict(dict)
        for row in inst:
            date = row['date']
            name = row.get('name','')
            net  = int(row.get('buy',0)) - int(row.get('sell',0))
            if '外資' in name and '自營' not in name:
                daily[date]['foreign'] = net
            elif '投信' in name:
                daily[date]['trust'] = net

        dates = sorted(daily.keys(), reverse=True)
        foreign_days = 0
        trust_days   = 0
        for d in dates:
            if daily[d].get('foreign', 0) > 0:
                foreign_days += 1
            else:
                break
        for d in dates:
            if daily[d].get('trust', 0) > 0:
                trust_days += 1
            else:
                break

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
