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


def to_int(val):
    """安全轉整數，支援含逗號字串如 '1,234,567'"""
    try:
        return int(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return 0


def calc_consecutive_days(inst, target_name):
    """
    計算某個法人（target_name）連續淨買超的天數。
    inst: FinMind TaiwanStockInstitutionalInvestorsBuySell 的資料列表
    target_name: "Foreign_Investor" 外資 / "Investment_Trust" 投信
    """
    daily = {}
    for row in inst:
        if row.get("name", "").strip() == target_name:
            date = row["date"]
            net = to_int(row.get("buy", 0)) - to_int(row.get("sell", 0))
            daily[date] = net

    count = 0
    for date in sorted(daily.keys(), reverse=True):
        if daily[date] > 0:
            count += 1
        else:
            break
    return count


def fetch_stock(code):
    try:
        start_30 = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        # 股價
        prices = fm("TaiwanStockPrice", code, start_30)
        if not prices:
            return {"code": code, "price": 0, "change": 0, "volume": 0,
                    "foreign_days": 0, "trust_days": 0, "error": True}

        latest = prices[-1]
        price = float(latest.get("close") or 0)
        prev = float(prices[-2].get("close") or price) if len(prices) >= 2 else price
        change = round((price - prev) / prev * 100, 2) if prev > 0 else 0
        volume = int(latest.get("Trading_Volume") or 0) // 1000

        # 三大法人
        inst = fm("TaiwanStockInstitutionalInvestorsBuySell", code, start_30)
        foreign_days = calc_consecutive_days(inst, "Foreign_Investor")
        trust_days = calc_consecutive_days(inst, "Investment_Trust")

        return {
            "code": code,
            "price": price,
            "change": change,
            "volume": volume,
            "foreign_days": foreign_days,
            "trust_days": trust_days,
            "error": False
        }
    except Exception as e:
        print(f"[ERR] {code}: {e}")
        return {"code": code, "price": 0, "change": 0, "volume": 0,
                "foreign_days": 0, "trust_days": 0, "error": True}


@app.route("/quote")
def quote():
    codes = request.args.get("codes", "").split(",")
    result = {}
    for code in codes:
        code = code.strip()
        if code:
            result[code] = fetch_stock(code)
    return jsonify({"ok": True, "data": result})


@app.route("/debug/<code>")
def debug(code):
    """回傳原始 FinMind 資料，用來確認欄位名稱與內容"""
    start_30 = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    inst = fm("TaiwanStockInstitutionalInvestorsBuySell", code, start_30)
    names = list({r.get("name") for r in inst})
    sample = inst[:5] if inst else []
    return jsonify({"names_found": names, "sample": sample, "total_rows": len(inst)})


@app.route("/health")
def health():
    return jsonify({"ok": True})


@app.route("/")
def index():
    base = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(base, "index.html"), encoding="utf-8") as f:
        return f.read()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
