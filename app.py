from flask import Flask, jsonify, request
import requests as req
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

app = Flask(__name__)

TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiV2luIiwiZW1haWwiOiJ6dTUzMDBAZ21haWwuY29tIn0.q5_lYazAnsTiNGKFdVNlIReL8Kq_FdwnkMd7IZKcPJI"
BASE = "https://api.finmindtrade.com/api/v4/data"

_cache = {}
_cache_lock = threading.Lock()

def get_cache(key):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and (datetime.now() - entry["time"]).seconds < 1800:
            return entry["data"]
    return None

def set_cache(key, data):
    with _cache_lock:
        _cache[key] = {"data": data, "time": datetime.now()}


def to_int(val):
    try:
        return int(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return 0


def fm(dataset, stock_id, start):
    params = {"dataset": dataset, "start_date": start, "token": TOKEN}
    if stock_id:
        params["data_id"] = stock_id
    r = req.get(BASE, params=params, timeout=20)
    return r.json().get("data", [])


def calc_consecutive_days(inst, target_name):
    daily = {}
    for row in inst:
        if row.get("name", "").strip() == target_name:
            net = to_int(row.get("buy", 0)) - to_int(row.get("sell", 0))
            daily[row["date"]] = net
    count = 0
    for date in sorted(daily.keys(), reverse=True):
        if daily[date] > 0:
            count += 1
        else:
            break
    return count


def load_stock_info():
    cached = get_cache("stock_info")
    if cached:
        return cached
    try:
        r = req.get(BASE, params={"dataset": "TaiwanStockInfo", "token": TOKEN}, timeout=30)
        data = r.json().get("data", [])
        info = {}
        for item in data:
            code = item.get("stock_id", "")
            info[code] = {
                "name": item.get("stock_name", code),
                "sector": item.get("industry_category", "—"),
            }
        set_cache("stock_info", info)
        return info
    except Exception as e:
        print(f"[WARN] TaiwanStockInfo: {e}")
        return {}


def get_top100_prices():
    cached = get_cache("top100_prices")
    if cached:
        return cached

    # 往前找 14 天，週末也能找到最近的交易日
    start = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
    try:
        r = req.get(BASE, params={
            "dataset": "TaiwanStockPrice",
            "start_date": start,
            "token": TOKEN
        }, timeout=120)
        resp = r.json()
    except Exception as e:
        print(f"[ERROR] TaiwanStockPrice request failed: {e}")
        return [], {}, None

    # 檢查 FinMind API 狀態
    status = resp.get("status", 200)
    if status != 200:
        msg = resp.get("msg", resp.get("message", "unknown"))
        print(f"[ERROR] FinMind API status={status} msg={msg}")
        return [], {}, f"FinMind API 錯誤：{msg}"

    data = resp.get("data", [])
    if not data:
        print("[WARN] TaiwanStockPrice returned empty data")
        return [], {}, None

    dates = sorted(set(d["date"] for d in data), reverse=True)
    latest_date = dates[0]
    prev_date = dates[1] if len(dates) > 1 else None

    prev_close = {}
    if prev_date:
        for d in data:
            if d["date"] == prev_date:
                prev_close[d["stock_id"]] = float(d.get("close") or 0)

    latest_prices = {}
    for d in data:
        if d["date"] == latest_date:
            code = d["stock_id"]
            close = float(d.get("close") or 0)
            if close <= 0:
                continue
            vol = to_int(d.get("Trading_Volume") or 0)
            prev = prev_close.get(code, close)
            change = round((close - prev) / prev * 100, 2) if prev > 0 else 0
            latest_prices[code] = {
                "price": round(close, 2),
                "change": change,
                "volume": vol // 1000,
                "trading_value": close * vol,
            }

    top100 = sorted(latest_prices.keys(),
                    key=lambda c: latest_prices[c]["trading_value"],
                    reverse=True)[:100]

    result = (top100, latest_prices, latest_date)
    set_cache("top100_prices", result)
    return result


def fetch_inst_one(code, start_30):
    try:
        inst = fm("TaiwanStockInstitutionalInvestorsBuySell", code, start_30)
        return code, {
            "foreign_days": calc_consecutive_days(inst, "Foreign_Investor"),
            "trust_days": calc_consecutive_days(inst, "Investment_Trust"),
        }
    except Exception as e:
        print(f"[WARN] inst {code}: {e}")
        return code, {"foreign_days": 0, "trust_days": 0}


@app.route("/quote")
def quote():
    try:
        start_30 = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        result_prices = get_top100_prices()
        top100, price_data, latest_date = result_prices
        if not top100:
            err_msg = latest_date if isinstance(latest_date, str) else "無法取得股價資料（可能是週末或 FinMind API 限制）"
            return jsonify({"ok": False, "error": err_msg})

        stock_info = load_stock_info()

        inst_results = {}
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_inst_one, code, start_30): code for code in top100}
            for future in as_completed(futures, timeout=90):
                code, inst = future.result()
                inst_results[code] = inst

        result = {}
        for code in top100:
            p = price_data[code]
            info = stock_info.get(code, {"name": code, "sector": "—"})
            result[code] = {
                "code": code,
                "name": info["name"],
                "sector": info["sector"],
                "price": p["price"],
                "change": p["change"],
                "volume": p["volume"],
                "foreign_days": inst_results.get(code, {}).get("foreign_days", 0),
                "trust_days": inst_results.get(code, {}).get("trust_days", 0),
                "error": False,
            }

        return jsonify({"ok": True, "data": result, "date": latest_date})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)})


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
