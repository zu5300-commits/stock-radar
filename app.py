from flask import Flask, jsonify
import requests as req
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

app = Flask(__name__)

FM_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiV2luIiwiZW1haWwiOiJ6dTUzMDBAZ21haWwuY29tIn0.q5_lYazAnsTiNGKFdVNlIReL8Kq_FdwnkMd7IZKcPJI"
FM_BASE  = "https://api.finmindtrade.com/api/v4/data"

TWSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": "https://www.twse.com.tw/",
    "Accept-Language": "zh-TW,zh;q=0.9",
}

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


def parse_twse_date(raw):
    """民國日期 1150425 → 2026/04/25"""
    try:
        raw = str(raw).strip()
        if len(raw) == 7:
            year  = int(raw[:3]) + 1911
            month = raw[3:5]
            day   = raw[5:7]
            return f"{year}/{month}/{day}"
    except Exception:
        pass
    return raw


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


def recent_weekdays(n=7):
    """回傳最近 n 個工作日（西元格式 YYYYMMDD）"""
    result = []
    d = datetime.now()
    while len(result) < n:
        if d.weekday() < 5:
            result.append(d.strftime("%Y%m%d"))
        d -= timedelta(days=1)
    return result


def fetch_twse_day(date_str):
    """
    嘗試從 TWSE 取得指定日期的全部上市股票資料。
    date_str: YYYYMMDD 格式
    回傳 (rows, date_label) 或 None
    """
    urls = [
        "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL",
        "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL",
    ]
    for url in urls:
        try:
            r = req.get(url, params={"response": "json", "date": date_str},
                        headers=TWSE_HEADERS, timeout=30)
            resp = r.json()
            rows = resp.get("data", [])
            print(f"[TWSE] {url} date={date_str} "
                  f"stat={resp.get('stat','?')} rows={len(rows)}")
            if rows:
                return rows, parse_twse_date(resp.get("date", date_str))
        except Exception as e:
            print(f"[ERROR] TWSE {url} date={date_str}: {e}")
    return None


def get_top100_prices():
    cached = get_cache("top100_prices")
    if cached:
        return cached

    # 依序嘗試最近 7 個工作日，取第一筆有資料的
    found = None
    for date_str in recent_weekdays(7):
        found = fetch_twse_day(date_str)
        if found:
            break

    if not found:
        print("[ERROR] All TWSE attempts returned empty data")
        return [], {}, None, {}

    rows, latest_date = found
    names = {}
    latest_prices = {}

    for row in rows:
        try:
            # [代號, 名稱, 成交股數, 成交金額, 開盤, 最高, 最低, 收盤, 漲跌符號, 漲跌價差, 本益比]
            code    = str(row[0]).strip()
            name    = str(row[1]).strip()
            close_s = str(row[7]).replace(",", "").strip()
            if close_s in ("--", "", "除權息", "除息", "除權"):
                continue
            close = float(close_s)
            if close <= 0:
                continue

            vol_s = str(row[2]).replace(",", "").strip()
            vol   = int(vol_s) if vol_s not in ("--", "") else 0

            diff_s = str(row[9]).replace(",", "").strip()
            sign   = str(row[8]).strip()
            diff   = float(diff_s) if diff_s not in ("--", "") else 0
            if sign == "-":
                diff = -diff
            prev   = close - diff
            change = round(diff / prev * 100, 2) if prev > 0 else 0

            tv_s     = str(row[3]).replace(",", "").strip()
            turnover = int(tv_s) if tv_s not in ("--", "") else 0

            names[code] = name
            latest_prices[code] = {
                "price":         round(close, 2),
                "change":        change,
                "volume":        vol // 1000,
                "trading_value": turnover,
            }
        except (ValueError, IndexError, TypeError):
            continue

    if not latest_prices:
        return [], {}, None, {}

    top100 = sorted(
        latest_prices.keys(),
        key=lambda c: latest_prices[c]["trading_value"],
        reverse=True
    )[:100]

    result = (top100, latest_prices, latest_date, names)
    set_cache("top100_prices", result)
    return result


def fetch_inst_one(code, start_30):
    try:
        r = req.get(FM_BASE, params={
            "dataset":    "TaiwanStockInstitutionalInvestorsBuySell",
            "data_id":    code,
            "start_date": start_30,
            "token":      FM_TOKEN,
        }, timeout=20)
        inst = r.json().get("data", [])
        return code, {
            "foreign_days": calc_consecutive_days(inst, "Foreign_Investor"),
            "trust_days":   calc_consecutive_days(inst, "Investment_Trust"),
        }
    except Exception as e:
        print(f"[WARN] inst {code}: {e}")
        return code, {"foreign_days": 0, "trust_days": 0}


@app.route("/quote")
def quote():
    try:
        start_30 = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        top100, price_data, latest_date, names = get_top100_prices()
        if not top100:
            return jsonify({
                "ok": False,
                "error": "TWSE 目前無資料，請查看 /debug-twse 了解原因"
            })

        inst_results = {}
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_inst_one, code, start_30): code
                       for code in top100}
            for future in as_completed(futures, timeout=90):
                code, inst = future.result()
                inst_results[code] = inst

        result = {}
        for code in top100:
            p = price_data[code]
            result[code] = {
                "code":         code,
                "name":         names.get(code, code),
                "sector":       "—",
                "price":        p["price"],
                "change":       p["change"],
                "volume":       p["volume"],
                "foreign_days": inst_results.get(code, {}).get("foreign_days", 0),
                "trust_days":   inst_results.get(code, {}).get("trust_days", 0),
                "error":        False,
            }

        return jsonify({"ok": True, "data": result, "date": latest_date})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)})


@app.route("/debug-twse")
def debug_twse():
    """偵錯：直接顯示 TWSE API 的原始回應（前 3 筆資料）"""
    results = []
    for date_str in recent_weekdays(5):
        for url in [
            "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL",
            "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL",
        ]:
            try:
                r = req.get(url, params={"response": "json", "date": date_str},
                            headers=TWSE_HEADERS, timeout=20)
                resp = r.json()
                rows = resp.get("data", [])
                results.append({
                    "url": url,
                    "date_param": date_str,
                    "stat": resp.get("stat", "?"),
                    "date_resp": resp.get("date", "?"),
                    "row_count": len(rows),
                    "sample": rows[:3] if rows else [],
                    "all_keys": list(resp.keys()),
                })
            except Exception as e:
                results.append({"url": url, "date_param": date_str, "error": str(e)})
    return jsonify(results)


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
