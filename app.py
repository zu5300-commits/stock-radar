from flask import Flask, jsonify
import requests as req
import urllib3
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# TWSE 憑證有 Missing Subject Key Identifier 問題，停用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)

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
    """解析 TWSE 日期格式：西元8碼 20260425 → 2026/04/25，民國7碼 1150425 → 2026/04/25"""
    try:
        raw = str(raw).strip()
        if len(raw) == 8:  # 西元格式 YYYYMMDD
            return f"{raw[:4]}/{raw[4:6]}/{raw[6:8]}"
        if len(raw) == 7:  # 民國格式 YYYMMDD
            year = int(raw[:3]) + 1911
            return f"{year}/{raw[3:5]}/{raw[5:7]}"
    except Exception:
        pass
    return raw


def recent_weekdays(n=30):
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
                        headers=TWSE_HEADERS, timeout=30, verify=False)
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
            # 實際欄位（10欄）: [代號, 名稱, 成交股數, 成交金額, 開盤, 最高, 最低, 收盤, 漲跌(含符號如+3.05), 本益比]
            code    = str(row[0]).strip()
            name    = str(row[1]).strip()
            close_s = str(row[7]).replace(",", "").strip()
            if close_s in ("--", "", "除權息", "除息", "除權", "X"):
                continue
            close = float(close_s)
            if close <= 0:
                continue

            vol_s = str(row[2]).replace(",", "").strip()
            vol   = int(vol_s) if vol_s not in ("--", "", "X") else 0

            # row[8] 是漲跌（含符號），如 "+3.05" 或 "-1.23" 或 "0.00"
            chg_s  = str(row[8]).replace(",", "").strip()
            diff   = float(chg_s) if chg_s not in ("--", "", "X", "除權息", "除息", "除權") else 0
            prev   = close - diff
            change = round(diff / prev * 100, 2) if prev > 0 else 0

            tv_s     = str(row[3]).replace(",", "").strip()
            turnover = int(tv_s) if tv_s not in ("--", "", "X") else 0

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


# ── TWSE T86 三大法人（取代 FinMind，無需帳號、無速率限制）─────────────────────

def _fetch_t86_day(date_str):
    """
    抓取 TWSE T86 三大法人買賣超（單日）。
    回傳 (date_str, {code: (foreign_net, trust_net)})
    """
    url = "https://www.twse.com.tw/rwd/zh/fund/T86"
    try:
        r = req.get(url,
                    params={"response": "json", "date": date_str, "selectType": "ALL"},
                    headers=TWSE_HEADERS, timeout=30, verify=False)
        resp = r.json()
        if resp.get("stat") != "OK":
            print(f"[T86] {date_str} stat={resp.get('stat','?')} → skip")
            return date_str, {}

        rows   = resp.get("data", [])
        fields = resp.get("fields", [])
        print(f"[T86] {date_str} rows={len(rows)}")

        # 自動偵測欄位索引（以 fields 陣列為準，預設值為已知常見格式）
        f_idx, t_idx = 4, 13
        for i, f in enumerate(fields):
            if "外資" in f and "買賣超" in f and "自營" not in f and i < 12:
                f_idx = i
            if "投信" in f and "買賣超" in f:
                t_idx = i

        day = {}
        for row in rows:
            try:
                code = str(row[0]).strip()
                # 過濾非股票列（如合計列）
                if not code or not code[0].isdigit():
                    continue
                foreign_net = to_int(row[f_idx])
                trust_net   = to_int(row[t_idx])
                day[code]   = (foreign_net, trust_net)
            except (IndexError, Exception):
                continue
        return date_str, day

    except Exception as e:
        print(f"[WARN] T86 {date_str}: {e}")
        return date_str, {}


def get_all_inst_data():
    """
    從 TWSE T86 抓取近 30 個工作日的三大法人資料，
    計算每檔股票外資 / 投信的「連續淨買超天數」。
    回傳 {code: {"foreign_days": N, "trust_days": N}}
    """
    cached = get_cache("inst_all")
    if cached:
        return cached

    dates = recent_weekdays(30)

    all_days = {}  # {date_str: {code: (f_net, t_net)}}

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_fetch_t86_day, d): d for d in dates}
        for future in as_completed(futures, timeout=90):
            date_str, day_data = future.result()
            if day_data:
                all_days[date_str] = day_data

    sorted_dates = sorted(all_days.keys(), reverse=True)

    # 收集所有股票代號
    all_codes = set()
    for day in all_days.values():
        all_codes.update(day.keys())

    result = {}
    for code in all_codes:
        f_count = 0
        t_count = 0

        # 外資連續淨買超天數
        for date in sorted_dates:
            day = all_days.get(date, {})
            if code not in day:
                continue          # 當日可能停牌，跳過不中斷
            f_net, _ = day[code]
            if f_net > 0:
                f_count += 1
            else:
                break

        # 投信連續淨買超天數
        for date in sorted_dates:
            day = all_days.get(date, {})
            if code not in day:
                continue
            _, t_net = day[code]
            if t_net > 0:
                t_count += 1
            else:
                break

        result[code] = {"foreign_days": f_count, "trust_days": t_count}

    set_cache("inst_all", result)
    return result


@app.route("/quote")
def quote():
    try:
        top100, price_data, latest_date, names = get_top100_prices()
        if not top100:
            return jsonify({
                "ok": False,
                "error": "TWSE 目前無資料，請查看 /debug-twse 了解原因"
            })

        inst_all = get_all_inst_data()

        result = {}
        for code in top100:
            p    = price_data[code]
            inst = inst_all.get(code, {"foreign_days": 0, "trust_days": 0})
            result[code] = {
                "code":         code,
                "name":         names.get(code, code),
                "sector":       "—",
                "price":        p["price"],
                "change":       p["change"],
                "volume":       p["volume"],
                "foreign_days": inst["foreign_days"],
                "trust_days":   inst["trust_days"],
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
                            headers=TWSE_HEADERS, timeout=20, verify=False)
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


@app.route("/debug-t86")
def debug_t86():
    """偵錯：顯示 TWSE T86 三大法人 API 的原始回應"""
    results = []
    for date_str in recent_weekdays(3):
        url = "https://www.twse.com.tw/rwd/zh/fund/T86"
        try:
            r = req.get(url,
                        params={"response": "json", "date": date_str, "selectType": "ALL"},
                        headers=TWSE_HEADERS, timeout=20, verify=False)
            resp = r.json()
            rows = resp.get("data", [])
            results.append({
                "date_param": date_str,
                "stat": resp.get("stat", "?"),
                "fields": resp.get("fields", []),
                "row_count": len(rows),
                "sample": rows[:3] if rows else [],
            })
        except Exception as e:
            results.append({"date_param": date_str, "error": str(e)})
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
