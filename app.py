from flask import Flask, jsonify
import requests as req
import urllib3
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# TWSE/TPEX 憑證問題，停用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)

TWSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": "https://www.twse.com.tw/",
    "Accept-Language": "zh-TW,zh;q=0.9",
}

TPEX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": "https://www.tpex.org.tw/",
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
    """西元8碼 20260425 → 2026/04/25，民國7碼 1150425 → 2026/04/25"""
    try:
        raw = str(raw).strip()
        if len(raw) == 8:
            return f"{raw[:4]}/{raw[4:6]}/{raw[6:8]}"
        if len(raw) == 7:
            year = int(raw[:3]) + 1911
            return f"{year}/{raw[3:5]}/{raw[5:7]}"
    except Exception:
        pass
    return raw


def to_roc_date(date_str):
    """YYYYMMDD → 民國 YYY/MM/DD（TPEX 用）"""
    year = int(date_str[:4]) - 1911
    return f"{year}/{date_str[4:6]}/{date_str[6:8]}"


def recent_weekdays(n=30):
    """回傳最近 n 個工作日（西元格式 YYYYMMDD）"""
    result = []
    d = datetime.now()
    while len(result) < n:
        if d.weekday() < 5:
            result.append(d.strftime("%Y%m%d"))
        d -= timedelta(days=1)
    return result


# ── TWSE 上市股票每日行情 ────────────────────────────────────────────────────────

def fetch_twse_day(date_str):
    """取得 TWSE 指定日期全部上市股票資料"""
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
            print(f"[TWSE] {url} date={date_str} stat={resp.get('stat','?')} rows={len(rows)}")
            if rows:
                return rows, parse_twse_date(resp.get("date", date_str))
        except Exception as e:
            print(f"[ERROR] TWSE {url} date={date_str}: {e}")
    return None


def _parse_twse_prices(rows):
    """解析 TWSE STOCK_DAY_ALL rows → {code: {...}}，含名稱"""
    names = {}
    prices = {}
    for row in rows:
        try:
            # [代號, 名稱, 成交股數, 成交金額, 開盤, 最高, 最低, 收盤, 漲跌(+/-), 本益比]
            code    = str(row[0]).strip()
            name    = str(row[1]).strip()
            close_s = str(row[7]).replace(",", "").strip()
            if close_s in ("--", "", "除權息", "除息", "除權", "X"):
                continue
            close = float(close_s)
            if close <= 0:
                continue
            vol_s    = str(row[2]).replace(",", "").strip()
            vol      = int(vol_s) if vol_s not in ("--", "", "X") else 0
            chg_s    = str(row[8]).replace(",", "").strip()
            diff     = float(chg_s) if chg_s not in ("--", "", "X", "除權息", "除息", "除權") else 0
            prev     = close - diff
            change   = round(diff / prev * 100, 2) if prev > 0 else 0
            tv_s     = str(row[3]).replace(",", "").strip()
            turnover = int(tv_s) if tv_s not in ("--", "", "X") else 0
            names[code] = name
            prices[code] = {
                "price":         round(close, 2),
                "change":        change,
                "volume":        vol // 1000,
                "trading_value": turnover,
                "market":        "上市",
            }
        except (ValueError, IndexError, TypeError):
            continue
    return names, prices


# ── TPEX 上櫃股票每日行情（OpenAPI）─────────────────────────────────────────────

def fetch_tpex_day(date_str):
    """
    從 TPEX OpenAPI 取得上櫃股票每日行情。
    回傳 (rows_list, date_str) 或 None。
    rows_list 為 list of dict（OpenAPI 格式）
    """
    # OpenAPI 接受西元 YYYYMMDD
    url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
    try:
        r = req.get(url, params={"date": date_str, "l": "zh-tw"},
                    headers=TPEX_HEADERS, timeout=30, verify=False)
        rows = r.json()  # list of dicts
        if isinstance(rows, list) and rows:
            print(f"[TPEX-OA] date={date_str} rows={len(rows)}")
            return rows, date_str
        print(f"[TPEX-OA] date={date_str} empty/unexpected: {str(rows)[:100]}")
    except Exception as e:
        print(f"[ERROR] TPEX-OA date={date_str}: {e}")
    return None


def _parse_tpex_prices(rows):
    """
    解析 TPEX OpenAPI rows（list of dict）→ {code: {...}}
    常見欄位名稱（OpenAPI v1）:
      SecuritiesCompanyCode / Close / Change / TradeVolume / TradeValue / CompanyName / Name
    """
    names  = {}
    prices = {}
    for row in rows:
        try:
            code = str(row.get("SecuritiesCompanyCode", "")).strip()
            if not code:
                continue
            name = str(row.get("CompanyName", row.get("Name", code))).strip()

            close_s = str(row.get("Close", "")).replace(",", "").strip()
            if close_s in ("--", "", "除權息", "除息", "除權"):
                continue
            close = float(close_s)
            if close <= 0:
                continue

            chg_s = str(row.get("Change", "0")).replace(",", "").strip()
            import re as _re
            chg_s = _re.sub(r"<[^>]+>", "", chg_s).strip()
            diff  = float(chg_s) if chg_s not in ("--", "", "除權息", "除息", "除權") else 0
            prev  = close - diff
            change = round(diff / prev * 100, 2) if prev > 0 else 0

            vol_s = str(row.get("TradeVolume", "0")).replace(",", "").strip()
            vol   = int(vol_s) if vol_s not in ("--", "") else 0

            tv_s     = str(row.get("TradeValue", "0")).replace(",", "").strip()
            turnover = int(tv_s) if tv_s not in ("--", "") else 0

            names[code] = name
            prices[code] = {
                "price":         round(close, 2),
                "change":        change,
                "volume":        vol // 1000,
                "trading_value": turnover,
                "market":        "上櫃",
            }
        except (ValueError, KeyError, TypeError):
            continue
    return names, prices


# ── 合併上市＋上櫃，取成交值前 100 ────────────────────────────────────────────────

def get_top100_prices():
    cached = get_cache("top100_prices")
    if cached:
        return cached

    # 1. 上市
    twse_found = None
    for date_str in recent_weekdays(7):
        twse_found = fetch_twse_day(date_str)
        if twse_found:
            break

    # 2. 上櫃
    tpex_found = None
    for date_str in recent_weekdays(7):
        tpex_found = fetch_tpex_day(date_str)
        if tpex_found:
            break

    all_names  = {}
    all_prices = {}
    latest_date = None

    if twse_found:
        rows, latest_date = twse_found
        twse_names, twse_prices = _parse_twse_prices(rows)
        all_names.update(twse_names)
        all_prices.update(twse_prices)

    if tpex_found:
        rows, _ = tpex_found
        tpex_names, tpex_prices = _parse_tpex_prices(rows)
        all_names.update(tpex_names)
        all_prices.update(tpex_prices)

    if not all_prices:
        print("[ERROR] No price data from TWSE or TPEX")
        return [], {}, None, {}

    top100 = sorted(
        all_prices.keys(),
        key=lambda c: all_prices[c]["trading_value"],
        reverse=True
    )[:100]

    result = (top100, all_prices, latest_date, all_names)
    set_cache("top100_prices", result)
    return result


# ── 三大法人：TWSE T86 + TPEX 合併 ────────────────────────────────────────────

def _fetch_t86_day(date_str):
    """TWSE T86 三大法人（上市）"""
    url = "https://www.twse.com.tw/rwd/zh/fund/T86"
    try:
        r = req.get(url,
                    params={"response": "json", "date": date_str, "selectType": "ALL"},
                    headers=TWSE_HEADERS, timeout=30, verify=False)
        resp = r.json()
        if resp.get("stat") != "OK":
            return date_str, {}
        rows   = resp.get("data", [])
        fields = resp.get("fields", [])
        print(f"[T86-TWSE] {date_str} rows={len(rows)}")

        # 自動偵測欄位索引
        f_idx, t_idx = 4, 10
        for i, f in enumerate(fields):
            if "外" in f and "買賣超" in f and "自營" not in f and i < 8:
                f_idx = i
            if "投信" in f and "買賣超" in f:
                t_idx = i

        day = {}
        for row in rows:
            try:
                code = str(row[0]).strip()
                if not code or not code[0].isdigit():
                    continue
                day[code] = (to_int(row[f_idx]), to_int(row[t_idx]))
            except (IndexError, Exception):
                continue
        return date_str, day
    except Exception as e:
        print(f"[WARN] T86-TWSE {date_str}: {e}")
        return date_str, {}


def _fetch_tpex_inst_day(date_str):
    """
    TPEX 三大法人（上櫃）— 使用 OpenAPI v1。
    回傳 (date_str, {code: (foreign_net, trust_net)})
    常見欄位: SecurityCode, ForeignNetBuyShares / InvestmentTrustNetBuyShares
              (或 ForeignInvestorNetBuy / InvestmentTrustNetBuy)
    """
    url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_institution_buy_sell_total"
    try:
        r = req.get(url, params={"date": date_str, "l": "zh-tw"},
                    headers=TPEX_HEADERS, timeout=30, verify=False)
        rows = r.json()
        print(f"[T86-TPEX-OA] {date_str} rows={len(rows) if isinstance(rows,list) else 'N/A'}")
        if not isinstance(rows, list) or not rows:
            return date_str, {}

        # 動態偵測欄位名稱（不同版本 API 欄位名可能不同）
        sample = rows[0]
        code_key = next((k for k in sample if "code" in k.lower() or "Code" in k), None)
        f_key    = next((k for k in sample if "Foreign" in k and "Net" in k and "Share" in k), None)
        t_key    = next((k for k in sample if "Investment" in k and "Net" in k and "Share" in k), None)
        if not f_key:
            f_key = next((k for k in sample if "Foreign" in k and ("Net" in k or "net" in k)), None)
        if not t_key:
            t_key = next((k for k in sample if ("Trust" in k or "Investment" in k) and ("Net" in k or "net" in k)), None)
        print(f"[T86-TPEX-OA] keys detected: code={code_key} f={f_key} t={t_key}")
        print(f"[T86-TPEX-OA] sample keys: {list(sample.keys())}")

        if not code_key:
            return date_str, {}

        day = {}
        for row in rows:
            try:
                code  = str(row.get(code_key, "")).strip()
                if not code or not code[0].isdigit():
                    continue
                f_net = to_int(row.get(f_key, 0)) if f_key else 0
                t_net = to_int(row.get(t_key, 0)) if t_key else 0
                day[code] = (f_net, t_net)
            except Exception:
                continue
        return date_str, day

    except Exception as e:
        print(f"[WARN] T86-TPEX-OA {date_str}: {e}")
        return date_str, {}


def get_all_inst_data():
    """
    合併 TWSE T86 + TPEX 三大法人，計算每股連續淨買超天數。
    回傳 {code: {"foreign_days": N, "trust_days": N}}
    """
    cached = get_cache("inst_all")
    if cached:
        return cached

    dates = recent_weekdays(30)
    all_days = {}  # {date_str: {code: (f_net, t_net)}}

    def _fetch_both(date_str):
        _, twse_day = _fetch_t86_day(date_str)
        _, tpex_day = _fetch_tpex_inst_day(date_str)
        merged = {}
        merged.update(twse_day)
        merged.update(tpex_day)
        return date_str, merged

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_fetch_both, d): d for d in dates}
        for future in as_completed(futures, timeout=120):
            date_str, day_data = future.result()
            if day_data:
                all_days[date_str] = day_data

    sorted_dates = sorted(all_days.keys(), reverse=True)

    all_codes = set()
    for day in all_days.values():
        all_codes.update(day.keys())

    result = {}
    for code in all_codes:
        f_count = 0
        t_count = 0
        for date in sorted_dates:
            day = all_days.get(date, {})
            if code not in day:
                continue
            if day[code][0] > 0:
                f_count += 1
            else:
                break
        for date in sorted_dates:
            day = all_days.get(date, {})
            if code not in day:
                continue
            if day[code][1] > 0:
                t_count += 1
            else:
                break
        result[code] = {"foreign_days": f_count, "trust_days": t_count}

    set_cache("inst_all", result)
    return result


# ── API 路由 ──────────────────────────────────────────────────────────────────

@app.route("/quote")
def quote():
    try:
        top100, price_data, latest_date, names = get_top100_prices()
        if not top100:
            return jsonify({
                "ok": False,
                "error": "TWSE/TPEX 目前無資料，請查看 /debug-twse 了解原因"
            })

        inst_all = get_all_inst_data()

        result = {}
        for code in top100:
            p    = price_data[code]
            inst = inst_all.get(code, {"foreign_days": 0, "trust_days": 0})
            result[code] = {
                "code":         code,
                "name":         names.get(code, code),
                "sector":       p.get("market", "—"),
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
    results = []
    for date_str in recent_weekdays(3):
        for url in [
            "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL",
        ]:
            try:
                r = req.get(url, params={"response": "json", "date": date_str},
                            headers=TWSE_HEADERS, timeout=20, verify=False)
                resp = r.json()
                rows = resp.get("data", [])
                results.append({
                    "url": url, "date_param": date_str,
                    "stat": resp.get("stat", "?"),
                    "row_count": len(rows),
                    "sample": rows[:2] if rows else [],
                })
            except Exception as e:
                results.append({"url": url, "date_param": date_str, "error": str(e)})
    return jsonify(results)


@app.route("/debug-t86")
def debug_t86():
    results = []
    for date_str in recent_weekdays(2):
        # TWSE T86
        url = "https://www.twse.com.tw/rwd/zh/fund/T86"
        try:
            r = req.get(url,
                        params={"response": "json", "date": date_str, "selectType": "ALL"},
                        headers=TWSE_HEADERS, timeout=20, verify=False)
            resp = r.json()
            rows = resp.get("data", [])
            results.append({
                "source": "TWSE-T86", "date": date_str,
                "stat": resp.get("stat", "?"),
                "fields": resp.get("fields", []),
                "row_count": len(rows),
                "sample": rows[:2] if rows else [],
            })
        except Exception as e:
            results.append({"source": "TWSE-T86", "date": date_str, "error": str(e)})

        # TPEX 三大法人 (OpenAPI)
        url2 = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_institution_buy_sell_total"
        try:
            r = req.get(url2, params={"date": date_str, "l": "zh-tw"},
                        headers=TPEX_HEADERS, timeout=20, verify=False)
            rows = r.json()
            sample_keys = list(rows[0].keys()) if isinstance(rows, list) and rows else []
            results.append({
                "source": "TPEX-inst-OA", "date": date_str,
                "row_count": len(rows) if isinstance(rows, list) else 0,
                "sample_keys": sample_keys,
                "sample": rows[:2] if isinstance(rows, list) else rows,
            })
        except Exception as e:
            results.append({"source": "TPEX-inst-OA", "date": date_str, "error": str(e)})

        # TPEX 行情 (OpenAPI)
        url3 = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
        try:
            r = req.get(url3, params={"date": date_str, "l": "zh-tw"},
                        headers=TPEX_HEADERS, timeout=20, verify=False)
            rows = r.json()
            sample_keys = list(rows[0].keys()) if isinstance(rows, list) and rows else []
            results.append({
                "source": "TPEX-price-OA", "date": date_str,
                "row_count": len(rows) if isinstance(rows, list) else 0,
                "sample_keys": sample_keys,
                "sample": rows[:2] if isinstance(rows, list) else rows,
            })
        except Exception as e:
            results.append({"source": "TPEX-price-OA", "date": date_str, "error": str(e)})
    return jsonify(results)


@app.route("/debug-tpex-raw")
def debug_tpex_raw():
    """回傳 TPEX API 的原始 JSON（用於除錯結構）"""
    roc = to_roc_date(recent_weekdays(3)[1])  # 前天
    results = {}
    # 行情
    try:
        r = req.get(
            "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php",
            params={"l": "zh-tw", "d": roc, "se": "EW", "s": "0,asc,0"},
            headers=TPEX_HEADERS, timeout=20, verify=False)
        raw = r.json()
        tables = raw.get("tables", [])
        aaData = raw.get("aaData", [])
        results["price"] = {
            "keys": list(raw.keys()),
            "stat": raw.get("stat"),
            "aaData_len": len(aaData),
            "aaData_sample": aaData[:1] if aaData else [],
            "tables_len": len(tables),
            "tables_t0_keys": list(tables[0].keys()) if tables else [],
            "tables_t0_sample": str(tables[0])[:500] if tables else "",
        }
    except Exception as e:
        results["price"] = {"error": str(e)}
    # 三大法人
    try:
        r = req.get(
            "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php",
            params={"l": "zh-tw", "o": "json", "se": "EW", "t": "D", "d": roc},
            headers=TPEX_HEADERS, timeout=20, verify=False)
        raw = r.json()
        tables = raw.get("tables", [])
        aaData = raw.get("aaData", [])
        results["inst"] = {
            "keys": list(raw.keys()),
            "stat": raw.get("stat"),
            "aaData_len": len(aaData),
            "aaData_sample": aaData[:1] if aaData else [],
            "tables_len": len(tables),
            "tables_t0_keys": list(tables[0].keys()) if tables else [],
            "tables_t0_sample": str(tables[0])[:500] if tables else "",
        }
    except Exception as e:
        results["inst"] = {"error": str(e)}
    return jsonify({"roc": roc, "results": results})


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
