from flask import Flask, jsonify, request
import requests as req
import urllib3
import re
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# TWSE/TPEX 憑證問題，停用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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

TPEX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": "https://www.tpex.org.tw/",
    "Accept-Language": "zh-TW,zh;q=0.9",
}

_cache = {}
_cache_lock = threading.Lock()


def get_cache(key, ttl=1800):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and (datetime.now() - entry["time"]).total_seconds() < ttl:
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
            chg_s = re.sub(r"<[^>]+>", "", chg_s).strip()
            diff  = float(chg_s) if chg_s not in ("--", "", "除權息", "除息", "除權") else 0
            prev  = close - diff
            change = round(diff / prev * 100, 2) if prev > 0 else 0

            # TradingShares (股) ÷ 1000 = 張數
            vol_s = str(row.get("TradingShares", row.get("TradeVolume", "0"))).replace(",", "").strip()
            vol   = int(vol_s) // 1000 if vol_s not in ("--", "") else 0

            # TransactionAmount = 成交金額（NTD），用於排行
            tv_s     = str(row.get("TransactionAmount", row.get("TradeValue", "0"))).replace(",", "").strip()
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

    # TWSE 和 TPEX 並行抓取
    def _try_twse():
        for d in recent_weekdays(7):
            r = fetch_twse_day(d)
            if r:
                return r
        return None

    def _try_tpex():
        for d in recent_weekdays(7):
            r = fetch_tpex_day(d)
            if r:
                return r
        return None

    with ThreadPoolExecutor(max_workers=2) as ex:
        f_twse = ex.submit(_try_twse)
        f_tpex = ex.submit(_try_tpex)
        twse_found = f_twse.result(timeout=40)
        tpex_found = f_tpex.result(timeout=40)

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
    """TWSE T86 三大法人（上市），失敗最多重試 2 次"""
    import time as _time
    url = "https://www.twse.com.tw/rwd/zh/fund/T86"
    for attempt in range(3):
      try:
        r = req.get(url,
                    params={"response": "json", "date": date_str, "selectType": "ALL"},
                    headers=TWSE_HEADERS, timeout=30, verify=False)
        resp = r.json()
        if resp.get("stat") != "OK":
            if attempt < 2:
                _time.sleep(2)
                continue
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
        print(f"[WARN] T86-TWSE {date_str} attempt={attempt}: {e}")
        if attempt < 2:
            _time.sleep(2)
    return date_str, {}


def _fetch_tpex_inst_day(date_str):
    """
    TPEX 三大法人（上櫃）— 使用 3itrade_hedge_result.php。
    回傳 (date_str, {code: (foreign_net, trust_net)})
    失敗最多重試 2 次。

    新版 API 資料在 tables[0]["data"]（舊版在頂層 aaData）。
    欄位索引（25欄，比 TWSE 多一欄「外資合計」）:
      [0]=代號 [1]=名稱
      [2-4]=外資(不含自營商) buy/sell/net  ← f_net = [4]
      [5-7]=外資自營商 buy/sell/net
      [8-10]=外資合計 buy/sell/net
      [11-13]=投信 buy/sell/net            ← t_net = [13]
      其餘=自營商各項
    """
    import time as _time
    roc = to_roc_date(date_str)
    url = ("https://www.tpex.org.tw/web/stock/3insti/daily_trade/"
           "3itrade_hedge_result.php")
    for attempt in range(3):
      try:
        r = req.get(url,
                    params={"l": "zh-tw", "o": "json", "se": "EW", "t": "D", "d": roc},
                    headers=TPEX_HEADERS, timeout=30, verify=False)
        resp = r.json()

        # 新版：tables[0]["data"]；舊版：頂層 aaData（向下相容）
        rows = resp.get("aaData", [])
        if not rows:
            tables = resp.get("tables", [])
            if tables:
                rows = tables[0].get("data", [])

        print(f"[T86-TPEX] {date_str} roc={roc} rows={len(rows)} attempt={attempt}")
        if not rows:
            if attempt < 2:
                _time.sleep(2)
                continue
            return date_str, {}

        # 自動偵測外資/投信欄位索引
        fields = []
        tables = resp.get("tables", [])
        if tables:
            fields = tables[0].get("fields", [])
        f_idx, t_idx = 4, 13  # TPEX 預設
        for i, f in enumerate(fields):
            if "外" in f and "買賣超" in f and "自營" not in f and "合計" not in f and i < 8:
                f_idx = i
            if "投信" in f and "買賣超" in f:
                t_idx = i

        day = {}
        for row in rows:
            try:
                code = str(row[0]).strip()
                if not code or not code[0].isdigit():
                    continue
                f_net = to_int(row[f_idx])
                t_net = to_int(row[t_idx])
                day[code] = (f_net, t_net)
            except (IndexError, Exception):
                continue
        return date_str, day

      except Exception as e:
        print(f"[WARN] T86-TPEX {date_str} attempt={attempt}: {e}")
        if attempt < 2:
            _time.sleep(2)
    return date_str, {}


_inst_bg_lock  = threading.Lock()
_inst_bg_running = False


def _compute_inst(n_days):
    """抓 n_days 個工作日的三大法人資料，回傳 {code: {foreign_days, trust_days}}"""
    dates    = recent_weekdays(n_days)
    all_days = {}
    _lock    = threading.Lock()

    def _merge(date_str, day_data):
        if day_data:
            with _lock:
                all_days.setdefault(date_str, {}).update(day_data)

    # workers=6：避免同時轟炸 TWSE/TPEX 被限速，分批送出確保資料完整
    with ThreadPoolExecutor(max_workers=6) as executor:
        twse_futs = {executor.submit(_fetch_t86_day, d): d for d in dates}
        tpex_futs = {executor.submit(_fetch_tpex_inst_day, d): d for d in dates}
        for future in as_completed({**twse_futs, **tpex_futs}, timeout=120):
            try:
                date_str, day_data = future.result()
                _merge(date_str, day_data)
            except Exception:
                pass

    sorted_dates = sorted(all_days.keys(), reverse=True)
    all_codes    = set(c for d in all_days.values() for c in d)
    result = {}
    for code in all_codes:
        f_count = t_count = 0
        for date in sorted_dates:
            day = all_days.get(date, {})
            if code not in day: continue
            if day[code][0] > 0: f_count += 1
            else: break
        for date in sorted_dates:
            day = all_days.get(date, {})
            if code not in day: continue
            if day[code][1] > 0: t_count += 1
            else: break
        result[code] = {"foreign_days": f_count, "trust_days": t_count}
    return result


def _start_bg_inst_fetch():
    """如果沒有在跑，啟動背景 thread 抓 20 天法人資料"""
    global _inst_bg_running
    with _inst_bg_lock:
        if _inst_bg_running:
            return
        _inst_bg_running = True

    def _bg():
        global _inst_bg_running
        try:
            result = _compute_inst(20)
            set_cache("inst_all", result)
            print(f"[BG] 20-day inst fetch done, {len(result)} stocks")
        except Exception as e:
            print(f"[BG] inst fetch error: {e}")
        finally:
            with _inst_bg_lock:
                _inst_bg_running = False

    threading.Thread(target=_bg, daemon=True).start()
    print("[BG] 20-day inst background fetch started")


# ── TWSE BWIBBU_ALL：殖利率 ───────────────────────────────────────────────────

def fetch_bwibbu():
    """取 TWSE 所有上市股票的殖利率（%）→ {code: float|None}"""
    cached = get_cache("bwibbu")
    if cached is not None:
        return cached
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_ALL"
    result = {}
    for d in recent_weekdays(5):
        try:
            r = req.get(url, params={"response": "json", "date": d},
                        headers=TWSE_HEADERS, timeout=20, verify=False)
            resp   = r.json()
            rows   = resp.get("data", [])
            fields = resp.get("fields", [])
            if not rows:
                continue
            # 自動偵測殖利率欄位索引（預設 col=2）
            y_idx = 2
            for i, f in enumerate(fields):
                if "殖利率" in f:
                    y_idx = i
                    break
            for row in rows:
                try:
                    code  = str(row[0]).strip()
                    val_s = str(row[y_idx]).replace(",", "").strip()
                    result[code] = float(val_s) if val_s not in ("--", "", "N/A") else None
                except Exception:
                    continue
            print(f"[BWIBBU] {d} → {len(result)} stocks")
            break
        except Exception as e:
            print(f"[BWIBBU] {d}: {e}")
    set_cache("bwibbu", result)
    return result


# ── FinMind 研發基本面 ─────────────────────────────────────────────────────────

def _fm_get(dataset, data_id=None, start_date=None):
    """FinMind API 單次呼叫，回傳 data list"""
    params = {"dataset": dataset, "token": FM_TOKEN}
    if data_id:
        params["data_id"] = data_id
    if start_date:
        params["start_date"] = start_date
    try:
        r = req.get(FM_BASE, params=params, timeout=30)
        resp = r.json()
        if resp.get("status") == 200:
            return resp.get("data", [])
        print(f"[FM] {dataset}/{data_id} status={resp.get('status')} msg={resp.get('msg','')}")
        return []
    except Exception as e:
        print(f"[FM] {dataset}/{data_id}: {e}")
        return []


_rd_bg_lock    = threading.Lock()
_rd_bg_running = False


def _compute_rd_data(codes):
    """
    對 codes 抓 FinMind:
    - TaiwanStockBalanceSheet (5年) → 5年平均負債比
    - TaiwanStockFinancialStatements (近5季) → 近4季研發費用合計
    - TaiwanStockInfo → 流通股數（計算市值）
    回傳 {code: {debt_ratio, rd_expense, shares}}
    """
    now      = datetime.now()
    start_5y = (now - timedelta(days=365 * 5 + 60)).strftime("%Y-%m-%d")
    start_5q = (now - timedelta(days=420)).strftime("%Y-%m-%d")

    # ① 一次取全部股票基本資訊（流通股數）
    shares_map = {}
    try:
        for row in _fm_get("TaiwanStockInfo"):
            code   = str(row.get("stock_id", "")).strip()
            shares = to_int(row.get("sharesissued", 0))
            if code and shares > 0:
                shares_map[code] = shares
        print(f"[RD] TaiwanStockInfo: {len(shares_map)} stocks")
    except Exception as e:
        print(f"[RD] TaiwanStockInfo: {e}")

    result = {}
    RD_TYPES = {
        "ResearchAndDevelopmentExpenses",
        "ResearchDevelopmentExpense",
        "研究發展費用", "研究費用", "研究與發展費用",
    }

    def _fetch_one(code):
        bs = _fm_get("TaiwanStockBalanceSheet",        code, start_5y)
        fs = _fm_get("TaiwanStockFinancialStatements", code, start_5q)
        return code, bs, fs

    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {ex.submit(_fetch_one, c): c for c in codes}
        for fut in as_completed(futs, timeout=360):
            code = futs[fut]
            try:
                _, bs, fs = fut.result()

                # ── 5年平均負債比 ─────────────────────────────────────────────
                # FinMind BalanceSheet 有 TotalAssets(資產總額) + Equity(權益總額)
                # 無獨立 TotalLiabilities；負債比 = (Assets-Equity)/Assets×100
                by_year = {}  # year → {assets, equity}
                for row in bs:
                    try:
                        year = str(row.get("date", ""))[:4]
                        typ  = str(row.get("type", ""))
                        val  = to_int(row.get("value", 0))
                        if typ in ("TotalAssets", "資產總額", "資產總計"):
                            by_year.setdefault(year, {})["assets"] = val
                        elif typ in ("Equity", "權益總額", "權益總計"):
                            by_year.setdefault(year, {})["equity"] = val
                    except Exception:
                        continue
                ratios = [
                    (d["assets"] - d["equity"]) / d["assets"] * 100
                    for d in by_year.values()
                    if d.get("assets", 0) > 0 and "equity" in d
                ]
                debt_ratio = round(sum(ratios) / len(ratios), 1) if ratios else None

                # ── 近4季研發費用合計 ─────────────────────────────────────────
                rd_rows = sorted(
                    [r for r in fs if str(r.get("type", "")) in RD_TYPES],
                    key=lambda r: r.get("date", ""),
                    reverse=True
                )[:4]
                rd_expense = sum(to_int(r.get("value", 0)) for r in rd_rows)

                result[code] = {
                    "debt_ratio": debt_ratio,
                    "rd_expense": rd_expense,
                    "shares":     shares_map.get(code, 0),
                }
            except Exception as e:
                print(f"[RD] {code}: {e}")

    print(f"[RD] _compute_rd_data done: {len(result)} stocks")
    return result


def _start_bg_rd_fetch(codes):
    global _rd_bg_running
    with _rd_bg_lock:
        if _rd_bg_running:
            return
        _rd_bg_running = True

    def _bg():
        global _rd_bg_running
        try:
            result = _compute_rd_data(codes)
            set_cache("rd_data", result)
            print(f"[BG] RD data done: {len(result)} stocks")
        except Exception as e:
            print(f"[BG] RD error: {e}")
        finally:
            with _rd_bg_lock:
                _rd_bg_running = False

    threading.Thread(target=_bg, daemon=True).start()
    print("[BG] RD background fetch started")


def get_rd_data(codes):
    """回傳研發基本面快取（24h TTL），沒有就背景抓，先回空"""
    cached = get_cache("rd_data", ttl=86400)
    if cached is not None:
        return cached
    _start_bg_rd_fetch(codes)
    return {}


def get_all_inst_data():
    """
    回傳三大法人連續買超天數。
    - 有 20 天快取 → 直接用（≥20 天篩選準確）
    - 沒有 → 同步抓 7 天（快速，確保 /quote 不超時），
              並在背景抓 20 天（下次請求時即可使用）
    """
    # 20 天完整資料優先
    cached = get_cache("inst_all")
    if cached:
        return cached

    # 7 天快速資料（同步，不超時）
    fast = get_cache("inst_fast")
    if not fast:
        fast = _compute_inst(7)
        set_cache("inst_fast", fast)

    # 背景補齊 20 天
    _start_bg_inst_fetch()
    return fast


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

        # 基本：成交值前 100
        codes = list(top100)

        # 補充：法人連續買超 ≥20 天，但不在前 100 的股票
        # （price_data 已含全市場資料，不需額外 API call）
        BUYUP_DAYS = 20
        for code, inst in inst_all.items():
            if (inst["foreign_days"] >= BUYUP_DAYS or inst["trust_days"] >= BUYUP_DAYS):
                if code in price_data and code not in codes:
                    codes.append(code)

        # 研發基本面（背景抓，第一次為空不阻塞）
        rd_data = get_rd_data(list(top100))
        bwibbu  = fetch_bwibbu()

        result = {}
        for code in codes:
            p    = price_data[code]
            inst = inst_all.get(code, {"foreign_days": 0, "trust_days": 0})
            rd   = rd_data.get(code, {})

            # 計算市值/研發費用比
            price    = p["price"]
            shares   = rd.get("shares", 0)
            rd_exp   = rd.get("rd_expense", 0)
            mkt_cap  = price * shares                                   # 元
            rd_ratio = round(mkt_cap / rd_exp, 1) if rd_exp > 0 and mkt_cap > 0 else None

            result[code] = {
                "code":         code,
                "name":         names.get(code, code),
                "sector":       p.get("market", "—"),
                "price":        price,
                "change":       p["change"],
                "volume":       p["volume"],
                "foreign_days": inst["foreign_days"],
                "trust_days":   inst["trust_days"],
                "div_yield":    bwibbu.get(code),     # 殖利率（%）
                "debt_ratio":   rd.get("debt_ratio"), # 負債比（%）
                "rd_ratio":     rd_ratio,             # 市值/研發費用
                "error":        False,
            }

        return jsonify({"ok": True, "data": result, "date": latest_date})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)})


@app.route("/debug-inst")
def debug_inst():
    """檢查指定代號在快取裡的法人天數，及它是否在 price_data 裡"""
    codes_to_check = request.args.get("codes", "00763U,00740B").split(",")
    top100, price_data, latest_date, names = get_top100_prices()
    inst_all  = get_cache("inst_all")
    inst_fast = get_cache("inst_fast")
    result = {}
    for code in codes_to_check:
        code = code.strip()
        result[code] = {
            "in_price_data": code in price_data,
            "price":         price_data.get(code, {}).get("price"),
            "market":        price_data.get(code, {}).get("market"),
            "inst_all":      inst_all.get(code)  if inst_all  else "cache_empty",
            "inst_fast":     inst_fast.get(code) if inst_fast else "cache_empty",
            "in_top100":     code in top100,
        }
    return jsonify({
        "inst_all_exists":  inst_all  is not None,
        "inst_fast_exists": inst_fast is not None,
        "inst_all_size":    len(inst_all)  if inst_all  else 0,
        "max_foreign_all":  max((v["foreign_days"] for v in inst_all.values()),  default=0) if inst_all  else 0,
        "codes": result,
    })


@app.route("/debug-mops")
def debug_mops():
    """查 MOPS ajax_t163sb04 原始 HTML 格式（前 3000 字）"""
    now = datetime.now()
    roc_year = now.year - 1911
    # 目前季別
    season = (now.month - 1) // 3  # 取「上一季」確保有資料
    if season == 0:
        season = 4
        roc_year -= 1
    url = "https://mops.twse.com.tw/mops/web/ajax_t163sb04"
    results = {}
    for typek in ("sii", "otc"):
        try:
            r = req.post(url, data={
                "step": "1", "firstin": "1", "off": "1",
                "keyword4": "", "code1": "", "TYPEK": typek,
                "year": str(roc_year), "season": str(season),
            }, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Referer": "https://mops.twse.com.tw/",
                "Content-Type": "application/x-www-form-urlencoded",
            }, timeout=30, verify=False)
            text = r.text
            # 找含 4958 或 研究 的片段
            idx = text.find("4958")
            snippet = text[max(0,idx-100):idx+300] if idx >= 0 else text[:500]
            results[typek] = {
                "status": r.status_code,
                "len": len(text),
                "year_season": f"{roc_year}Q{season}",
                "snippet_4958": snippet,
                "first500": text[:500],
            }
        except Exception as e:
            results[typek] = {"error": str(e)}
    return jsonify(results)


@app.route("/debug-rd")
def debug_rd():
    """檢查研發基本面快取；?codes=4958,2489 可指定標的"""
    codes_to_check = request.args.get("codes", "4958,2489,3481,3035").split(",")
    rd_data = get_cache("rd_data", ttl=86400)
    bwibbu  = get_cache("bwibbu")
    out = {}
    top100, price_data, _, _ = get_top100_prices()
    for code in codes_to_check:
        code = code.strip()
        rd   = (rd_data or {}).get(code, {})
        p    = price_data.get(code, {})
        price = p.get("price", 0)
        shares   = rd.get("shares", 0)
        rd_exp   = rd.get("rd_expense", 0)
        mkt_cap  = price * shares
        rd_ratio = round(mkt_cap / rd_exp, 1) if rd_exp > 0 and mkt_cap > 0 else None
        out[code] = {
            "price":      price,
            "shares":     shares,
            "mkt_cap_億": round(mkt_cap / 1e8, 1) if mkt_cap else None,
            "rd_expense_億": round(rd_exp / 1e8, 1) if rd_exp else None,
            "rd_ratio":   rd_ratio,
            "debt_ratio": rd.get("debt_ratio"),
            "div_yield":  bwibbu.get(code) if bwibbu else None,
        }
    return jsonify({
        "rd_cache_exists":   rd_data is not None,
        "rd_cache_size":     len(rd_data) if rd_data else 0,
        "bwibbu_cache_exists": bwibbu is not None,
        "bwibbu_cache_size":   len(bwibbu) if bwibbu else 0,
        "codes": out,
    })


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

        # TPEX 三大法人 (tables[0]["data"] 新格式)
        roc = to_roc_date(date_str)
        url2 = ("https://www.tpex.org.tw/web/stock/3insti/daily_trade/"
                "3itrade_hedge_result.php")
        try:
            r = req.get(url2,
                        params={"l": "zh-tw", "o": "json", "se": "EW", "t": "D", "d": roc},
                        headers=TPEX_HEADERS, timeout=20, verify=False)
            resp2 = r.json()
            rows = resp2.get("aaData", [])
            if not rows:
                tabs = resp2.get("tables", [])
                if tabs:
                    rows = tabs[0].get("data", [])
            fields2 = []
            tabs2 = resp2.get("tables", [])
            if tabs2:
                fields2 = tabs2[0].get("fields", [])
            results.append({
                "source": "TPEX-inst", "date": date_str, "roc": roc,
                "row_count": len(rows),
                "col_count": len(rows[0]) if rows else 0,
                "fields": fields2,
                "sample": rows[:2] if rows else [],
            })
        except Exception as e:
            results.append({"source": "TPEX-inst", "date": date_str, "error": str(e)})

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
    """測試 TPEX 法人 API 各種參數組合，找出哪個真的回傳資料"""
    dates = recent_weekdays(3)
    date_str = dates[1]  # 前一個工作日
    roc_slash = to_roc_date(date_str)          # 115/04/28
    roc_plain = roc_slash.replace("/", "")     # 1150428
    results = []

    inst_url = ("https://www.tpex.org.tw/web/stock/3insti/daily_trade/"
                "3itrade_hedge_result.php")

    # 測試多種參數組合
    combos = [
        {"l": "zh-tw", "o": "json", "se": "EW", "t": "D", "d": roc_slash},
        {"l": "zh-tw", "o": "json", "se": "EW", "t": "D", "d": roc_plain},
        {"l": "zh-tw", "o": "json", "se": "EW",            "d": roc_slash},
        {"l": "zh-tw", "o": "json",               "t": "D", "d": roc_slash},
        {"l": "zh-tw", "o": "json", "se": "AL", "t": "D", "d": roc_slash},
    ]
    for p in combos:
        try:
            r = req.get(inst_url, params=p, headers=TPEX_HEADERS,
                        timeout=15, verify=False)
            raw = r.json()
            aa    = raw.get("aaData", [])
            tabs  = raw.get("tables", [])
            tab0_keys  = list(tabs[0].keys()) if tabs else []
            tab0_aa    = tabs[0].get("aaData", []) if tabs else []
            tab0_data  = tabs[0].get("data", [])   if tabs else []
            results.append({
                "params": p,
                "status": r.status_code,
                "keys": list(raw.keys()),
                "stat": raw.get("stat"),
                "aaData_len": len(aa),
                "tables_len": len(tabs),
                "tables_t0_keys": tab0_keys,
                "tables_t0_aaData_len": len(tab0_aa),
                "tables_t0_data_len": len(tab0_data) if isinstance(tab0_data, list) else f"type:{type(tab0_data).__name__}",
                "tables_t0_totalCount": tabs[0].get("totalCount") if tabs else None,
                "sample": aa[0] if aa else (tab0_data[0] if isinstance(tab0_data, list) and tab0_data else []),
            })
        except Exception as e:
            results.append({"params": p, "error": str(e)})

    # 第一個 combo 顯示原始 response 文字
    try:
        r0 = req.get(inst_url,
                     params={"l": "zh-tw", "o": "json", "se": "EW", "t": "D", "d": roc_slash},
                     headers=TPEX_HEADERS, timeout=15, verify=False)
        results.append({
            "raw_text_first200": r0.text[:200],
            "content_type": r0.headers.get("Content-Type", ""),
        })
    except Exception as e:
        results.append({"raw_error": str(e)})

    # 試 POST 方式
    try:
        rp = req.post(inst_url,
                      data={"l": "zh-tw", "o": "json", "se": "EW", "t": "D", "d": roc_slash},
                      headers={**TPEX_HEADERS,
                                "Referer": "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge.php",
                                "X-Requested-With": "XMLHttpRequest"},
                      timeout=15, verify=False)
        rp_j = rp.json()
        aa_p = rp_j.get("aaData", [])
        results.append({
            "method": "POST",
            "status": rp.status_code,
            "aaData_len": len(aa_p),
            "stat": rp_j.get("stat"),
            "sample": aa_p[0] if aa_p else [],
        })
    except Exception as e:
        results.append({"method": "POST", "error": str(e)})

    # 也試試較新的 OpenAPI v1 institution 端點（不同名稱）
    for oa_name in ["tpex_mainboard_institution_buy_sell_total",
                    "tpex_mainboard_3investors_buy_sell"]:
        u = f"https://www.tpex.org.tw/openapi/v1/{oa_name}?date={date_str}&l=zh-tw"
        try:
            r = req.get(u, headers=TPEX_HEADERS, timeout=15, verify=False)
            is_json = "json" in r.headers.get("Content-Type", "")
            body = r.text[:200]
            if is_json:
                parsed = r.json()
                body = f"JSON len={len(parsed) if isinstance(parsed, list) else type(parsed)}"
            results.append({"oa_name": oa_name, "status": r.status_code,
                            "is_json": is_json, "body": body})
        except Exception as e:
            results.append({"oa_name": oa_name, "error": str(e)})

    return jsonify({"date_str": date_str, "roc_slash": roc_slash, "results": results})


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
