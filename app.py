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

# 跨裝置同步資料（依 token 隔離）
_sync_store = {}
_sync_lock  = threading.Lock()

# ── Cloudflare R2 永久存檔（同步資料 durable backing）──────────────────────────
# 沒設環境變數時 _r2=None，自動退回「純記憶體」模式，行為與舊版完全相同（絕不破壞現狀）。
import json as _json
R2_ENDPOINT = os.environ.get("R2_ENDPOINT", "").strip()
R2_BUCKET   = os.environ.get("R2_BUCKET", "radar-sync").strip()
R2_ACCESS   = os.environ.get("R2_ACCESS_KEY", "").strip()
R2_SECRET   = os.environ.get("R2_SECRET_KEY", "").strip()

_r2 = None
if R2_ENDPOINT and R2_ACCESS and R2_SECRET:
    try:
        import boto3
        from botocore.config import Config as _BotoConfig
        _r2 = boto3.client(
            "s3", endpoint_url=R2_ENDPOINT,
            aws_access_key_id=R2_ACCESS, aws_secret_access_key=R2_SECRET,
            region_name="auto",
            config=_BotoConfig(signature_version="s3v4", retries={"max_attempts": 2}),
        )
        print(f"[R2] enabled, bucket={R2_BUCKET}")
    except Exception as e:
        print(f"[R2] init failed, fallback to memory-only: {e}")
        _r2 = None
else:
    print("[R2] not configured (env vars missing) — memory-only mode")


def _r2_key(token):
    return f"sync/{token}.json"


def _r2_get(token):
    """從 R2 讀回某 token 的 entry；沒有或失敗回 None（首次 NoSuchKey 也走這）"""
    if not _r2:
        return None
    try:
        obj = _r2.get_object(Bucket=R2_BUCKET, Key=_r2_key(token))
        return _json.loads(obj["Body"].read())
    except Exception:
        return None


def _r2_put(token, entry):
    """把 entry 寫進 R2（write-through）"""
    if not _r2:
        return
    try:
        _r2.put_object(Bucket=R2_BUCKET, Key=_r2_key(token),
                       Body=_json.dumps(entry).encode("utf-8"),
                       ContentType="application/json")
    except Exception as e:
        print(f"[R2] put failed token={token}: {e}")


def _r2_day_key(date_str):
    return f"inst/{date_str}.json"


def _r2_get_day(date_str):
    """從 R2 讀某交易日的完整法人資料 {code:[f_net,t_net]}；沒有或失敗回 None"""
    if not _r2:
        return None
    try:
        obj = _r2.get_object(Bucket=R2_BUCKET, Key=_r2_day_key(date_str))
        return _json.loads(obj["Body"].read())
    except Exception:
        return None


def _r2_put_day(date_str, day_data):
    """把某交易日完整法人資料寫進 R2（永久存檔，之後不必重抓）"""
    if not _r2:
        return
    try:
        _r2.put_object(Bucket=R2_BUCKET, Key=_r2_day_key(date_str),
                       Body=_json.dumps(day_data).encode("utf-8"),
                       ContentType="application/json")
    except Exception as e:
        print(f"[R2] put day failed {date_str}: {e}")


def _ensure_loaded(token):
    """記憶體沒有此 token 時，從 R2 載回（主機冷啟動/重新部署後復原永久資料）"""
    with _sync_lock:
        if token in _sync_store:
            return
    r2entry = _r2_get(token)
    if r2entry is not None:
        with _sync_lock:
            _sync_store.setdefault(token, r2entry)
        print(f"[R2] restored token={token} from R2")


def _persist(token):
    """把記憶體中此 token 的 entry 寫回 R2"""
    with _sync_lock:
        entry = _sync_store.get(token)
    if entry is not None:
        _r2_put(token, entry)


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
    """安全轉整數，支援浮點字串 '4829144000.0' → 4829144000"""
    try:
        return int(float(str(val).replace(",", "")))
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


# 指標上市股（台積電／鴻海／聯發科）— 幾乎每個交易日都在 T86 名單裡，
# 用來判斷「上市(TWSE)三大法人」那批資料是否成功抓到。
_SANITY_LISTED = ("2330", "2317", "2454")

# 指標上櫃股（環球晶／元太／穩懋）— 用來判斷「上櫃(TPEX)」那批是否抓到，
# 確保存進 R2 永久檔的交易日「上市+上櫃」兩源都齊，不會永久缺一邊。
_SANITY_OTC = ("6488", "8069", "3105")


def _inst_is_healthy(result):
    """法人連買快取是否健康：必須含足夠指標上市股；否則代表 TWSE(上市)那批抓失敗，
    只剩上櫃(TPEX)→ 所有上市股連買天數都會被當 0，不可拿來覆蓋舊的好快取。"""
    if not result:
        return False
    return sum(1 for c in _SANITY_LISTED if c in result) >= 2


def _fetch_inst_day_full(date_str):
    """抓某交易日完整法人資料(上市 T86 + 上櫃 TPEX 合併)。
    回傳 (date_str, merged or None)。只有「上市+上櫃」兩個指標股都抓到，
    才算這天完整、可以存進 R2 永久檔；任一源缺(抓失敗或非交易日)就回 None。"""
    _, twse = _fetch_t86_day(date_str)
    _, tpex = _fetch_tpex_inst_day(date_str)
    merged = {}
    merged.update(tpex or {})
    merged.update(twse or {})            # 同代號以上市為準(理論上不重疊)
    has_twse = any(c in merged for c in _SANITY_LISTED)
    has_tpex = any(c in merged for c in _SANITY_OTC)
    if not (has_twse and has_tpex):
        return date_str, None            # 任一源缺 → 不完整，不存 R2
    return date_str, merged


def _compute_inst(n_days):
    """回傳近 n_days 個工作日的三大法人連買天數 {code: {foreign_days, trust_days}}。
    資料來源：每個交易日的完整法人資料持久化在 R2(inst/<date>.json)，
    先讀 R2，只有 R2 沒有的日才即時去 TWSE/TPEX 抓、抓到再存回 R2。
    → 歷史只抓一次、永久累積，連買天數鏈不會因偶發漏抓而斷裂。"""
    dates    = recent_weekdays(n_days)
    all_days = {}
    _lock    = threading.Lock()

    # 1) 先從 R2 載入已存的完整交易日
    need_fetch = []
    for d in dates:
        cached = _r2_get_day(d)
        if cached:
            all_days[d] = cached
        else:
            need_fetch.append(d)

    # 2) 只抓 R2 缺的日(workers=4 降並發避免 TWSE 限速)，完整才存回 R2
    def _work(d):
        try:
            _, merged = _fetch_inst_day_full(d)
        except Exception:
            merged = None
        if merged:
            _r2_put_day(d, merged)
            with _lock:
                all_days[d] = merged

    if need_fetch:
        with ThreadPoolExecutor(max_workers=4) as executor:
            list(executor.map(_work, need_fetch))

    # 只保留「當天有抓到上市(TWSE)指標股」的交易日參與連續計算。
    # 否則某天上市資料漏抓時，下面的 `code not in day → continue` 會把那天
    # 當成不存在而跳過，導致連買天數虛報（跟看盤軟體對不上）。
    good_dates = [d for d in all_days
                  if any(c in all_days[d] for c in _SANITY_LISTED)]
    if not good_dates:                       # 完全沒上市資料 → 退而用全部（至少上櫃能算）
        good_dates = list(all_days.keys())
    sorted_dates = sorted(good_dates, reverse=True)
    all_codes    = set(c for d in good_dates for c in all_days[d])
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
            if _inst_is_healthy(result):
                set_cache("inst_all", result)
                print(f"[BG] 20-day inst fetch done, {len(result)} stocks")
            else:
                print(f"[BG] 20-day inst INCOMPLETE ({len(result)} stocks, TWSE missing) — keep old cache, will retry")
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
    - TaiwanStockBalanceSheet (5年) → 5年平均負債比(Liabilities_per) + 最新股本(OrdinaryShare)
    - TaiwanStockFinancialStatements (近5季) → 近4季OperatingExpenses合計
    回傳 {code: {debt_ratio, rd_expense, shares}}
    """
    now      = datetime.now()
    start_5y = (now - timedelta(days=365 * 5 + 60)).strftime("%Y-%m-%d")
    start_5q = (now - timedelta(days=420)).strftime("%Y-%m-%d")

    result = {}
    # FinMind TaiwanStockFinancialStatements 有 OperatingExpenses（營業費用）
    # = 研發 + 銷售 + 管理費用，以較低閾值(rr<8)過濾
    RD_TYPES = {
        "OperatingExpenses",
        "ResearchAndDevelopmentExpenses",  # 若未來 FinMind 加入，自動生效
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

                # ── 5年平均負債比 + 最新股本 ──────────────────────────────────
                # Liabilities_per = 負債比(%) 直接可用
                # OrdinaryShare   = 股本(元)，除以10=股數（面值10元/股）
                by_year = {}   # year → {debt_pct, capital}
                for row in bs:
                    try:
                        year = str(row.get("date", ""))[:4]
                        typ  = str(row.get("type", ""))
                        val  = float(str(row.get("value", 0)).replace(",", "") or 0)
                        if typ == "Liabilities_per":
                            by_year.setdefault(year, {})["debt_pct"] = val
                        elif typ in ("OrdinaryShare", "CapitalStock"):
                            by_year.setdefault(year, {}).setdefault("capital", val)
                    except Exception:
                        continue

                debt_pcts = [d["debt_pct"] for d in by_year.values() if "debt_pct" in d]
                debt_ratio = round(sum(debt_pcts) / len(debt_pcts), 1) if debt_pcts else None

                # 取最新年份的股本
                shares = 0
                for yr in sorted(by_year.keys(), reverse=True):
                    cap = by_year[yr].get("capital", 0)
                    if cap > 0:
                        shares = int(cap / 10)   # 面值10元/股
                        break

                # ── 近4季OperatingExpenses合計 ───────────────────────────────
                rd_rows = sorted(
                    [r for r in fs if str(r.get("type", "")) in RD_TYPES],
                    key=lambda r: r.get("date", ""),
                    reverse=True
                )[:4]
                rd_expense = sum(to_int(r.get("value", 0)) for r in rd_rows)

                result[code] = {
                    "debt_ratio": debt_ratio,
                    "rd_expense": rd_expense,
                    "shares":     shares,
                }
                if debt_ratio is not None:
                    print(f"[RD] {code}: debt={debt_ratio}% shares={shares//10000}萬 opex={rd_expense//1e8:.1f}億")
            except Exception as e:
                print(f"[RD] {code}: {e}")

    valid = sum(1 for v in result.values() if v.get("debt_ratio") is not None)
    print(f"[RD] _compute_rd_data done: {len(result)} stocks, {valid} with debt_ratio")
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
    # 20 天完整資料優先（法人一天才更新一次，快取放長 6 小時，
    # 減少頻繁重抓 20 天而觸發 TWSE 限速）
    cached = get_cache("inst_all", ttl=21600)
    if cached:
        return cached

    # 7 天快速資料（同步，不超時）
    fast = get_cache("inst_fast", ttl=21600)
    if not fast:
        fast = _compute_inst(7)
        if _inst_is_healthy(fast):           # 殘缺(缺上市)就不存，下次再抓，避免毒化好資料
            set_cache("inst_fast", fast)

    # 背景補齊 20 天
    _start_bg_inst_fetch()
    return fast


# ── API 路由 ──────────────────────────────────────────────────────────────────

def compute_quote_data():
    """抓取並組合行情資料，回傳 (result_dict, latest_date)。供 /quote 與 /cron/snapshot 共用。"""
    top100, price_data, latest_date, names = get_top100_prices()
    if not top100:
        return None, None

    inst_all = get_all_inst_data()

    # 基本：成交值前 100
    codes = list(top100)

    # 補充：法人連續買超 ≥20 天，但不在前 100 的股票
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

        price    = p["price"]
        shares   = rd.get("shares", 0)
        rd_exp   = rd.get("rd_expense", 0)
        mkt_cap  = price * shares
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
            "div_yield":    bwibbu.get(code),
            "debt_ratio":   rd.get("debt_ratio"),
            "rd_ratio":     rd_ratio,
            "error":        False,
        }
    return result, latest_date


@app.route("/quote")
def quote():
    try:
        result, latest_date = compute_quote_data()
        if result is None:
            return jsonify({
                "ok": False,
                "error": "TWSE/TPEX 目前無資料，請查看 /debug-twse 了解原因"
            })
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

    # ── 即時測試 FinMind 是否可從 Render 連線（只抓1筆）──────────────────────────
    fm_test = {}
    try:
        test_rows = _fm_get("TaiwanStockBalanceSheet", "4958", "2024-01-01")
        assets = [r for r in test_rows if r.get("type") in ("TotalAssets", "資產總額")]
        equity = [r for r in test_rows if r.get("type") in ("Equity", "權益總額")]
        fm_test = {
            "ok":      len(test_rows) > 0,
            "rows":    len(test_rows),
            "assets":  assets[:1],
            "equity":  equity[:1],
        }
    except Exception as e:
        fm_test = {"ok": False, "error": str(e)}

    return jsonify({
        "rd_cache_exists":     rd_data is not None,
        "rd_cache_size":       len(rd_data) if rd_data else 0,
        "rd_bg_running":       _rd_bg_running,
        "bwibbu_cache_exists": bwibbu is not None,
        "bwibbu_cache_size":   len(bwibbu) if bwibbu else 0,
        "fm_test":             fm_test,
        "codes":               out,
    })


@app.route("/clear-rd-cache")
def clear_rd_cache():
    """清除 rd_data 快取，讓下次 /quote 重新觸發背景抓取"""
    with _cache_lock:
        removed = "rd_data" in _cache
        _cache.pop("rd_data", None)
    with _rd_bg_lock:
        global _rd_bg_running
        _rd_bg_running = False
    return jsonify({"ok": True, "cleared": removed, "msg": "請重新點『抓取行情』觸發背景更新"})


@app.route("/admin/refresh-inst")
def admin_refresh_inst():
    """維運用：清掉法人連買快取並同步重算（會把 R2 缺的交易日補抓進去）。
    回補階段反覆呼叫即可逐步把 R2 補齊；資料源出包時也可手動重整。
    安全性：只讀公開行情、自我收斂（每個缺日抓一次就存進 R2、之後直接讀 R2），
    不碰任何同步清單，故不需密鑰。用法：/admin/refresh-inst"""
    with _cache_lock:
        _cache.pop("inst_all", None)
        _cache.pop("inst_fast", None)
    result = _compute_inst(20)                    # 同步重算：R2 有的日直接用，缺的日抓+存回 R2
    healthy = _inst_is_healthy(result)
    if healthy:
        set_cache("inst_all", result)
    sample = {c: result.get(c) for c in ("2379", "8996", "2887", "0050", "2330")}
    return jsonify({"ok": True, "healthy": healthy, "size": len(result), "sample": sample})


@app.route("/test-yahoo")
def test_yahoo():
    """從 Render server 測試 Yahoo Finance 財報資料（含 R&D）"""
    results = {}
    for symbol in ("4958.TW", "3035.TW", "3481.TW"):
        url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
        try:
            r = req.get(url,
                params={"modules": "incomeStatementHistory"},
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
                timeout=15)
            data = r.json()
            stmts = (data.get("quoteSummary", {})
                        .get("result", [{}])[0]
                        .get("incomeStatementHistory", {})
                        .get("incomeStatementHistory", []))
            if stmts:
                keys = list(stmts[0].keys())
                rd_vals = [(s.get("endDate", {}).get("fmt", "?"),
                            s.get("researchDevelopment", {}).get("raw"))
                           for s in stmts]
                results[symbol] = {"keys": keys, "rd_values": rd_vals, "count": len(stmts)}
            else:
                results[symbol] = {"error": "no statements", "raw": str(data)[:300]}
        except Exception as e:
            results[symbol] = {"error": str(e)}
    return jsonify(results)


@app.route("/debug-fm-datasets")
def debug_fm_datasets():
    """查詢 FinMind 所有合法 Taiwan dataset 名稱 + 測試 R&D 相關 dataset"""
    import re as _re
    # 1. 用不存在的名稱觸發 validation error，取得完整清單
    try:
        r = req.get(FM_BASE, params={
            "dataset": "INVALID_DATASET_XYZ", "data_id": "4958",
            "start_date": "2024-01-01", "token": FM_TOKEN
        }, timeout=15)
        raw = r.text
        # 從 validation error 訊息提取所有 dataset 名稱
        datasets = sorted(set(_re.findall(r"'(Taiwan\w+)'", raw)))
    except Exception as e:
        raw = str(e)
        datasets = []

    # 2. 試幾個可能含 R&D 的 dataset
    rd_tests = {}
    candidates = [
        "TaiwanStockFinancialStatements",
        "TaiwanStockCashFlowsStatement",
        "TaiwanStockCapitalReductionReferencePrice",
    ]
    for ds in candidates:
        try:
            r2 = req.get(FM_BASE, params={
                "dataset": ds, "data_id": "3035",
                "start_date": "2024-10-01", "token": FM_TOKEN
            }, timeout=15)
            d2 = r2.json()
            rows = d2.get("data", [])
            types = sorted(set(r["type"] for r in rows if "type" in r))
            rd_types = [t for t in types if any(k in t.lower() for k in
                        ["research","develop","rd","研發","研究"])]
            rd_tests[ds] = {"rows": len(rows), "all_types": types, "rd_types": rd_types}
        except Exception as e:
            rd_tests[ds] = {"error": str(e)}

    return jsonify({
        "all_taiwan_datasets": datasets,
        "rd_type_search": rd_tests,
        "raw_error_preview": raw[:500],
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


@app.route("/sync/save", methods=["POST"])
def sync_save():
    """跨裝置同步：儲存 watchlist（依 token 隔離）"""
    try:
        body  = request.get_json(force=True) or {}
        token = str(body.get("token", "")).strip()[:64]
        data  = body.get("data")
        ts    = int(body.get("ts", 0))
        if not token or data is None:
            return jsonify({"ok": False, "error": "missing token or data"})
        _ensure_loaded(token)                    # 冷啟動先從 R2 載回，避免舊資料蓋掉永久版
        saved = False
        with _sync_lock:
            cur = _sync_store.get(token, {})
            if ts >= cur.get("ts", 0):           # 只接受較新的資料
                _sync_store[token] = {"data": data, "ts": ts, "cron_ts": cur.get("cron_ts", 0)}
                saved = True
        if saved:
            _persist(token)                      # 寫進 R2 永久存檔
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/sync/load")
def sync_load():
    """跨裝置同步：讀取 watchlist"""
    token = str(request.args.get("token", "")).strip()[:64]
    if not token:
        return jsonify({"ok": False, "error": "missing token"})
    _ensure_loaded(token)                        # 冷啟動從 R2 載回永久資料
    with _sync_lock:
        entry = _sync_store.get(token)
    if entry:
        return jsonify({"ok": True, "data": entry["data"], "ts": entry["ts"]})
    return jsonify({"ok": True, "data": None, "ts": 0})


# ── 歷史回補（買超：外資連續買超 ≥ 20 天） ──────────────────────────────────────
_backfill_store = {}
_backfill_lock  = threading.Lock()


def _build_inst_days(n_days):
    """
    抓 n_days 個交易日的法人買賣超，回傳 {date_str: {code: (f_net, t_net)}}。
    溫和模式：完全循序、每個請求間隔，避免被 TWSE/TPEX 封 IP，也不壓垮免費主機。
    """
    import time as _time
    dates    = recent_weekdays(n_days)
    all_days = {}
    for d in dates:
        try:
            ds, dd = _fetch_t86_day(d)
            if dd:
                all_days.setdefault(ds, {}).update(dd)
        except Exception:
            pass
        _time.sleep(1.2)                      # 節流：上市
        try:
            ds, dd = _fetch_tpex_inst_day(d)
            if dd:
                all_days.setdefault(ds, {}).update(dd)
        except Exception:
            pass
        _time.sleep(1.2)                      # 節流：上櫃
    return {d: v for d, v in all_days.items() if v}


def _consec_foreign_days(all_days, sorted_dates, anchor_idx, code):
    """從 sorted_dates[anchor_idx] 往前數，外資連續買超天數（遇到非買超或無資料即中斷）"""
    cnt = 0
    for i in range(anchor_idx, len(sorted_dates)):
        day = all_days.get(sorted_dates[i], {})
        if code not in day:
            break
        if day[code][0] > 0:
            cnt += 1
        else:
            break
    return cnt


def _run_backfill(token, window_days=20, BUYUP_DAYS=20):
    """重建過去 window_days 個交易日，每檔「外資連續買超 ≥ BUYUP_DAYS」的首次達標日"""
    try:
        total        = window_days + BUYUP_DAYS + 3       # 多抓少量緩衝確保連續天數算得到
        all_days     = _build_inst_days(total)
        sorted_dates = sorted(all_days.keys(), reverse=True)   # 新 → 舊（YYYYMMDD）

        # 取最近交易日的上市股票名稱
        names = {}
        if sorted_dates:
            try:
                rows, _ = fetch_twse_day(sorted_dates[0])
                for row in (rows or []):
                    try:
                        names[str(row[0]).strip()] = str(row[1]).strip()
                    except Exception:
                        pass
            except Exception:
                pass

        first_trigger = {}   # code -> {date, fdays}
        anchors = range(min(window_days, len(sorted_dates)))
        # 由舊到新掃，記下第一次達標的日期
        for idx in sorted(anchors, reverse=True):
            date_str = sorted_dates[idx]
            for code in all_days.get(date_str, {}):
                if code in first_trigger:
                    continue
                fdays = _consec_foreign_days(all_days, sorted_dates, idx, code)
                if fdays >= BUYUP_DAYS:
                    first_trigger[code] = {"date": date_str, "fdays": fdays}

        result = []
        for code, info in first_trigger.items():
            d = info["date"]
            result.append({
                "code":         code,
                "name":         names.get(code, code),
                "strategy":     "buyup",
                "first_date":   f"{d[:4]}-{d[4:6]}-{d[6:8]}",
                "foreign_days": info["fdays"],
            })
        result.sort(key=lambda x: x["first_date"])

        with _backfill_lock:
            _backfill_store[token] = {"status": "done", "result": result}
        print(f"[BACKFILL] {token}: done, {len(result)} stocks")
    except Exception as e:
        print(f"[BACKFILL] {token}: error {e}")
        with _backfill_lock:
            _backfill_store[token] = {"status": "error", "error": str(e), "result": []}


@app.route("/backfill/start", methods=["POST"])
def backfill_start():
    body  = request.get_json(force=True) or {}
    token = str(body.get("token", "")).strip()[:64] or "default"
    days  = max(5, min(int(body.get("days", 20)), 40))
    with _backfill_lock:
        cur = _backfill_store.get(token)
        if cur and cur.get("status") == "running":
            return jsonify({"ok": True, "status": "running"})
        _backfill_store[token] = {"status": "running", "result": []}
    threading.Thread(target=_run_backfill, args=(token, days), daemon=True).start()
    return jsonify({"ok": True, "status": "started"})


@app.route("/backfill/status")
def backfill_status():
    token = str(request.args.get("token", "")).strip()[:64] or "default"
    with _backfill_lock:
        entry = _backfill_store.get(token)
    if not entry:
        return jsonify({"ok": True, "status": "none"})
    return jsonify({"ok": True, "status": entry["status"],
                    "result": entry.get("result", []), "error": entry.get("error")})


# ── 伺服器端每日自動快照（給外部排程器 cron-job.org 呼叫） ──────────────────────
# 台股國定假日（與前端一致，平日休市才需列）
_TW_HOLIDAYS = {
    "2025-01-01","2025-01-27","2025-01-28","2025-01-29","2025-01-30","2025-01-31",
    "2025-02-28","2025-04-03","2025-04-04","2025-05-01","2025-06-02","2025-10-06","2025-10-10",
    "2026-01-01","2026-01-26","2026-01-27","2026-01-28","2026-01-29","2026-01-30",
    "2026-04-03","2026-05-01","2026-06-19","2026-09-25","2026-10-09",
}


def _market_closed(now=None):
    now = now or datetime.now()
    if now.weekday() >= 5:                       # 週六(5)、週日(6)
        return True
    return now.strftime("%Y-%m-%d") in _TW_HOLIDAYS


def _sig_wind(d):
    return d.get("change", 0) > 0 and (d.get("foreign_days") or 0) >= 3 and (d.get("trust_days") or 0) >= 3


def _sig_rd(d):
    dr, dy, rr = d.get("debt_ratio"), d.get("div_yield"), d.get("rd_ratio")
    if dr is None or dy is None or rr is None:
        return False
    return dr < 50 and dy > 1 and rr < 15


def _sig_buyup(d):
    return (d.get("foreign_days") or 0) >= 20


_STRAT_ICON = {"wind": "🌪️", "rd": "🔬", "buyup": "📈"}


@app.route("/cron/snapshot")
def cron_snapshot():
    """
    外部排程器每個交易日呼叫一次：抓行情→把新觸發策略的股票記進該 token 的追蹤名單。
    用法：/cron/snapshot?token=你的同步碼
    為避免重複/被濫用，同一 token 6 小時內只處理一次。
    """
    token = str(request.args.get("token", "")).strip()[:64]
    if not token:
        return jsonify({"ok": False, "error": "missing token"})

    now = datetime.now()
    if _market_closed(now):
        return jsonify({"ok": True, "skipped": "market closed"})

    today   = now.strftime("%Y-%m-%d")
    iso     = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    ts_ms   = int(now.timestamp() * 1000)

    _ensure_loaded(token)                        # 冷啟動從 R2 復原（節流時間+清單才正確）

    # 節流：6 小時內已處理過就跳過
    with _sync_lock:
        entry = _sync_store.get(token, {"data": {"wl": {}, "lastFetch": None}, "ts": 0})
        last_cron = entry.get("cron_ts", 0)
    if ts_ms - last_cron < 6 * 3600 * 1000:
        return jsonify({"ok": True, "skipped": "throttled (<6h)"})

    # 每日選股前先清掉法人快取，強制用「今天盤後最新」的法人資料重算
    # （T86 約 15:00 後才公布；若沿用盤中舊快取會漏掉今天剛符合的股票）。
    # _compute_inst 會先讀 R2 歷史、只補抓今天這一天，故快又正確。
    with _cache_lock:
        _cache.pop("inst_all", None)
        _cache.pop("inst_fast", None)

    try:
        data, _ = compute_quote_data()
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})
    if not data:
        return jsonify({"ok": False, "error": "no market data"})

    with _sync_lock:
        entry = _sync_store.get(token, {"data": {"wl": {}, "lastFetch": None}, "ts": 0})
        wl = entry.get("data", {}).get("wl", {}) or {}
        added = 0
        for code, d in data.items():
            if not d.get("price"):
                continue
            met = []
            if _sig_wind(d):  met.append("wind")
            if _sig_rd(d):    met.append("rd")
            if _sig_buyup(d): met.append("buyup")
            if not met:
                continue
            if code in wl:                                # 歷史紀錄：已存在不重複加
                continue
            label = "📡 " + "+".join(_STRAT_ICON[m] for m in met) + " 觸發入選（自動）"
            wl[code] = {
                "stars": 1, "name": d.get("name", code), "sector": d.get("sector", "—"),
                "windActive": "wind" in met, "windMissCount": 0, "windCycles": 0, "hasBuySignal": False,
                "snapshots": [{
                    "type": "entry", "label": label, "date": today, "iso": iso,
                    "price": d.get("price", 0), "stars": 1, "windCycle": 0, "met": met,
                }],
            }
            added += 1

        entry["data"] = {"wl": wl, "lastFetch": iso}
        entry["ts"] = ts_ms
        entry["cron_ts"] = ts_ms
        _sync_store[token] = entry

    _persist(token)                              # 寫進 R2 永久存檔
    print(f"[CRON] {token}: snapshot done, +{added} new, total {len(wl)}")
    return jsonify({"ok": True, "added": added, "total": len(wl), "date": today})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
