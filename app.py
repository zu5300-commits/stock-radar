
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
