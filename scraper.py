"""
SwapHunter Data Pipeline v9 — HF Markets column mapping confirmed
col[1] = symbol, col[5] = swap_short, col[6] = swap_long
"""

import json
import time
import urllib.request
from datetime import datetime, timezone

SYMBOL_OIDS = {
    "EURUSD": 116025, "GBPUSD": 116026, "USDJPY": 116027,
    "GBPJPY": 116029, "USDCAD": 116030, "EURAUD": 116031,
    "EURJPY": 116032, "AUDCAD": 116033, "AUDJPY": 116034,
    "AUDNZD": 116036, "AUDUSD": 116037, "CADJPY": 116038,
    "EURCAD": 116039, "EURCHF": 116040, "EURGBP": 116042,
    "EURNZD": 116044, "GBPCAD": 116048, "GBPCHF": 116049,
    "NZDCAD": 116050, "NZDJPY": 116051, "NZDUSD": 116052,
    "USDCHF": 116053, "CHFJPY": 116068, "AUDCHF": 116069,
    "GBPNZD": 116070, "NZDCHF": 116071, "SILVER": 116072,
    "GOLD":   116073, "CADCHF": 116082, "GBPAUD": 116083,
}
OUR_SYMBOLS = list(SYMBOL_OIDS.keys())
OUR_SYMBOLS_SET = {
    "EURUSD","GBPUSD","USDJPY","GBPJPY","USDCAD","EURAUD","EURJPY",
    "AUDCAD","AUDJPY","AUDNZD","AUDUSD","CADJPY","EURCAD","EURCHF",
    "EURGBP","EURNZD","GBPCAD","GBPCHF","NZDCAD","NZDJPY","NZDUSD",
    "USDCHF","CHFJPY","AUDCHF","GBPNZD","NZDCHF","SILVER","GOLD",
    "CADCHF","GBPAUD","UKOIL","USOIL","NATGAS",
}

# ── EXNESS ──
METAL_MAP = {"GOLD": "XAUUSDm", "SILVER": "XAGUSDm"}
GRAPHQL_QUERY = (
    "query getTradingInstruments($account_type: String!, $instruments: [String]) {\n"
    "  tradingInstruments: allExnessAccountTypeInstruments(\n"
    "    sort: {fields: \"instrument\"}\n"
    "    account_type: $account_type\n"
    "    filter: {can_trade: true, instrument: {in: $instruments}}\n"
    "  ) {\n"
    "    instrument swap_long swap_short commission_per_lot median_spread __typename\n"
    "  }\n"
    "}"
)

def run_exness():
    print("── Exness (std only) ──")
    instruments = [METAL_MAP.get(s, s + "m") for s in OUR_SYMBOLS]
    reverse = {METAL_MAP.get(s, s + "m"): s for s in OUR_SYMBOLS}
    payload = json.dumps({
        "operationName": "getTradingInstruments",
        "query": GRAPHQL_QUERY,
        "variables": {"account_type": "mt5_mini_real_vc", "instruments": instruments}
    }).encode()
    output = {}
    try:
        req = urllib.request.Request(
            "https://www.exness.com/pwapi/", data=payload,
            headers={"Content-Type":"application/json","User-Agent":"Mozilla/5.0",
                     "Origin":"https://www.exness.com","Referer":"https://www.exness.com/trading/swap-rates/"},
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode())
        for item in (data.get("data",{}).get("tradingInstruments") or []):
            canon = reverse.get(item["instrument"])
            if canon:
                output.setdefault(canon, {})["exness-std"] = {"long": item["swap_long"], "short": item["swap_short"]}
        print(f"  Got {len(output)} symbols")
    except Exception as e:
        print(f"  Error: {e}")
    return output

# ── CASHBACKFOREX ──
CBF_BASE = "https://spreads-api.cashbackforex.com/api/swapratesforbroker"
CBF_BROKERS = {
    3133: {"key": "icmarkets", "groups": ["Forex Majors", "Forex Minors", "Metals", "Energies"]},
    1149: {"key": "vantage",   "groups": ["Forex", "Gold", "Silver", "Oil", "Commodities"]},
    970:  {"key": "blackbull", "groups": ["Forex Majors", "Forex", "Energies", "Commodities"]},
}
CBF_SYMBOL_MAP = {
    "XAUUSD":"GOLD","XAGUSD":"SILVER","XBRUSD":"UKOIL","XTIUSD":"USOIL","XNGUSD":"NATGAS",
    "UKOUSD":"UKOIL","USOUSD":"USOIL","NG-C":"NATGAS","BRENT":"UKOIL","WTI":"USOIL",
    "UKOUSDft":None,"CL-OIL":None,"Rolltest":None,"GCM25":None,
}

def fetch_cbf_group(cbf_id, group):
    url = f"{CBF_BASE}/{cbf_id}?currentPage=1&countPerPage=100&search=&group={group.replace(' ','%20')}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0","Accept":"application/json","Referer":"https://fxverify.com/"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode()).get("swapRates",{}).get("swapRates",[])
    except Exception as e:
        print(f"    CBF error [{cbf_id}] {group}: {e}")
        return []

def run_cbf():
    print("── CashbackForex ──")
    output = {}
    for cbf_id, config in CBF_BROKERS.items():
        broker_key = config["key"]
        print(f"  {broker_key}")
        broker_data = {}
        for group in config["groups"]:
            for item in fetch_cbf_group(cbf_id, group):
                if item.get("swapType") != "InPoints": continue
                raw = item.get("name","")
                canon = CBF_SYMBOL_MAP.get(raw, raw)
                if canon is None or canon not in OUR_SYMBOLS_SET or canon in broker_data: continue
                lv, sv = item.get("swapLong"), item.get("swapShort")
                broker_data[canon] = {
                    "long":  round(float(lv),4) if lv is not None else None,
                    "short": round(float(sv),4) if sv is not None else None,
                }
            time.sleep(0.8)
        print(f"    Got {len(broker_data)} symbols")
        for sym, rates in broker_data.items():
            output.setdefault(sym,{})[broker_key] = rates
    return output

# ── HF MARKETS — confirmed column mapping: sym=1, short=5, long=6 ──
HF_URL = "https://hfeu.com/en/trading-instruments/forex"
HF_SYMBOL_MAP = {"XAUUSD": "GOLD", "XAGUSD": "SILVER"}
HF_CONTAINERS = {
    "premium-table-container": "hf-premium",
    "pro-table-container":     "hf-pro",
    "zero-table-container":    "hf-zero",
}

def parse_rate(val):
    if not val or str(val).strip() in ("-","—","N/A",""): return None
    try: return round(float(str(val).strip().replace(",",".")), 4)
    except: return None

def run_hfmarkets():
    print("── HF Markets ──")
    results = {}
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-setuid-sandbox","--disable-dev-shm-usage"])
            page = browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
            page.goto(HF_URL, wait_until="domcontentloaded", timeout=60000)
            time.sleep(5)
            # Remove cookie overlay
            page.evaluate("""
                ['cookiescript_injected_wrapper','cookiescript_injected'].forEach(id => {
                    const el = document.getElementById(id); if (el) el.remove();
                });
            """)
            time.sleep(1)

            # Extract using confirmed column indices: sym=1, short=5, long=6
            table_data = page.evaluate("""
                (containers) => {
                    const result = {};
                    for (const [containerId, brokerKey] of Object.entries(containers)) {
                        const container = document.getElementById(containerId);
                        if (!container) { result[brokerKey] = []; continue; }
                        const rows = container.querySelectorAll('table tbody tr');
                        const rowData = [];
                        rows.forEach(row => {
                            const cells = row.querySelectorAll('td');
                            if (cells.length >= 7) {
                                rowData.push([
                                    cells[1].innerText.trim(),  // symbol
                                    cells[5].innerText.trim(),  // swap short
                                    cells[6].innerText.trim()   // swap long
                                ]);
                            }
                        });
                        result[brokerKey] = rowData;
                    }
                    return result;
                }
            """, HF_CONTAINERS)

            for broker_key, rows in table_data.items():
                matched = 0
                for row in rows:
                    raw_sym = row[0].upper()
                    sym = HF_SYMBOL_MAP.get(raw_sym, raw_sym)
                    if sym not in OUR_SYMBOLS_SET: continue
                    short = parse_rate(row[1])
                    long  = parse_rate(row[2])
                    results.setdefault(sym, {})[broker_key] = {"long": long, "short": short}
                    matched += 1
                print(f"  {broker_key}: {len(rows)} rows → {matched} matched")

            browser.close()
    except Exception as e:
        print(f"  Exception: {e}")

    brokers = set(b for s in results.values() for b in s.keys())
    print(f"  Done. {len(results)} symbols, brokers: {brokers}")
    return results

# ── MYFXBOOK ──
MYFXBOOK_BROKER_MAP = {
    "Pepperstone":"pepperstone","Tickmill":"tickmill",
    "XMTrading":"xm","XM":"xm","FP Markets":"fpmarkets",
    "Deriv":"deriv","Deriv.com":"deriv",
    "Fusion Markets":"fusionmarkets",
    "Eightcap":"eightcap",
}

def fetch_myfxbook(oid, long_or_short):
    url = f"https://www.myfxbook.com/get-swap-chart-by-symbol.json?symbolInfoOid={oid}&swapShortLong={long_or_short}&rand=0.5"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept":"application/json, text/javascript, */*; q=0.01",
            "Referer":"https://www.myfxbook.com/forex-broker-swaps/pepperstone/208",
            "X-Requested-With":"XMLHttpRequest",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        brokers = data.get("categories",[])
        values  = data.get("series",[{}])[0].get("data",[])
        result = {}
        for name, val in zip(brokers, values):
            key = MYFXBOOK_BROKER_MAP.get(name)
            if key: result[key] = round(float(val), 4)
        return result
    except Exception as e:
        print(f"    Myfxbook error OID {oid}: {e}")
        return {}

def run_myfxbook():
    print("── Myfxbook ──")
    output = {}
    blocked = 0
    for symbol, oid in SYMBOL_OIDS.items():
        print(f"  {symbol}...")
        ld = fetch_myfxbook(oid, 0)
        if not ld: blocked += 1
        time.sleep(1.2)
        sd = fetch_myfxbook(oid, 1)
        time.sleep(1.2)
        output[symbol] = {}
        for broker in set(ld) | set(sd):
            output[symbol][broker] = {"long": ld.get(broker), "short": sd.get(broker)}
        if blocked >= 3 and len(output) == 3:
            print("  Blocked. Skipping.")
            break
    print(f"  Done. Brokers: {set(b for s in output.values() for b in s)}")
    return output

# ── MAIN ──
def run():
    print(f"\nSwapHunter scraper — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")
    exn_data = run_exness()
    cbf_data = run_cbf()
    hfm_data = run_hfmarkets()
    mfx_data = run_myfxbook()

    all_syms = set(list(exn_data)+list(cbf_data)+list(hfm_data)+list(mfx_data))
    merged = {}
    for sym in all_syms:
        merged[sym] = {}
        merged[sym].update(exn_data.get(sym,{}))
        merged[sym].update(cbf_data.get(sym,{}))
        merged[sym].update(hfm_data.get(sym,{}))
        merged[sym].update(mfx_data.get(sym,{}))

    brokers = set(b for s in merged.values() for b in s)
    with open("swaps.json","w") as f:
        json.dump({
            "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "sources": sorted(brokers),
            "swaps": merged
        }, f, indent=2)

    total = sum(len(v) for v in merged.values())
    print(f"\n✓ Done — {len(merged)} symbols, {len(brokers)} brokers, {total} entries")
    print(f"  Brokers: {sorted(brokers)}")

if __name__ == "__main__":
    run()
