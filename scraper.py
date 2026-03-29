"""
SwapHunter Data Pipeline v6
"""

import json
import time
import urllib.request
from datetime import datetime, timezone

# ─────────────────────────────────────────
# SHARED SYMBOL CONFIG
# ─────────────────────────────────────────
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

# ─────────────────────────────────────────
# EXNESS — GraphQL API (std only until auth solved)
# ─────────────────────────────────────────
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

def fetch_exness_single(account_type):
    instruments = [METAL_MAP.get(s, s + "m") for s in OUR_SYMBOLS]
    reverse = {METAL_MAP.get(s, s + "m"): s for s in OUR_SYMBOLS}
    payload = json.dumps({
        "operationName": "getTradingInstruments",
        "query": GRAPHQL_QUERY,
        "variables": {"account_type": account_type, "instruments": instruments}
    }).encode()
    req = urllib.request.Request(
        "https://www.exness.com/pwapi/",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Origin": "https://www.exness.com",
            "Referer": "https://www.exness.com/trading/swap-rates/",
        },
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        data = json.loads(resp.read().decode())
    items = data.get("data", {}).get("tradingInstruments") or []
    result = {}
    for item in items:
        canon = reverse.get(item["instrument"])
        if canon:
            result[canon] = {"long": item["swap_long"], "short": item["swap_short"]}
    return result

EXNESS_ACCOUNTS = {
    "exness-std": "mt5_mini_real_vc",
}

def run_exness():
    print("── Exness (std only — pro/zero/raw require auth investigation) ──")
    output = {}
    for account_key, account_type in EXNESS_ACCOUNTS.items():
        print(f"  Fetching {account_key} ({account_type})...")
        try:
            result = fetch_exness_single(account_type)
            print(f"    Got {len(result)} symbols")
            for symbol, rates in result.items():
                output.setdefault(symbol, {})[account_key] = rates
        except Exception as e:
            print(f"    Error: {e}")
        time.sleep(2)
    brokers_found = set(b for sym in output.values() for b in sym.keys())
    print(f"  Done. Brokers found: {brokers_found}")
    return output

# ─────────────────────────────────────────
# CASHBACKFOREX — IC Markets, Vantage, BlackBull
# ─────────────────────────────────────────
CBF_BASE = "https://spreads-api.cashbackforex.com/api/swapratesforbroker"

CBF_BROKERS = {
    3133: {"key": "icmarkets", "groups": ["Forex Majors", "Forex Minors", "Metals", "Energies"]},
    1149: {"key": "vantage",   "groups": ["Forex", "Gold", "Silver", "Oil", "Commodities"]},
    970:  {"key": "blackbull", "groups": ["Forex Majors", "Forex", "Energies", "Commodities"]},
}

CBF_SYMBOL_MAP = {
    "XAUUSD": "GOLD",   "XAGUSD": "SILVER",
    "XBRUSD": "UKOIL",  "XTIUSD": "USOIL",  "XNGUSD": "NATGAS",
    "UKOUSD": "UKOIL",  "USOUSD": "USOIL",  "NG-C": "NATGAS",
    "BRENT":  "UKOIL",  "WTI":    "USOIL",
    "UKOUSDft": None,   "CL-OIL": None,     "Rolltest": None, "GCM25": None,
}

def fetch_cbf_group(cbf_id, group):
    url = f"{CBF_BASE}/{cbf_id}?currentPage=1&countPerPage=100&search=&group={group.replace(' ', '%20')}"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json", "Referer": "https://fxverify.com/",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        return data.get("swapRates", {}).get("swapRates", [])
    except Exception as e:
        print(f"    CBF error [{cbf_id}] group={group}: {e}")
        return []

def run_cbf():
    print("── CashbackForex (IC Markets, Vantage, BlackBull) ──")
    output = {}
    for cbf_id, config in CBF_BROKERS.items():
        broker_key = config["key"]
        print(f"  {broker_key} (CBF ID: {cbf_id})")
        broker_data = {}
        for group in config["groups"]:
            for item in fetch_cbf_group(cbf_id, group):
                if item.get("swapType") != "InPoints":
                    continue
                raw = item.get("name", "")
                canon = CBF_SYMBOL_MAP.get(raw, raw)
                if canon is None or canon not in OUR_SYMBOLS_SET or canon in broker_data:
                    continue
                lv, sv = item.get("swapLong"), item.get("swapShort")
                broker_data[canon] = {
                    "long":  round(float(lv), 4) if lv is not None else None,
                    "short": round(float(sv), 4) if sv is not None else None,
                }
            time.sleep(0.8)
        print(f"    Got {len(broker_data)} symbols")
        for symbol, rates in broker_data.items():
            output.setdefault(symbol, {})[broker_key] = rates
    print(f"  Done. {len(output)} symbols")
    return output

# ─────────────────────────────────────────
# HF MARKETS — extract all tables via JS, no clicking needed
# ─────────────────────────────────────────
HF_URL = "https://hfeu.com/en/trading-instruments/forex"

# Map container IDs to broker keys (from page source)
HF_CONTAINERS = {
    "premium-table-container": "hf-premium",
    "premium-pro-table-container": "hf-pro",
    "zero-table-container": "hf-zero",
}
HF_SYMBOL_MAP = {"XAUUSD": "GOLD", "XAGUSD": "SILVER"}

def parse_rate(val):
    if not val or str(val).strip() in ("-", "—", "N/A", ""):
        return None
    try:
        return round(float(str(val).strip().replace(",", ".")), 4)
    except Exception:
        return None

def run_hfmarkets():
    print("── HF Markets (Premium, Pro, Zero) ──")
    results = {}
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
            )
            page = browser.new_page(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            print(f"  Loading {HF_URL}")
            page.goto(HF_URL, wait_until="domcontentloaded", timeout=60000)
            time.sleep(5)

            # Remove cookie overlay via JS
            page.evaluate("""
                ['cookiescript_injected_wrapper','cookiescript_injected'].forEach(id => {
                    const el = document.getElementById(id);
                    if (el) el.remove();
                });
            """)
            time.sleep(1)

            # Extract ALL table data via JS — no clicking needed
            # All tab containers exist in DOM, just hidden via CSS
            table_data = page.evaluate("""
                () => {
                    const containers = {
                        'premium-table-container': 'hf-premium',
                        'premium-pro-table-container': 'hf-pro',
                        'zero-table-container': 'hf-zero'
                    };
                    const result = {};
                    for (const [containerId, brokerKey] of Object.entries(containers)) {
                        const container = document.getElementById(containerId);
                        if (!container) { result[brokerKey] = {found: false}; continue; }
                        const rows = container.querySelectorAll('table tbody tr');
                        const rowData = [];
                        rows.forEach(row => {
                            const cells = row.querySelectorAll('td');
                            if (cells.length >= 6) {
                                rowData.push({
                                    symbol: cells[0].innerText.trim(),
                                    col4: cells[4].innerText.trim(),
                                    col5: cells[5].innerText.trim()
                                });
                            }
                        });
                        result[brokerKey] = {found: true, rows: rowData.length, data: rowData};
                    }
                    return result;
                }
            """)

            print(f"  JS extraction result: {json.dumps({k: v.get('rows', 'not found') for k, v in table_data.items()})}")

            for broker_key, container_data in table_data.items():
                if not container_data.get("found"):
                    print(f"  Container not found for {broker_key}")
                    continue
                for row in container_data.get("data", []):
                    raw_sym = row["symbol"].upper()
                    sym = HF_SYMBOL_MAP.get(raw_sym, raw_sym)
                    if sym not in OUR_SYMBOLS_SET:
                        continue
                    short = parse_rate(row["col4"])
                    long  = parse_rate(row["col5"])
                    results.setdefault(sym, {})[broker_key] = {"long": long, "short": short}

            browser.close()
    except Exception as e:
        print(f"  HF Markets exception: {e}")

    brokers = set(b for s in results.values() for b in s.keys())
    print(f"  Done. {len(results)} symbols, brokers: {brokers}")
    return results

# ─────────────────────────────────────────
# MYFXBOOK
# ─────────────────────────────────────────
MYFXBOOK_BROKER_MAP = {
    "Pepperstone": "pepperstone", "Tickmill": "tickmill",
    "XMTrading": "xm", "XM": "xm",
    "FP Markets": "fpmarkets",
    "Deriv": "deriv", "Deriv.com": "deriv",
}

def fetch_myfxbook(oid, long_or_short):
    url = (f"https://www.myfxbook.com/get-swap-chart-by-symbol.json"
           f"?symbolInfoOid={oid}&swapShortLong={long_or_short}&rand=0.5")
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Referer": "https://www.myfxbook.com/forex-broker-swaps/pepperstone/208",
            "X-Requested-With": "XMLHttpRequest",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        brokers = data.get("categories", [])
        values  = data.get("series", [{}])[0].get("data", [])
        result = {}
        for name, val in zip(brokers, values):
            key = MYFXBOOK_BROKER_MAP.get(name)
            if key:
                result[key] = round(float(val), 4)
        return result
    except Exception as e:
        print(f"    Myfxbook error OID {oid}: {e}")
        return {}

def run_myfxbook():
    print("── Myfxbook (Pepperstone, Tickmill, XM, FP Markets, Deriv) ──")
    output = {}
    blocked_count = 0
    for symbol, oid in SYMBOL_OIDS.items():
        print(f"  {symbol}...")
        long_data  = fetch_myfxbook(oid, 0)
        if not long_data: blocked_count += 1
        time.sleep(1.2)
        short_data = fetch_myfxbook(oid, 1)
        time.sleep(1.2)
        output[symbol] = {}
        for broker in set(long_data) | set(short_data):
            output[symbol][broker] = {"long": long_data.get(broker), "short": short_data.get(broker)}
        if blocked_count >= 3 and len(output) == 3:
            print("  Myfxbook fully blocked. Skipping.")
            break
    brokers_found = set(b for sym in output.values() for b in sym.keys())
    print(f"  Done. Brokers found: {brokers_found}")
    return output

# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def run():
    print(f"\nSwapHunter scraper — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")

    exn_data = run_exness()
    cbf_data = run_cbf()
    hfm_data = run_hfmarkets()
    mfx_data = run_myfxbook()

    all_symbols = set(list(exn_data.keys()) + list(cbf_data.keys()) + list(hfm_data.keys()) + list(mfx_data.keys()))
    merged = {}
    for sym in all_symbols:
        merged[sym] = {}
        merged[sym].update(exn_data.get(sym, {}))
        merged[sym].update(cbf_data.get(sym, {}))
        merged[sym].update(hfm_data.get(sym, {}))
        merged[sym].update(mfx_data.get(sym, {}))

    brokers = set(b for sym in merged.values() for b in sym.keys())
    output = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "sources": sorted(brokers),
        "swaps": merged
    }
    with open("swaps.json", "w") as f:
        json.dump(output, f, indent=2)

    total = sum(len(v) for v in merged.values())
    print(f"\n✓ Done — {len(merged)} symbols, {len(brokers)} brokers, {total} entries")
    print(f"  Brokers: {sorted(brokers)}")

if __name__ == "__main__":
    run()
