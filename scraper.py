"""
SwapHunter Data Pipeline v3
Sources:
  - Exness GraphQL API       -> exness-std, exness-pro, exness-zero, exness-raw
  - CashbackForex API        -> IC Markets, Vantage, BlackBull
  - HF Markets (Playwright)  -> hf-premium, hf-pro, hf-zero
  - Myfxbook API             -> Pepperstone, Tickmill, XM, FP Markets, Deriv

Runs via GitHub Actions every 2 hours. No server needed.
"""

import json
import time
import urllib.request
import urllib.error
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
# EXNESS — GraphQL API
# ─────────────────────────────────────────
EXNESS_ACCOUNTS = {
    "exness-std":  "mt5_mini_real_vc",
    "exness-pro":  "mt5_pro_real_vc",
    "exness-zero": "mt5_zero_spread_real_vc",
    "exness-raw":  "mt5_raw_spread_real_vc",
}

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

def fetch_exness(account_key, account_type, retries=3):
    instruments = [METAL_MAP.get(s, s + "m") for s in OUR_SYMBOLS]
    reverse = {METAL_MAP.get(s, s + "m"): s for s in OUR_SYMBOLS}

    payload = json.dumps({
        "operationName": "getTradingInstruments",
        "query": GRAPHQL_QUERY,
        "variables": {"account_type": account_type, "instruments": instruments}
    }).encode()

    for attempt in range(retries):
        try:
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
                raw = resp.read().decode()
            data = json.loads(raw)
            items = data.get("data", {}).get("tradingInstruments", [])
            result = {}
            for item in items:
                canon = reverse.get(item["instrument"])
                if canon:
                    result[canon] = {
                        "long":  item["swap_long"],
                        "short": item["swap_short"],
                    }
            return result
        except Exception as e:
            print(f"    Exness error [{account_key}] attempt {attempt+1}: {e}")
            if attempt < retries - 1:
                time.sleep(5)
    return {}

def run_exness():
    print("── Exness (4 servers) ──")
    output = {}
    for account_key, account_type in EXNESS_ACCOUNTS.items():
        print(f"  Fetching {account_key} ({account_type})...")
        data = fetch_exness(account_key, account_type)
        print(f"    Got {len(data)} symbols")
        for symbol, rates in data.items():
            if symbol not in output:
                output[symbol] = {}
            output[symbol][account_key] = rates
        time.sleep(4)
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
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Referer": "https://fxverify.com/",
    }
    try:
        req = urllib.request.Request(url, headers=headers)
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
                raw_name  = item.get("name", "")
                if item.get("swapType") != "InPoints":
                    continue
                canon = CBF_SYMBOL_MAP.get(raw_name, raw_name)
                if canon is None or canon not in OUR_SYMBOLS_SET or canon in broker_data:
                    continue
                lv = item.get("swapLong")
                sv = item.get("swapShort")
                broker_data[canon] = {
                    "long":  round(float(lv), 4) if lv is not None else None,
                    "short": round(float(sv), 4) if sv is not None else None,
                }
            time.sleep(0.8)
        print(f"    Got {len(broker_data)} symbols")
        for symbol, rates in broker_data.items():
            output.setdefault(symbol, {})[broker_key] = rates
    brokers = set(b for sym in output.values() for b in sym.keys())
    print(f"  Done. {len(output)} symbols, {len(brokers)} brokers")
    return output

# ─────────────────────────────────────────
# HF MARKETS — Playwright with extended timeout
# ─────────────────────────────────────────
HF_URLS = [
    "https://hfeu.com/en/trading-instruments/forex",
    "https://www.hfm.com/eu/en/trading-tools/swap-rates/",
]
HF_TABS = {
    "Premium Account":     "hf-premium",
    "Premium Pro Account": "hf-pro",
    "Zero Account":        "hf-zero",
}
HF_SYMBOL_MAP = {"XAUUSD": "GOLD", "XAGUSD": "SILVER"}

def parse_rate(val):
    if not val or val.strip() in ("-", "—", "N/A", ""):
        return None
    try:
        return round(float(val.strip().replace(",", ".")), 4)
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
            loaded = False
            for url in HF_URLS:
                try:
                    print(f"  Trying {url}")
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    time.sleep(5)
                    loaded = True
                    print(f"  Loaded OK")
                    break
                except Exception as e:
                    print(f"  Failed: {e}")

            if loaded:
                for tab_text, broker_key in HF_TABS.items():
                    print(f"  Tab: {tab_text}")
                    try:
                        page.click(f"text={tab_text}", timeout=15000)
                        time.sleep(3)
                        rows = page.query_selector_all("table tbody tr")
                        for row in rows:
                            cells = row.query_selector_all("td")
                            if len(cells) < 6:
                                continue
                            raw_sym = cells[0].inner_text().strip().upper()
                            sym = HF_SYMBOL_MAP.get(raw_sym, raw_sym)
                            if sym not in OUR_SYMBOLS_SET:
                                continue
                            short = parse_rate(cells[4].inner_text())
                            long  = parse_rate(cells[5].inner_text())
                            results.setdefault(sym, {})[broker_key] = {"long": long, "short": short}
                    except Exception as e:
                        print(f"    Error on {tab_text}: {e}")
            else:
                print("  All HF URLs failed — skipping")

            browser.close()
    except Exception as e:
        print(f"  HF Markets exception: {e}")

    brokers = set(b for s in results.values() for b in s.keys())
    print(f"  Done. {len(results)} symbols, brokers: {brokers}")
    return results

# ─────────────────────────────────────────
# MYFXBOOK — Pepperstone, Tickmill, XM, FP Markets, Deriv
# ─────────────────────────────────────────
MYFXBOOK_BROKER_MAP = {
    "Pepperstone": "pepperstone",
    "Tickmill":    "tickmill",
    "XMTrading":   "xm",
    "XM":          "xm",
    "FP Markets":  "fpmarkets",
    "Deriv":       "deriv",
    "Deriv.com":   "deriv",
}

def fetch_myfxbook(oid, long_or_short):
    url = (f"https://www.myfxbook.com/get-swap-chart-by-symbol.json"
           f"?symbolInfoOid={oid}&swapShortLong={long_or_short}&rand=0.5")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.myfxbook.com/forex-broker-swaps/pepperstone/208",
        "X-Requested-With": "XMLHttpRequest",
    }
    try:
        req = urllib.request.Request(url, headers=headers)
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
        if not long_data:
            blocked_count += 1
        time.sleep(1.2)
        short_data = fetch_myfxbook(oid, 1)
        time.sleep(1.2)
        output[symbol] = {}
        for broker in set(long_data) | set(short_data):
            output[symbol][broker] = {
                "long":  long_data.get(broker),
                "short": short_data.get(broker),
            }
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

    all_symbols = set(
        list(exn_data.keys()) + list(cbf_data.keys()) +
        list(hfm_data.keys()) + list(mfx_data.keys())
    )
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
