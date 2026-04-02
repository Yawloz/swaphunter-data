"""
SwapHunter Data Pipeline v5
Sources:
  - CashbackForex API  -> all brokers except Exness (raw swap points, quote currency per lot)
  - Exness GraphQL API -> exness-std only (CBF data incomplete for Exness majors)
  - HF Markets         -> hf-premium/pro/zero via Playwright (3 separate account types)

Swap values stored as raw points (quote currency per lot per day).
contractSize stored per entry for USD conversion in the arb engine.
USD conversion happens in the FRONTEND, NOT here.

NOTE: BlackBull Energies contractSize from CBF shows 1 — needs manual verification
against BlackBull website (likely 100 barrels/lot like other brokers).
"""

import json
import time
import urllib.request
from datetime import datetime, timezone

# ─────────────────────────────────────────────────────
# SYMBOL CONFIG
# ─────────────────────────────────────────────────────
OUR_SYMBOLS_SET = {
    "EURUSD", "GBPUSD", "USDJPY", "GBPJPY", "USDCAD", "EURAUD", "EURJPY",
    "AUDCAD", "AUDJPY", "AUDNZD", "AUDUSD", "CADJPY", "EURCAD", "EURCHF",
    "EURGBP", "EURNZD", "GBPCAD", "GBPCHF", "NZDCAD", "NZDJPY", "NZDUSD",
    "USDCHF", "CHFJPY", "AUDCHF", "GBPNZD", "NZDCHF", "SILVER", "GOLD",
    "CADCHF", "GBPAUD", "UKOIL", "USOIL", "NATGAS",
}

CBF_SYMBOL_MAP = {
    # Metals
    "XAUUSD":    "GOLD",    "XAGUSD":    "SILVER",
    # Energies — standard names
    "XBRUSD":    "UKOIL",   "XTIUSD":    "USOIL",    "XNGUSD":    "NATGAS",
    "UKOUSD":    "UKOIL",   "USOUSD":    "USOIL",
    "BRENT":     "UKOIL",   "WTI":       "USOIL",    "NATGAS":    "NATGAS",
    # Pepperstone specific
    "SpotBrent": "UKOIL",   "SpotCrude": "USOIL",    "NatGas":    "NATGAS",
    # Tickmill specific
    "NAT.GAS":   "NATGAS",
    # Vantage specific (NG-C already in CBF_SYMBOL_MAP via Commodities group)
    "NG-C":      "NATGAS",
    # Exness energies (canonical already)
    "UKOIL":     "UKOIL",   "USOIL":     "USOIL",
    # XM uses canonical names directly
    "GOLD":      "GOLD",    "SILVER":    "SILVER",
    # Nulled — skip these
    "UKOUSDft":  None,      "CL-OIL":    None,
    "Rolltest":  None,      "GCM25":     None,
    "Gasoline":  None,
}

# ─────────────────────────────────────────────────────
# CASHBACKFOREX BROKER CONFIG
# ─────────────────────────────────────────────────────
CBF_BASE = "https://spreads-api.cashbackforex.com/api/swapratesforbroker"

# pages: {group_name: num_pages} — only needed for paginated groups
# strip_suffix: list of suffixes to strip from symbol names
CBF_BROKERS = {
    3133: {
        "key": "icmarkets",
        "groups": ["Forex Majors", "Forex Minors", "Metals", "Energies"],
    },
    1149: {
        "key": "vantage",
        "groups": ["Forex Raw ECN", "Gold +", "Silver", "Oil", "Commodities"],
        "strip_suffix": ["+"],
    },
    970: {
        "key": "blackbull",
        "groups": ["Forex Majors", "Forex", "Commodities", "Energies"],
        # CBF shows contractSize=1 for BRENT/WTI — actual is 1000. NATGAS is 10000.
        "contractSize_override": {"UKOIL": 1000, "USOIL": 1000, "NATGAS": 1},
    },
    # Pepperstone moved to Myfxbook source (CBF data is wrong for them)
    # 256: pepperstone — disabled
    451: {
        "key": "tickmill",
        "groups": ["Forex", "CFD-Crude-Oil", "CFD-2", ""],
        "pages": {"": 2},  # page 2 of empty group has Gold/Silver
        # CBF shows wrong contractSize for Tickmill energies
        # cs=10 makes swapToUSD formula correct: pts * 10 * 0.001 = pts * 0.01
        "contractSize_override": {"UKOIL": 10, "USOIL": 10},
    },
    278: {
        "key": "xm",
        "groups": ["Forex 2", "Forex 3", "Spot Metals"],
    },
    500: {
        "key": "fpmarkets",
        "groups": ["Forex R", "Metals 1 R", "Metals 2 R", "Commodity"],
        "pages": {"Forex R": 2},
    },
    957: {
        "key": "fusionmarkets",
        "groups": ["Forex", "Commodities", "Energy"],
    },
    1101: {
        "key": "eightcap",
        "groups": ["Forex", "Oil UK", "Oil US", "Metals"],
    },
}

# ─────────────────────────────────────────────────────
# CBF FETCH
# ─────────────────────────────────────────────────────
def fetch_cbf_page(cbf_id, group, page=1, retries=3):
    import urllib.parse
    group_encoded = urllib.parse.quote(group, safe='')
    url = (f"{CBF_BASE}/{cbf_id}"
           f"?currentPage={page}&countPerPage=100&search=&group={group_encoded}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://fxverify.com",
        "Referer": "https://fxverify.com/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
    }
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
            return data.get("swapRates", {}).get("swapRates", [])
        except Exception as e:
            print(f"    CBF error [{cbf_id}] group={group} page={page} attempt {attempt+1}: {e}")
            if attempt < retries - 1:
                time.sleep(3)
    return []


def strip_symbol_suffix(name, suffixes):
    for suffix in suffixes:
        if name.endswith(suffix):
            return name[:-len(suffix)]
    return name


def run_cbf():
    print("── CashbackForex ──")
    output = {}

    for cbf_id, config in CBF_BROKERS.items():
        broker_key = config["key"]
        print(f"  {broker_key} (CBF ID: {cbf_id})")
        broker_data = {}
        pages_config = config.get("pages", {})
        strip_suffixes = config.get("strip_suffix", [])
        cs_override = config.get("contractSize_override", {})
        exclude_syms = config.get("exclude_symbols", set())

        for group in config["groups"]:
            num_pages = pages_config.get(group, 1)
            for page in range(1, num_pages + 1):
                rows = fetch_cbf_page(cbf_id, group, page)
                for item in rows:
                    raw_name = item.get("name", "")

                    if strip_suffixes:
                        raw_name = strip_symbol_suffix(raw_name, strip_suffixes)

                    swap_type = item.get("swapType", "")

                    canon = CBF_SYMBOL_MAP.get(raw_name, raw_name)
                    if canon is None or canon not in OUR_SYMBOLS_SET:
                        continue
                    if canon in exclude_syms:
                        continue
                    if canon in broker_data:
                        continue  # first occurrence wins

                    lv = item.get("swapLong")
                    sv = item.get("swapShort")
                    cs = cs_override.get(canon) or item.get("contractSize")

                    if swap_type == "InPoints":
                        long_val  = round(float(lv), 4) if lv is not None else None
                        short_val = round(float(sv), 4) if sv is not None else None
                    elif swap_type == "InPips":
                        # 1 pip = 10 points for most pairs, JPY pairs same
                        pip_to_pts = 10
                        long_val  = round(float(lv) * pip_to_pts, 4) if lv is not None else None
                        short_val = round(float(sv) * pip_to_pts, 4) if sv is not None else None
                    elif swap_type == "InMoney":
                        # Already in account currency per lot — store as-is
                        # Tag with a special contractSize=0 so frontend knows to use directly
                        long_val  = round(float(lv), 4) if lv is not None else None
                        short_val = round(float(sv), 4) if sv is not None else None
                    else:
                        # Unknown type — skip
                        print(f"      Unknown swapType={swap_type} for {raw_name}, skipping")
                        continue

                    broker_data[canon] = {
                        "long":         long_val,
                        "short":        short_val,
                        "contractSize": int(cs) if cs is not None else None,
                        "swapType":     swap_type,
                    }
                time.sleep(0.8)

        # Log swapTypes found for debugging
        swap_types_found = {}
        for sym_data in broker_data.values():
            t = sym_data.get("swapType", "unknown")
            swap_types_found[t] = swap_types_found.get(t, 0) + 1
        print(f"    Got {len(broker_data)} symbols | swapTypes: {swap_types_found}")
        for symbol, rates in broker_data.items():
            output.setdefault(symbol, {})[broker_key] = rates

    brokers = set(b for sym in output.values() for b in sym.keys())
    print(f"  Done. {len(output)} symbols, {len(brokers)} brokers: {sorted(brokers)}")
    return output


# ─────────────────────────────────────────────────────
# EXNESS — GraphQL (CBF missing majors for Exness)
# ─────────────────────────────────────────────────────
EXNESS_METAL_MAP = {"GOLD": "XAUUSDm", "SILVER": "XAGUSDm"}
EXNESS_SYMBOLS = [
    "EURUSD", "GBPUSD", "USDJPY", "GBPJPY", "USDCAD", "EURAUD", "EURJPY",
    "AUDCAD", "AUDJPY", "AUDNZD", "AUDUSD", "CADJPY", "EURCAD", "EURCHF",
    "EURGBP", "EURNZD", "GBPCAD", "GBPCHF", "NZDCAD", "NZDJPY", "NZDUSD",
    "USDCHF", "CHFJPY", "AUDCHF", "GBPNZD", "NZDCHF", "SILVER", "GOLD",
    "CADCHF", "GBPAUD",
]
EXNESS_CONTRACT_SIZES = {"GOLD": 100, "SILVER": 5000}  # FX default 100000

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
    print("── Exness (std, GraphQL) ──")
    instruments = [EXNESS_METAL_MAP.get(s, s + "m") for s in EXNESS_SYMBOLS]
    reverse = {EXNESS_METAL_MAP.get(s, s + "m"): s for s in EXNESS_SYMBOLS}
    payload = json.dumps({
        "operationName": "getTradingInstruments",
        "query": GRAPHQL_QUERY,
        "variables": {"account_type": "mt5_mini_real_vc", "instruments": instruments}
    }).encode()
    output = {}
    try:
        req = urllib.request.Request(
            "https://www.exness.com/pwapi/",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0",
                "Origin": "https://www.exness.com",
                "Referer": "https://www.exness.com/trading/swap-rates/",
            },
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode())
        for item in (data.get("data", {}).get("tradingInstruments") or []):
            canon = reverse.get(item["instrument"])
            if canon:
                output.setdefault(canon, {})["exness-std"] = {
                    "long":         round(item["swap_long"] * 10, 4),
                    "short":        round(item["swap_short"] * 10, 4),
                    "contractSize": EXNESS_CONTRACT_SIZES.get(canon, 100000),
                }
        print(f"  Got {len(output)} symbols")
    except Exception as e:
        print(f"  Error: {e}")
    return output


# ─────────────────────────────────────────────────────
# HF MARKETS — Playwright (3 account types separately)
# col mapping confirmed: sym=col[1], short=col[5], long=col[6]
# ─────────────────────────────────────────────────────
HF_URLS = [
    "https://hfeu.com/en/trading-instruments/forex",
]
HF_SYMBOL_MAP = {"XAUUSD": "GOLD", "XAGUSD": "SILVER", "USOIL.S": "USOIL", "usoil.s": "USOIL"}

# HF Markets only offers these symbols — filter out anything else
# No AUD crosses (except AUDUSD), no metals, no energies
# HF Markets confirmed symbols - exclude everything not on their platform
HF_EXCLUDED_SYMBOLS = {
    # Metals HF doesn't have
    "SILVER",
    # Energies HF doesn't have
    "UKOIL", "NATGAS",
    # AUD crosses (only AUDUSD exists)
    "AUDCAD", "AUDJPY", "AUDNZD", "AUDCHF",
    # NZD pairs not offered
    "NZDUSD", "NZDCAD", "NZDCHF",
    # EUR crosses not offered
    "EURAUD", "EURNZD",
    # GBP crosses not offered
    "GBPAUD",
    # NZD crosses
    "GBPNZD", "NZDJPY",
}

# HF contract sizes confirmed from CBF:
# FX: 100000, GOLD: 100, SILVER: 1000, UKOIL/USOIL: 100
HF_CONTRACT_SIZES = {
    "GOLD": 100, "SILVER": 1000,
    "UKOIL": 100, "USOIL": 100, "NATGAS": 10000,
}

HF_CONTAINERS = {
    "premium-table-container": "hf-premium",
    "pro-table-container":     "hf-pro",
    "zero-table-container":    "hf-zero",
}

def parse_rate(val):
    if not val or str(val).strip() in ("-", "—", "N/A", ""):
        return None
    try:
        return round(float(str(val).strip().replace(",", ".")), 4)
    except Exception:
        return None

def run_hfmarkets():
    print("── HF Markets (Playwright) ──")
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
            for hf_url in HF_URLS:
                print(f"  Scraping {hf_url}")
                page.goto(hf_url, wait_until="domcontentloaded", timeout=60000)
                time.sleep(4)
                page.evaluate("""
                    ['cookiescript_injected_wrapper','cookiescript_injected'].forEach(id => {
                        const el = document.getElementById(id); if (el) el.remove();
                    });
                """)
                time.sleep(1)

                # Debug: dump ALL symbol names found in ALL tables on this page
                all_syms = page.evaluate("""
                    () => {
                        const rows = document.querySelectorAll('table tbody tr');
                        const syms = [];
                        rows.forEach(r => {
                            const cells = r.querySelectorAll('td');
                            if(cells.length >= 2) syms.push(cells[1].innerText.trim());
                        });
                        return [...new Set(syms)].filter(s => s.length > 0);
                    }
                """)
                print(f"    All symbols on page: {all_syms}")

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
                                        cells[1].innerText.trim(),
                                        cells[5].innerText.trim(),
                                        cells[6].innerText.trim()
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
                        if sym not in OUR_SYMBOLS_SET:
                            continue
                        if sym in HF_EXCLUDED_SYMBOLS:
                            continue
                        short = parse_rate(row[1])
                        long  = parse_rate(row[2])
                        cs = HF_CONTRACT_SIZES.get(sym, 100000)
                        results.setdefault(sym, {})[broker_key] = {
                            "long": long, "short": short, "contractSize": cs
                        }
                        matched += 1
                    print(f"  {broker_key}: {len(rows)} rows → {matched} matched")

            browser.close()
    except Exception as e:
        print(f"  Exception: {e}")

    brokers = set(b for s in results.values() for b in s.keys())
    print(f"  Done. {len(results)} symbols, brokers: {brokers}")
    return results



# ─────────────────────────────────────────────────────
# MYFXBOOK — Pepperstone only (CBF data wrong for them)
# Returns USD per lot directly — stored with swapType="InMoney"
# so frontend skips swapToUSD conversion
# ─────────────────────────────────────────────────────
PEPPERSTONE_OIDS = {
    "EURUSD": 116025, "GBPUSD": 116026, "USDJPY": 116027,
    "GBPJPY": 116029, "USDCAD": 116030, "EURAUD": 116031,
    "EURJPY": 116032, "AUDCAD": 116034, "AUDJPY": 116037,
    "AUDNZD": 116035, "AUDUSD": 116028, "CADJPY": 116038,
    "EURCAD": 116039, "EURCHF": 116040, "EURGBP": 116042,
    "EURNZD": 116044, "GBPCAD": 116048, "GBPCHF": 116049,
    "NZDCAD": 116050, "NZDJPY": 116051, "NZDUSD": 116052,
    "USDCHF": 116053, "CHFJPY": 116068, "AUDCHF": 116036,
    "GBPNZD": 116070, "NZDCHF": 116071, "SILVER": 116072,
    "GOLD":   116073, "CADCHF": 116082, "GBPAUD": 116083,
}

PEPPERSTONE_CS = {
    "GOLD": 100, "SILVER": 5000,
    "UKOIL": 1000, "USOIL": 1000, "NATGAS": 10000,
}

def fetch_myfxbook_pepperstone(oid, direction):
    url = (f"https://www.myfxbook.com/get-swap-chart-by-symbol.json"
           f"?symbolInfoOid={oid}&swapShortLong={direction}&rand=0.5")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": "https://www.myfxbook.com/forex-broker-swaps/pepperstone/208",
        "X-Requested-With": "XMLHttpRequest",
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        brokers = data.get("categories", [])
        values  = data.get("series", [{}])[0].get("data", [])
        for name, val in zip(brokers, values):
            if name in ("Pepperstone", "Pepperstone AU"):
                return round(float(val), 4)
        return None
    except Exception as e:
        print(f"    Myfxbook error OID {oid}: {e}")
        return None

def run_pepperstone_myfxbook():
    print("── Pepperstone (Myfxbook) ──")
    output = {}
    for symbol, oid in PEPPERSTONE_OIDS.items():
        print(f"  {symbol}...", end=" ", flush=True)
        long_val  = fetch_myfxbook_pepperstone(oid, 0)
        time.sleep(1.0)
        short_val = fetch_myfxbook_pepperstone(oid, 1)
        time.sleep(1.0)
        if long_val is not None or short_val is not None:
            cs = PEPPERSTONE_CS.get(symbol, 100000)
            output.setdefault(symbol, {})["pepperstone"] = {
                