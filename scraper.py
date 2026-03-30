"""
SwapHunter Data Pipeline v4
Source: CashbackForex API (all brokers — raw swap points, quote currency per lot)
  - icmarkets, vantage, blackbull, pepperstone, tickmill, xm, fpmarkets,
    fusionmarkets, eightcap, exness-std, hf-premium, hf-pro, hf-zero

Runs via GitHub Actions every 2 hours. No server needed.
Swap values are stored as raw points (quote currency per lot per day).
USD conversion happens in the frontend arb engine, NOT here.
"""

import json
import time
import urllib.request
from datetime import datetime, timezone

# ─────────────────────────────────────────────────────
# BROKER CONFIG — CashbackForex IDs + group filters
# ─────────────────────────────────────────────────────
CBF_BASE = "https://spreads-api.cashbackforex.com/api/swapratesforbroker"

CBF_BROKERS = {
    3133: {"key": "icmarkets",     "groups": ["Forex Majors", "Forex Minors", "Metals", "Energies"]},
    1149: {"key": "vantage",       "groups": ["Forex", "Gold", "Silver", "Oil", "Commodities"]},
     970: {"key": "blackbull",     "groups": ["Forex Majors", "Forex", "Energies", "Commodities"]},
     256: {"key": "pepperstone",   "groups": ["Forex Majors", "Forex Minors", "Metals", "Energies"]},
     451: {"key": "tickmill",      "groups": ["Forex Majors", "Forex Minors", "Metals", "Energies"]},
     278: {"key": "xm",            "groups": ["Forex Majors", "Forex Minors", "Metals", "Energies"]},
     500: {"key": "fpmarkets",     "groups": ["Forex Majors", "Forex Minors", "Metals", "Energies"]},
     957: {"key": "fusionmarkets", "groups": ["Forex Majors", "Forex Minors", "Metals", "Energies"]},
    1101: {"key": "eightcap",      "groups": ["Forex Majors", "Forex Minors", "Metals", "Energies"]},
     498: {"key": "exness-std",    "groups": ["Forex Majors", "Forex Minors", "Metals", "Energies"]},
     158: {"key": "hf-premium",    "groups": ["Forex Majors", "Forex Minors", "Metals", "Energies"]},
}

# HF Pro and Zero share the same CBF entry as hf-premium (same broker page).
# We store as hf-premium only — frontend maps accordingly.

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

# CBF uses different names for some symbols — map to our canonical names
CBF_SYMBOL_MAP = {
    "XAUUSD":   "GOLD",   "XAGUSD":   "SILVER",
    "XBRUSD":   "UKOIL",  "XTIUSD":   "USOIL",  "XNGUSD":  "NATGAS",
    "UKOUSD":   "UKOIL",  "USOUSD":   "USOIL",  "NG-C":    "NATGAS",
    "BRENT":    "UKOIL",  "WTI":      "USOIL",
    # nulled — skip these if they appear
    "UKOUSDft": None, "CL-OIL": None, "Rolltest": None, "GCM25": None,
}

# ─────────────────────────────────────────────────────
# FETCH
# ─────────────────────────────────────────────────────
def fetch_cbf_group(cbf_id, group, retries=3):
    url = (f"{CBF_BASE}/{cbf_id}"
           f"?currentPage=1&countPerPage=100&search=&group={group.replace(' ', '%20')}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Referer": "https://fxverify.com/",
    }
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
            return data.get("swapRates", {}).get("swapRates", [])
        except Exception as e:
            print(f"    CBF error [{cbf_id}] group={group} attempt {attempt+1}: {e}")
            if attempt < retries - 1:
                time.sleep(3)
    return []


def run_cbf():
    print("── CashbackForex (all brokers) ──")
    output = {}

    for cbf_id, config in CBF_BROKERS.items():
        broker_key = config["key"]
        print(f"  {broker_key} (CBF ID: {cbf_id})")
        broker_data = {}

        for group in config["groups"]:
            rows = fetch_cbf_group(cbf_id, group)
            for item in rows:
                raw_name = item.get("name", "")

                # Only accept raw swap points — skip USD-converted entries
                if item.get("swapType") != "InPoints":
                    continue

                canon = CBF_SYMBOL_MAP.get(raw_name, raw_name)
                if canon is None or canon not in OUR_SYMBOLS_SET:
                    continue
                if canon in broker_data:
                    continue  # first group wins, no duplicates

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
    print(f"  Done. {len(output)} symbols, {len(brokers)} brokers: {sorted(brokers)}")
    return output


# ─────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────
def run():
    print(f"\nSwapHunter scraper v4 — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")

    merged = run_cbf()

    brokers = set(b for sym in merged.values() for b in sym.keys())
    output = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "sources": sorted(brokers),
        "swaps": merged,
    }

    with open("swaps.json", "w") as f:
        json.dump(output, f, indent=2)

    total = sum(len(v) for v in merged.values())
    print(f"\n✓ Done — {len(merged)} symbols, {len(brokers)} brokers, {total} entries")
    print(f"  Brokers: {sorted(brokers)}")


if __name__ == "__main__":
    run()
