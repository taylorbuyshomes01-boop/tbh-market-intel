#!/usr/bin/env python3
"""
Scout Market Research Pipeline — Full 50-State Analysis
Scores markets: YOY 40% | DOM 30% | Ratio 30%
Qualifies: YOY >= 5% AND DOM <= 50 AND Ratio >= 0.5 (sale-to-list >= 0.95 maps to score)
"""
import csv
import json
from collections import defaultdict

# --- REDFIN STATE DATA ---
print("=" * 60)
print("LOADING REDFIN STATE DATA...")
print("=" * 60)

state_data = []
with open("data/raw/redfin_state.tsv", "r") as f:
    reader = csv.DictReader(f, delimiter="\t")
    for row in reader:
        # Get most recent data, all residential, not seasonally adjusted
        pt = row.get("PROPERTY_TYPE", "").strip('"').lower()
        sa = row.get("IS_SEASONALLY_ADJUSTED", "").strip('"').lower()
        if pt == "all residential" and sa == "false":
            state_data.append(row)

# Find the most recent period
periods = sorted(set(r["PERIOD_BEGIN"].strip('"') for r in state_data), reverse=True)
latest_period = periods[0]
print(f"Latest period: {latest_period}")

# Filter to latest period
latest_states = [r for r in state_data if r["PERIOD_BEGIN"].strip('"') == latest_period]
print(f"States in latest period: {len(latest_states)}")

# Parse state metrics
state_results = []
for row in latest_states:
    state = row["STATE"].strip('"')
    state_code = row["STATE_CODE"].strip('"')
    
    def safe_float(val):
        v = val.strip('"') if val else ""
        try:
            return float(v)
        except:
            return None
    
    yoy = safe_float(row.get("MEDIAN_SALE_PRICE_YOY", ""))
    dom = safe_float(row.get("MEDIAN_DOM", ""))
    ratio = safe_float(row.get("AVG_SALE_TO_LIST", ""))
    med_price = safe_float(row.get("MEDIAN_SALE_PRICE", ""))
    homes_sold = safe_float(row.get("HOMES_SOLD", ""))
    inventory = safe_float(row.get("INVENTORY", ""))
    pending = safe_float(row.get("PENDING_SALES", ""))
    new_listings = safe_float(row.get("NEW_LISTINGS", ""))
    price_drops = safe_float(row.get("PRICE_DROPS", ""))
    months_supply = safe_float(row.get("MONTHS_OF_SUPPLY", ""))
    sold_above = safe_float(row.get("SOLD_ABOVE_LIST", ""))
    
    if state and yoy is not None and dom is not None and ratio is not None:
        state_results.append({
            "state": state,
            "state_code": state_code,
            "yoy_pct": round(yoy * 100, 2),
            "dom": int(dom) if dom else None,
            "ratio": round(ratio, 4),
            "med_price": int(med_price) if med_price else None,
            "homes_sold": int(homes_sold) if homes_sold else None,
            "inventory": int(inventory) if inventory else None,
            "pending": int(pending) if pending else None,
            "new_listings": int(new_listings) if new_listings else None,
            "price_drops_pct": round(price_drops * 100, 2) if price_drops is not None else None,
            "months_supply": round(months_supply, 1) if months_supply is not None else None,
            "sold_above_pct": round(sold_above * 100, 2) if sold_above is not None else None,
        })

# Categorize states
hot = [s for s in state_results if s["yoy_pct"] >= 8]
growing = [s for s in state_results if 5 <= s["yoy_pct"] < 8]
cooling = [s for s in state_results if s["yoy_pct"] < 0]
stable = [s for s in state_results if 0 <= s["yoy_pct"] < 5]

hot.sort(key=lambda x: x["yoy_pct"], reverse=True)
growing.sort(key=lambda x: x["yoy_pct"], reverse=True)
cooling.sort(key=lambda x: x["yoy_pct"])
stable.sort(key=lambda x: x["yoy_pct"], reverse=True)

print(f"\n🔥 Hot (8%+): {len(hot)} states")
for s in hot:
    print(f"  {s['state_code']}: YOY {s['yoy_pct']}% | DOM {s['dom']} | Ratio {s['ratio']} | ${s['med_price']:,}")

print(f"\n📈 Growing (5-8%): {len(growing)} states")
for s in growing:
    print(f"  {s['state_code']}: YOY {s['yoy_pct']}% | DOM {s['dom']} | Ratio {s['ratio']} | ${s['med_price']:,}")

print(f"\n📊 Stable (0-5%): {len(stable)} states")
for s in stable:
    print(f"  {s['state_code']}: YOY {s['yoy_pct']}% | DOM {s['dom']} | Ratio {s['ratio']} | ${s['med_price']:,}")

print(f"\n📉 Cooling (<0%): {len(cooling)} states")
for s in cooling:
    print(f"  {s['state_code']}: YOY {s['yoy_pct']}% | DOM {s['dom']} | Ratio {s['ratio']} | ${s['med_price']:,}")

# --- REDFIN METRO DATA ---
print("\n" + "=" * 60)
print("LOADING REDFIN METRO DATA...")
print("=" * 60)

metro_data = []
with open("data/raw/redfin_metro.tsv", "r") as f:
    reader = csv.DictReader(f, delimiter="\t")
    for row in reader:
        pt = row.get("PROPERTY_TYPE", "").strip('"').lower()
        sa = row.get("IS_SEASONALLY_ADJUSTED", "").strip('"').lower()
        if pt == "all residential" and sa == "false":
            metro_data.append(row)

metro_periods = sorted(set(r["PERIOD_BEGIN"].strip('"') for r in metro_data), reverse=True)
metro_latest = metro_periods[0]
print(f"Latest metro period: {metro_latest}")

latest_metros = [r for r in metro_data if r["PERIOD_BEGIN"].strip('"') == metro_latest]
print(f"Metros in latest period: {len(latest_metros)}")

# Parse and score metros
metro_results = []
for row in latest_metros:
    region = row["REGION"].strip('"')
    state_code = row["STATE_CODE"].strip('"')
    
    def safe_float(val):
        v = val.strip('"') if val else ""
        try:
            return float(v)
        except:
            return None
    
    yoy = safe_float(row.get("MEDIAN_SALE_PRICE_YOY", ""))
    dom = safe_float(row.get("MEDIAN_DOM", ""))
    ratio = safe_float(row.get("AVG_SALE_TO_LIST", ""))
    med_price = safe_float(row.get("MEDIAN_SALE_PRICE", ""))
    homes_sold = safe_float(row.get("HOMES_SOLD", ""))
    inventory = safe_float(row.get("INVENTORY", ""))
    pending = safe_float(row.get("PENDING_SALES", ""))
    new_listings = safe_float(row.get("NEW_LISTINGS", ""))
    price_drops = safe_float(row.get("PRICE_DROPS", ""))
    months_supply = safe_float(row.get("MONTHS_OF_SUPPLY", ""))
    sold_above = safe_float(row.get("SOLD_ABOVE_LIST", ""))
    dom_yoy = safe_float(row.get("MEDIAN_DOM_YOY", ""))
    inv_yoy = safe_float(row.get("INVENTORY_YOY", ""))
    
    if yoy is None or dom is None or ratio is None:
        continue
    
    yoy_pct = round(yoy * 100, 2)
    
    # QUALIFICATION: YOY >= 5% AND DOM <= 50 AND sale-to-list ratio >= 0.95
    # The "ratio 0.5+" in config likely means sale-to-list score, but actual ratio is ~0.95-1.02
    # We interpret ratio >= 0.95 as the qualifying threshold
    qualifies = yoy_pct >= 5 and dom <= 50 and ratio >= 0.95
    
    # SCORING: YOY 40% | DOM 30% | Ratio 30%
    # Normalize: YOY score = yoy_pct / 20 (cap at 1.0)
    # DOM score = (50 - dom) / 50 (lower DOM = higher score)  
    # Ratio score = (ratio - 0.90) / 0.15 (cap at 1.0)
    yoy_score = min(max(yoy_pct / 20, 0), 1.0)
    dom_score = min(max((50 - dom) / 50, 0), 1.0)
    ratio_score = min(max((ratio - 0.90) / 0.15, 0), 1.0)
    
    composite = round(yoy_score * 0.4 + dom_score * 0.3 + ratio_score * 0.3, 4)
    
    metro_results.append({
        "metro": region,
        "state_code": state_code,
        "yoy_pct": yoy_pct,
        "dom": int(dom),
        "ratio": round(ratio, 4),
        "med_price": int(med_price) if med_price else None,
        "homes_sold": int(homes_sold) if homes_sold else None,
        "inventory": int(inventory) if inventory else None,
        "pending": int(pending) if pending else None,
        "new_listings": int(new_listings) if new_listings else None,
        "price_drops_pct": round(price_drops * 100, 2) if price_drops is not None else None,
        "months_supply": round(months_supply, 1) if months_supply is not None else None,
        "sold_above_pct": round(sold_above * 100, 2) if sold_above is not None else None,
        "dom_yoy": int(dom_yoy) if dom_yoy is not None else None,
        "inv_yoy_pct": round(inv_yoy * 100, 2) if inv_yoy is not None else None,
        "yoy_score": round(yoy_score, 4),
        "dom_score": round(dom_score, 4),
        "ratio_score": round(ratio_score, 4),
        "composite": composite,
        "qualifies": qualifies,
    })

# Filter qualifying and sort by composite score
qualifying = [m for m in metro_results if m["qualifies"]]
qualifying.sort(key=lambda x: x["composite"], reverse=True)

# Top 25
top25 = qualifying[:25]

print(f"\nTotal metros analyzed: {len(metro_results)}")
print(f"Qualifying markets (YOY>=5%, DOM<=50, Ratio>=0.95): {len(qualifying)}")
print(f"\n{'='*80}")
print("TOP 25 QUALIFYING MARKETS")
print(f"{'='*80}")
print(f"{'Rank':<5} {'Metro':<40} {'YOY%':<8} {'DOM':<6} {'Ratio':<8} {'Price':<12} {'Sold':<8} {'Score':<8}")
print("-" * 95)
for i, m in enumerate(top25, 1):
    price_str = f"${m['med_price']:,}" if m['med_price'] else "N/A"
    sold_str = str(m['homes_sold']) if m['homes_sold'] else "N/A"
    print(f"{i:<5} {m['metro'][:38]:<40} {m['yoy_pct']:<8} {m['dom']:<6} {m['ratio']:<8} {price_str:<12} {sold_str:<8} {m['composite']:<8}")

# Also show all qualifying that didn't make top 25
if len(qualifying) > 25:
    print(f"\n--- Additional qualifying markets ({len(qualifying) - 25} more) ---")
    for m in qualifying[25:50]:
        price_str = f"${m['med_price']:,}" if m['med_price'] else "N/A"
        print(f"  {m['metro'][:40]}: YOY {m['yoy_pct']}% | DOM {m['dom']} | Ratio {m['ratio']} | {price_str} | Score {m['composite']}")

# Save results
output = {
    "period": metro_latest,
    "total_metros": len(metro_results),
    "qualifying_count": len(qualifying),
    "state_summary": {
        "hot": [{"state": s["state_code"], "yoy": s["yoy_pct"], "dom": s["dom"], "ratio": s["ratio"], "price": s["med_price"]} for s in hot],
        "growing": [{"state": s["state_code"], "yoy": s["yoy_pct"], "dom": s["dom"], "ratio": s["ratio"], "price": s["med_price"]} for s in growing],
        "stable": [{"state": s["state_code"], "yoy": s["yoy_pct"], "dom": s["dom"], "ratio": s["ratio"], "price": s["med_price"]} for s in stable],
        "cooling": [{"state": s["state_code"], "yoy": s["yoy_pct"], "dom": s["dom"], "ratio": s["ratio"], "price": s["med_price"]} for s in cooling],
    },
    "top25": top25,
    "all_qualifying": qualifying,
    "all_states": state_results,
}

with open("data/market_research_latest.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"\n✅ Results saved to data/market_research_latest.json")
print(f"Total states: {len(state_results)}")
