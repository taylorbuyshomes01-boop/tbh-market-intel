#!/usr/bin/env python3
"""Extract zip codes for top 25 qualifying markets from Redfin zip data."""
import csv
import json

# Load top 25 market results
with open("data/market_research_latest.json") as f:
    results = json.load(f)

top25 = results["top25"]
top25_metros_full = {m["metro"] for m in top25}
# Zip data uses short names like "Rochester, NY" instead of "Rochester, NY metro area"
top25_metros_short = {m["metro"].replace(" metro area", "") for m in top25}
# Map short -> full for output
short_to_full = {m["metro"].replace(" metro area", ""): m["metro"] for m in top25}
top25_metros = top25_metros_short
# Zip data may lag behind metro data - find the latest available period in zip data
import subprocess
result = subprocess.run(['bash', '-c', 'awk -F\'\\t\' \'NR>1 {gsub(/"/, "", $1); print $1}\' data/raw/redfin_zips.tsv | sort -u | tail -1'], capture_output=True, text=True)
latest_period = result.stdout.strip()
print(f"Using latest available zip period: {latest_period} (metro period was {results['period']})")

print(f"Looking for zips in {len(top25_metros)} metros, period {latest_period}")
print(f"Metros: {', '.join(sorted(top25_metros))}")

# Scan zip data - match by PARENT_METRO_REGION
zip_results = {}
row_count = 0

with open("data/raw/redfin_zips.tsv", "r") as f:
    reader = csv.DictReader(f, delimiter="\t")
    for row in reader:
        row_count += 1
        if row_count % 1000000 == 0:
            print(f"  Processed {row_count:,} rows...")
        
        pt = row.get("PROPERTY_TYPE", "").strip('"').lower()
        sa = row.get("IS_SEASONALLY_ADJUSTED", "").strip('"').lower()
        period = row.get("PERIOD_BEGIN", "").strip('"')
        
        if pt != "all residential" or sa != "false" or period != latest_period:
            continue
        
        parent_metro = row.get("PARENT_METRO_REGION", "").strip('"')
        region = row.get("REGION", "").strip('"')  # zip code
        
        if parent_metro not in top25_metros:
            continue
        parent_metro = short_to_full.get(parent_metro, parent_metro)
        
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
        state_code = row.get("STATE_CODE", "").strip('"')
        city = row.get("CITY", "").strip('"')
        
        if homes_sold and homes_sold >= 10:
            if parent_metro not in zip_results:
                zip_results[parent_metro] = []
            
            zip_results[parent_metro].append({
                "zip": region,
                "city": city,
                "state": state_code,
                "homes_sold": int(homes_sold),
                "med_price": int(med_price) if med_price else None,
                "dom": int(dom) if dom is not None else None,
                "yoy_pct": round(yoy * 100, 2) if yoy is not None else None,
                "ratio": round(ratio, 4) if ratio is not None else None,
            })

print(f"\nTotal rows processed: {row_count:,}")
print(f"Metros with qualifying zips: {len(zip_results)}")

# Sort zips by deal volume
for metro in zip_results:
    zip_results[metro].sort(key=lambda x: x["homes_sold"], reverse=True)

# Print results
total_zips = 0
for metro in sorted(zip_results.keys()):
    zips = zip_results[metro]
    total_zips += len(zips)
    print(f"\n📍 {metro} ({len(zips)} zips)")
    for z in zips:
        price_str = f"${z['med_price']:,}" if z['med_price'] else "N/A"
        yoy_str = f"{z['yoy_pct']}%" if z['yoy_pct'] is not None else "N/A"
        dom_str = str(z['dom']) if z['dom'] is not None else "N/A"
        ratio_str = str(z['ratio']) if z['ratio'] is not None else "N/A"
        print(f"  {z['zip']} | {z['city']}, {z['state']} | {z['homes_sold']} deals | {price_str} | DOM {dom_str} | YOY {yoy_str} | Ratio {ratio_str}")

print(f"\nTotal qualifying zips: {total_zips}")

# Save
with open("data/zip_targets_latest.json", "w") as f:
    json.dump(zip_results, f, indent=2)
print("✅ Saved to data/zip_targets_latest.json")
