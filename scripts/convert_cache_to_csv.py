import json
import pandas as pd
import os

CACHE_FILE = '.cache/territory_resolution.json'
OUTPUT_CSV = 'broker_location_cache.csv'

if os.path.exists(CACHE_FILE):
    print(f"Loading JSON Cache: {CACHE_FILE}")
    with open(CACHE_FILE, 'r') as f:
        data = json.load(f)
    
    # Convert { "Firm": {data} } -> DataFrame
    df = pd.DataFrame.from_dict(data, orient='index')
    
    # Ensure key column exists
    df['target_firm_raw'] = df.index
    
    # Save as CSV for the Canonical Script
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"✅ Converted Cache to {OUTPUT_CSV}: {len(df)} rows")
else:
    print(f"❌ Cache file not found: {CACHE_FILE}")
