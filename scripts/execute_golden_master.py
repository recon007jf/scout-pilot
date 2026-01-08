import pandas as pd
import os
import sys
import json
import glob
import time
from datetime import datetime

# Ensure we can import the resolver
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from resolve_territory_production_v2 import TerritoryResolver, normalize_firm_name

# --- CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
INPUT_DIR = os.path.join(PROJECT_ROOT, "backend", "data", "input")
ARTIFACTS_DIR = os.path.join(PROJECT_ROOT, "artifacts")

OUTPUT_CSV = os.path.join(ARTIFACTS_DIR, "Master_Hunting_List_Production_v2.csv")

# File Mappings: Pattern -> Broker Column -> Contact Columns Mapping
FILE_CONFIGS = [
    {
        "pattern": "AndrewWestRegion_Benefit_Flow_2025.csv",
        "broker_col": "Broker Office Name",
        "contact_cols": {"Contact Full Name": "Contact_Name", "Contact Email": "Email", "Contact Job Title": "Title", "Company Name": "Client_Firm"}
    },
    {
        "pattern": "IGNORED_BF_RAW_*.csv", # AZ, OR, WA state exports
        "broker_col": "Broker Office Name",
        "contact_cols": {"Contact Full Name": "Contact_Name", "Contact Email": "Email", "Contact Job Title": "Title", "Company Name": "Client_Firm"}
    },
    {
        "pattern": "BF_RAW_Leads.csv",
        "broker_col": "PROVIDER_NAME_NORM",
        "contact_cols": {"Contact Full Name": "Contact_Name", "Contact Email": "Email", "Contact Job Title": "Title", "Company Name": "Company Name"} # Note: Check Company Name col in source
    },
    {
        "pattern": "Jan6_Master_HitList.csv",
        "broker_col": "Target_Broker_Firm",
        "contact_cols": {"Broker_Contact_Name": "Contact_Name", "Broker_Contact_Email": "Email", "Broker_Role": "Title", "Client_Company": "Client_Firm"}
    }
]

def load_reservoir():
    all_rows = []
    print(f"Scanning {INPUT_DIR}...")
    
    # Track loaded files to avoid dupes/overlaps if globs duplicate
    loaded_files = set()
    
    for config in FILE_CONFIGS:
        search_path = os.path.join(INPUT_DIR, config["pattern"])
        files = glob.glob(search_path)
        
        for fpath in files:
            fname = os.path.basename(fpath)
            if fname in loaded_files: continue
            loaded_files.add(fname)
            
            print(f"  Loading {fname}...")
            try:
                df = pd.read_csv(fpath, low_memory=False)
                broker_col = config["broker_col"]
                
                if broker_col not in df.columns:
                    print(f"    WARN: Missing broker col '{broker_col}' in {fname}. Skipping.")
                    continue
                
                # Standardize
                for idx, row in df.iterrows():
                    broker_raw = row.get(broker_col)
                    if pd.isna(broker_raw): continue
                    
                    record = {
                        "Target_Firm": str(broker_raw).strip(),
                        "Source_File": fname,
                        "Raw_Row_ID": idx
                    }
                    
                    # Map Contact Data
                    for src_col, dest_col in config["contact_cols"].items():
                        # Handle potential missing cols gracefully
                        val = row.get(src_col)
                        if pd.isna(val): val = ""
                        record[dest_col] = str(val).strip()
                    
                    all_rows.append(record)
                    
            except Exception as e:
                print(f"    ERROR loading {fname}: {e}")
                
    return pd.DataFrame(all_rows)

def main():
    print("--- GOLDEN MASTER PRODUCTION RUN ---")
    
    # 1. Extract Unique Firms
    print("\nSTEP 1: Extracting Reservoir...")
    df_leads = load_reservoir()
    
    if df_leads.empty:
        print("CRITICAL: No leads loaded.")
        return
    
    unique_firms = df_leads["Target_Firm"].unique()
    print(f"  Total Input Rows: {len(df_leads)}")
    print(f"  Unique Firms: {len(unique_firms)}")
    
    # 2. Resolve Territory
    print("\nSTEP 2: Territory Resolution (The Refinery - Sequential Debug)...")
    resolver = TerritoryResolver()
    
    firm_map = {} # Firm Name -> Resolution Result
    
    total = len(unique_firms)
    for idx, firm in enumerate(unique_firms):
        print(f"  Resolving [{idx+1}/{total}]: {firm}...", end="", flush=True)
        try:
            res = resolver.resolve(firm)
            firm_map[firm] = res
            print(f" DONE ({res.get('firm_state_method')})", flush=True)
            
            # Rate limit
            if res.get('firm_state_method') != 'CACHE_HIT':
                 time.sleep(0.5)
        except Exception as e:
            print(f" ERROR: {e}", flush=True)
             
    # 3. Map & Generate
    print("\nSTEP 3: Generating Hunting List...")
    
    final_rows = []
    
    in_territory_count = 0
    regional_firms_count = 0
    firm_counts = {}
    
    for idx, row in df_leads.iterrows():
        firm = row["Target_Firm"]
        res = firm_map.get(firm)
        
        if not res: continue
        
        # Filter for IN_TERRITORY
        # Also include explicit Override cases if we had them, but standard is In-Territory
        if res.get("firm_state_class") == "IN_TERRITORY":
            in_territory_count += 1
            
            # Enrich row with Territory Data
            row["Firm_State"] = res.get("firm_state", "UNKNOWN")
            row["Firm_State_Class"] = res.get("firm_state_class", "UNKNOWN")
            row["Firm_State_Method"] = res.get("firm_state_method", "UNKNOWN")
            row["Firm_State_Evidence"] = res.get("firm_state_evidence", "")
            
            final_rows.append(row)
            
            # Stats
            norm_firm = res.get("firm_name_norm")
            firm_counts[norm_firm] = firm_counts.get(norm_firm, 0) + 1

    # Save
    out_df = pd.DataFrame(final_rows)
    out_df.to_csv(OUTPUT_CSV, index=False)
    resolver._save_cache()
    
    # Metrics
    final_unique_firms = len(firm_counts)
    
    # Top 10
    sorted_firms = sorted(firm_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    
    print("\n--- RUN COMPLETE ---")
    print(f"Total Client Rows Processed: {len(df_leads)}")
    print(f"Total Unique Firms Resolved: {len(unique_firms)}")
    print(f"Final 'In-Territory' Hunting List Size: {len(out_df)}")
    print(f"Unique Firms in List: {final_unique_firms}")
    print(f"Regional Metric: {final_unique_firms} firms covered.")
    
    print("\nTop 10 Firms by Volume:")
    for f, count in sorted_firms:
        print(f"  {f}: {count}")
    
    print(f"\nSaved to: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
