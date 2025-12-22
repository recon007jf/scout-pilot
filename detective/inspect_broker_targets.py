import pandas as pd
import glob
import os

# TASK: INSPECT BROKER MAP FOR TARGETS
# GOAL: Verify if Mercer/Aon/etc exist in the 2021 data at all.

def inspect_targets():
    SPINE_ROOT = "/Users/josephlf/.gemini/antigravity/dol_spine/20_silver_standardized"
    
    print(">>> INSPECTING BROKER MAP FOR TARGETS")
    
    # Load Broker Map
    df_broker = pd.concat([pd.read_parquet(f) for f in glob.glob(os.path.join(SPINE_ROOT, "broker_providers/year=2021/*.parquet"))])
    print(f"   Total Broker Rows: {len(df_broker):,}")
    
    if "_PROV_NORM" in df_broker.columns:
        df_broker["PROVIDER_NAME_NORM"] = df_broker["_PROV_NORM"]
        
    targets = ['MERCER', 'AON', 'GALLAGHER', 'WTW', 'LOCKTON', 'MARSH', 'ALLIANT', 'HUB', 'USI']
    pat = "|".join(targets)
    
    matches = df_broker[df_broker["PROVIDER_NAME_NORM"].str.contains(pat, na=False, regex=True)]
    print(f"   Rows matching Target Regex: {len(matches):,}")
    
    if not matches.empty:
        print("\n   [Sample Matches]")
        print(matches["PROVIDER_NAME_NORM"].value_counts().head(20))
        
        # Check if these matches are for HEALTH PLANS?
        # We need to join back to 5500 to see the Welfare Code.
        # matches has ACK_ID.
        # Load 5500 2021
        df_2021 = pd.concat([pd.read_parquet(f) for f in glob.glob(os.path.join(SPINE_ROOT, "health_plans/year=2021/*.parquet"))])
        
        joined = pd.merge(matches, df_2021, left_on="ACK_ID", right_on="ACK_ID", how="inner")
        print(f"\n   Target Matches linked to 2021 5500s: {len(joined):,}")
        
        # Check Welfare Code
        health_matches = joined[joined["WELFARE_CODE"].astype(str).str.contains("4A", na=False)]
        print(f"   Target Matches that are HEALTH PLANS (4A): {len(health_matches):,}")
        
        if not health_matches.empty:
            print("\n   [Health Plan Broker Matches]")
            print(health_matches["PROVIDER_NAME_NORM"].value_counts().head(10))
    else:
        print("   NO MATCHES FOUND FOR TARGETS.")

if __name__ == "__main__":
    inspect_targets()
