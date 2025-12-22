import os
import pandas as pd
import glob

# TASK: DEBUG TIME TRAVEL MATCH RATE
# GOAL: Inspect EIN/PlanNum overlap and Broker String Matching.

def debug_detective():
    print(">>> DEBUGGING TIME TRAVEL LOGIC")
    
    SPINE_ROOT = "/Users/josephlf/.gemini/antigravity/dol_spine/20_silver_standardized"
    
    # 1. LOAD DATA SUBSETS
    print("   Loading Data...")
    df_2021 = pd.concat([pd.read_parquet(f) for f in glob.glob(os.path.join(SPINE_ROOT, "health_plans/year=2021/*.parquet"))])
    df_2023 = pd.concat([pd.read_parquet(f) for f in glob.glob(os.path.join(SPINE_ROOT, "health_plans/year=2023/*.parquet"))])
    df_broker = pd.concat([pd.read_parquet(f) for f in glob.glob(os.path.join(SPINE_ROOT, "broker_providers/year=2021/*.parquet"))])
    
    # Filter 2023 for Self-Funded
    df_2023 = df_2023[df_2023["IS_SELF_FUNDED"] == True]

    print(f"   2021 Anchor Plans: {len(df_2021):,}")
    print(f"   2023 Target Plans: {len(df_2023):,}")
    print(f"   2021 Broker Rows:  {len(df_broker):,}")
    
    # 2. CHECK FORMATTING
    print("\n   [Sample IDs]")
    print(f"   2021: EIN='{df_2021['EIN'].iloc[0]}' PN='{df_2021['PLAN_NUM'].iloc[0]}'")
    print(f"   2023: EIN='{df_2023['EIN'].iloc[0]}' PN='{df_2023['PLAN_NUM'].iloc[0]}'")
    
    # 3. CHECK OVERLAP
    # Set based intersection
    eins_2021 = set(df_2021['EIN'])
    eins_2023 = set(df_2023['EIN'])
    
    overlap_eins = eins_2021.intersection(eins_2023)
    print(f"\n   [Overlap Stats]")
    print(f"   EIN Overlap: {len(overlap_eins):,} (Companies present in both years)")
    
    # STRICT JOIN (EIN + PN)
    df_2021["LINK_KEY"] = df_2021["EIN"].astype(str) + "_" + df_2021["PLAN_NUM"].astype(str).str.zfill(3)
    df_2023["LINK_KEY"] = df_2023["EIN"].astype(str) + "_" + df_2023["PLAN_NUM"].astype(str).str.zfill(3)
    
    links_2021 = set(df_2021["LINK_KEY"])
    links_2023 = set(df_2023["LINK_KEY"])
    
    overlap_links = links_2021.intersection(links_2023)
    print(f"   EIN+PN Overlap: {len(overlap_links):,} (Plans present in both years)")
    
    # 4. BROKER MATCH DIAGNOSTIC
    # How many of the OVERLAPPING plans have a Broker Match in 2021?
    linked_2021 = df_2021[df_2021["LINK_KEY"].isin(overlap_links)]
    
    # Join to Broker
    # df_broker key is ACK_ID
    # linked_2021 key is ACK_ID
    
    anchor_with_broker = pd.merge(linked_2021, df_broker, left_on="ACK_ID", right_on="ACK_ID", how="inner")
    print(f"   Anchor Plans with Broker Record: {len(anchor_with_broker):,}")
    
    # Check Whitelist Frequency
    if "_PROV_NORM" in anchor_with_broker.columns:
        col = "_PROV_NORM"
    else:
        col = "PROVIDER_NAME"
        
    print(f"\n   [Top 10 Brokers in Linked Set]")
    print(anchor_with_broker[col].value_counts().head(10))

if __name__ == "__main__":
    debug_detective()
