import pandas as pd
import os
import re

# TASK: JAN 6 FINAL PROTOCOL (SCHEMA-VERIFIED)
# CONTEXT: Scout-AI Pilot. Generates "Jan6_Office_HitList.csv".
# PATH: /Users/josephlf/.gemini/antigravity/scratch/pilot_outputs_2021

def execute_jan6_final():
    # 1. SETUP WORKSPACE
    # NOTE: Modified path logic to handle artifact location vs output location
    ARTIFACT_DIR = "Scout_Data_Artifacts" 
    OUTPUT_DIR = "pilot_outputs_2021"
    
    print(f">>> SCOUT FINAL PROTOCOL INITIATED")
    print(f"    Artifact Source: {ARTIFACT_DIR}")
    print(f"    Output Destination: {OUTPUT_DIR}")

    # Files
    INPUT_TARGET_CSV = os.path.join(OUTPUT_DIR, "Marketing Regional Planning - 2026 - West_Mercer_AON.csv")
    DOL_5500_CSV = os.path.join(ARTIFACT_DIR, "F_5500_2021_latest.csv")
    DOL_SCHED_C_CSV = os.path.join(ARTIFACT_DIR, "F_SCH_C_PART1_ITEM1_2021_latest.csv")
    OUTPUT_FILENAME = os.path.join(OUTPUT_DIR, "Jan6_Office_HitList.csv")

    # Exclusions (Pension/401k noise)
    EXCLUDE_PLAN_TERMS = re.compile(r"(401K|401\(K\)|PENSION|RETIREMENT|DEFINED BENEFIT|SAVINGS PLAN|PROFIT SHARING)", re.IGNORECASE)

    # 2. HELPER FUNCTIONS
    def require_file(path):
        if not os.path.exists(path):
            raise FileNotFoundError(f"CRITICAL: Missing required file: {path}")

    def pick_col(df, candidates, required=True):
        cols_upper = {c.upper(): c for c in df.columns}
        for cand in candidates:
            if cand.upper() in cols_upper:
                return cols_upper[cand.upper()]
        if required:
            return None 
        return None

    def normalize_text(s):
        s = "" if pd.isna(s) else str(s)
        s = s.strip().upper()
        s = re.sub(r"[^A-Z0-9\s]", "", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def firm_pattern(firm):
        f = normalize_text(firm)
        if len(f) <= 4:
            return re.compile(rf"(?<![A-Z0-9]){re.escape(f)}(?![A-Z0-9])")
        return re.compile(re.escape(f))

    # 3. LOAD TARGET OFFICES
    def load_target_offices():
        path = INPUT_TARGET_CSV
        require_file(path)
        offices = []
        try:
            df = pd.read_csv(path, low_memory=False)
            firm_col = pick_col(df, ["FIRM", "OFFICE", "COMPETITOR", "BROKER", "FIRM NAME"], required=False)
            city_col = pick_col(df, ["CITY", "OFFICE CITY", "LOCATION"], required=False)
            state_col = pick_col(df, ["STATE", "ST", "OFFICE STATE"], required=False)

            if not firm_col or not state_col:
                print("   WARNING: Could not identify Firm/State in Input CSV. Script may fail to find specific offices.")
                return []

            for _, row in df.iterrows():
                f = normalize_text(row.get(firm_col))
                c = normalize_text(row.get(city_col))
                s = normalize_text(row.get(state_col))
                if f and s:
                   offices.append({"Target_Firm": f, "Target_City": c, "Target_State": s})
            
            print(f"   Loaded {len(offices)} Target Offices.")
            return offices
        except Exception as e:
            print(f"   CSV Error: {e}")
            return []

    # 4. MAIN EXECUTION
    print("... Loading DOL Data ...")
    
    require_file(DOL_5500_CSV)
    require_file(DOL_SCHED_C_CSV)

    # Load 5500
    f5500 = pd.read_csv(DOL_5500_CSV, low_memory=False)
    
    # Map Columns (Based on your Diagnostics)
    ack_col = pick_col(f5500, ["ACK_ID"], required=True)
    plan_num_col = pick_col(f5500, ["PLAN_NUM", "PN"], required=True)
    plan_name_col = pick_col(f5500, ["PLAN_NAME", "PLAN_NM"], required=True)
    sponsor_state_col = pick_col(f5500, ["SPONS_DFE_MAIL_US_STATE", "SPONSOR_US_STATE", "SPONSOR_STATE"], required=True)
    sponsor_city_col = pick_col(f5500, ["SPONS_DFE_MAIL_US_CITY", "SPONSOR_CITY"], required=False)
    employer_col = pick_col(f5500, ["SPONSOR_DFE_NAME", "SPONSOR_NAME"], required=False)
    
    # Funding/Benefit Columns (VERIFIED)
    fund_ins_col = pick_col(f5500, ["FUNDING_INSURANCE_IND"], required=False)
    fund_trust_col = pick_col(f5500, ["FUNDING_TRUST_IND"], required=False)
    fund_gen_col = pick_col(f5500, ["FUNDING_GEN_ASSET_IND"], required=False)
    welfare_code_col = pick_col(f5500, ["TYPE_WELFARE_BNFT_CODE"], required=False)

    print(f"   Key Cols Mapped: State={sponsor_state_col}, City={sponsor_city_col}")
    print(f"   Funding Cols Mapped: Ins={fund_ins_col}, Trust={fund_trust_col}, Gen={fund_gen_col}")

    # FILTER 1: HEALTH ONLY (5xx + No Pension + Code 4A)
    plan_num_str = f5500[plan_num_col].astype(str)
    plan_name_str = f5500[plan_name_col].astype(str)
    
    # Logic: PlanNum 5xx AND Name Safe AND (Optional: Code contains 4A)
    health_mask = (
        plan_num_str.str.startswith("5", na=False) &
        (~plan_name_str.str.contains(EXCLUDE_PLAN_TERMS, na=False))
    )
    
    # If we have the welfare code column, strict check for "4A" (Health)
    if welfare_code_col:
        health_mask = health_mask & (f5500[welfare_code_col].astype(str).str.contains("4A", na=False))

    health = f5500[health_mask].copy()
    print(f"   Health Plans Isolated: {len(health)}")

    # FILTER 2: SELF-FUNDED CHECK
    # Logic: Keep if TRUST=1 OR GEN_ASSET=1. (This confirms "Self-Funded" components exist)
    # Exclude if ONLY INSURANCE=1 (Fully Insured).
    
    print("   Applying Self-Funded Filter (GenAsset=1 OR Trust=1)...")
    
    def check_sf(row):
        # Default to False if cols missing
        ins = int(row.get(fund_ins_col, 0)) if fund_ins_col and pd.notna(row.get(fund_ins_col)) else 0
        trust = int(row.get(fund_trust_col, 0)) if fund_trust_col and pd.notna(row.get(fund_trust_col)) else 0
        gen = int(row.get(fund_gen_col, 0)) if fund_gen_col and pd.notna(row.get(fund_gen_col)) else 0
        
        is_sf = (trust == 1) or (gen == 1)
        is_fully_insured = (ins == 1) and (trust == 0) and (gen == 0)
        
        return is_sf and not is_fully_insured

    health["_IS_SELF_FUNDED"] = health.apply(check_sf, axis=1)
    
    sf_count = health["_IS_SELF_FUNDED"].sum()
    print(f"   Self-Funded Candidates Found: {sf_count}")
    
    # Keep only self funded
    health = health[health["_IS_SELF_FUNDED"] == True].copy()

    # Load Schedule C & Merge
    c = pd.read_csv(DOL_SCHED_C_CSV, low_memory=False)
    c_ack = pick_col(c, ["ACK_ID"], required=True)
    prov_name_col = pick_col(c, ["PROVIDER_NAME", "PROVIDER_ELIGIBLE_NAME", "SRVC_PROV_NAME"], required=True)
    
    health[ack_col] = health[ack_col].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    c[c_ack] = c[c_ack].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)

    merged = pd.merge(health, c[[c_ack, prov_name_col]], left_on=ack_col, right_on=c_ack, how="inner")
    print(f"   Merged Rows (Self-Funded + Provider): {len(merged)}")

    # Pre-calc normalized columns
    merged["_PROV_NORM"] = merged[prov_name_col].astype(str).map(normalize_text)
    merged["_STATE_NORM"] = merged[sponsor_state_col].astype(str).map(normalize_text)
    if sponsor_city_col:
        merged["_CITY_NORM"] = merged[sponsor_city_col].astype(str).map(normalize_text)
    else:
        merged["_CITY_NORM"] = ""

    # Lives
    lives_col = None
    for cand in ["TOT_ACT_PARTCP_CNT", "TOT_PARTCP_CNT", "PARTICIPANTS"]:
        if cand in merged.columns:
            lives_col = cand
            break

    # 5. MATCHING (OFFICE LEVEL)
    target_offices = load_target_offices()
    matches = []
    
    if not target_offices:
        print("   No offices to match. Exiting.")
        return

    print(f"   Scanning {len(target_offices)} offices against {len(merged)} plans...")

    for office in target_offices:
        t_firm = office["Target_Firm"]
        t_city = office["Target_City"]
        t_state = office["Target_State"]
        
        # State Gate
        state_subset = merged[merged["_STATE_NORM"] == t_state]
        if state_subset.empty: continue
            
        # Firm Gate
        pat = firm_pattern(t_firm)
        firm_hits = state_subset[state_subset["_PROV_NORM"].str.contains(pat, na=False)]
        
        for _, row in firm_hits.iterrows():
            conf = "MEDIUM (State)"
            if t_city and row["_CITY_NORM"] == t_city:
                conf = "HIGH (City+State)"
                
            lives = 0
            if lives_col:
                try: lives = int(float(row[lives_col]))
                except: pass
            
            matches.append({
                "Target_Firm": t_firm,
                "Target_City": t_city,
                "Target_State": t_state,
                "Found_Provider": row[prov_name_col],
                "Employer": row.get(employer_col, ""),
                "Plan": row.get(plan_name_col, ""),
                "Lives": lives,
                "Confidence": conf,
                "SelfFunded": True
            })

    # 6. OUTPUT
    results = pd.DataFrame(matches)
    if not results.empty:
        results = results[results["Lives"] >= 100]
        results = results.sort_values(by=["Confidence", "Lives"], ascending=[True, False])
        results = results.drop_duplicates(subset=["Employer", "Plan", "Found_Provider"])
        
    results.to_csv(OUTPUT_FILENAME, index=False)
    
    print("-" * 30)
    print("JAN 6 HIT LIST GENERATED")
    print(f"Total Self-Funded Matches: {len(results)}")
    print(f"Saved to: {OUTPUT_FILENAME}")
    print("-" * 30)

if __name__ == "__main__":
    execute_jan6_final()
