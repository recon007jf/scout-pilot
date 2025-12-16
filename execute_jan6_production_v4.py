import pandas as pd
import os
import re

# TASK: JAN 6 PRODUCTION PROTOCOL (V4 - FINAL LOCK)
# CONTEXT: Scout-AI Pilot. Generates "Jan6_Master_HitList.csv"
# FIXES: Hard stop on Lives column, Data Quality Logging.
# ADAPTATION: Corrected paths for Artifacts vs Output Directory.

def execute_jan6_production_v4():
    # 1. SETUP WORKSPACE
    ARTIFACT_DIR = "Scout_Data_Artifacts"
    OUTPUT_DIR = "pilot_outputs_2021"
    
    print(f">>> SCOUT PRODUCTION PROTOCOL V4 INITIATED")
    print(f"    Artifact Source: {os.path.abspath(ARTIFACT_DIR)}")
    print(f"    Output Destination: {os.path.abspath(OUTPUT_DIR)}")

    # Files
    INPUT_TARGET_CSV = os.path.join(OUTPUT_DIR, "Marketing Regional Planning - 2026 - West_Mercer_AON.csv")
    DOL_5500_CSV = os.path.join(ARTIFACT_DIR, "F_5500_2021_latest.csv")
    DOL_SCHED_C_CSV = os.path.join(ARTIFACT_DIR, "F_SCH_C_PART1_ITEM1_2021_latest.csv")
    OUTPUT_FILENAME = os.path.join(OUTPUT_DIR, "Jan6_Master_HitList.csv")

    # Exclusions
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
            # Construct meaningful error
            available = sorted(list(cols_upper.keys()))[:10]
            raise KeyError(f"CRITICAL SCHEMA ERROR: Could not find any of: {candidates}. Available candidates sample: {available}")
        return None

    def normalize_text(s):
        s = "" if pd.isna(s) else str(s)
        s = s.strip().upper()
        s = re.sub(r"[^A-Z0-9\s]", "", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def get_firm_pattern(firm):
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
            print(f"   Input CSV Columns: {list(df.columns)}")
            
            # Geo/Firm Cols
            firm_col = pick_col(df, ["FIRM", "OFFICE", "COMPETITOR", "BROKER", "FIRM NAME"], required=True)
            city_col = pick_col(df, ["CITY", "OFFICE CITY", "LOCATION"], required=False)
            state_col = pick_col(df, ["STATE", "ST", "OFFICE STATE"], required=True)
            
            # Contact Cols
            first_name_col = pick_col(df, ["FIRST NAME", "FIRST", "FNAME", "CONTACT FIRST"], required=False)
            last_name_col = pick_col(df, ["LAST NAME", "LAST", "LNAME", "CONTACT LAST"], required=False)
            email_col = pick_col(df, ["EMAIL", "E-MAIL", "CONTACT EMAIL"], required=False)
            role_col = pick_col(df, ["ROLE", "TITLE", "POSITION"], required=False)

            for _, row in df.iterrows():
                f = normalize_text(row.get(firm_col))
                c = normalize_text(row.get(city_col))
                s = normalize_text(row.get(state_col))
                
                # Contact Data
                contact_first = str(row.get(first_name_col, "")) if first_name_col else ""
                contact_last = str(row.get(last_name_col, "")) if last_name_col else ""
                contact_email = str(row.get(email_col, "")) if email_col else ""
                contact_role = str(row.get(role_col, "")) if role_col else ""

                if f and s:
                   offices.append({
                       "Target_Firm": f, 
                       "Target_City": c, 
                       "Target_State": s,
                       "Contact_Name": f"{contact_first} {contact_last}".strip(),
                       "Contact_Email": contact_email,
                       "Contact_Role": contact_role,
                       "Firm_Pattern": get_firm_pattern(f)
                   })
            
            print(f"   Loaded {len(offices)} Target Offices with Contact Data.")
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
    
    # Map Columns
    ack_col = pick_col(f5500, ["ACK_ID"], required=True)
    plan_num_col = pick_col(f5500, ["PLAN_NUM", "PN"], required=True)
    plan_name_col = pick_col(f5500, ["PLAN_NAME", "PLAN_NM"], required=True)
    sponsor_state_col = pick_col(f5500, ["SPONS_DFE_MAIL_US_STATE", "SPONSOR_US_STATE", "SPONSOR_STATE"], required=True)
    employer_col = pick_col(f5500, ["SPONSOR_DFE_NAME", "SPONSOR_NAME"], required=False)
    
    # Funding/Benefit Columns
    fund_ins_col = pick_col(f5500, ["FUNDING_INSURANCE_IND"], required=False)
    fund_trust_col = pick_col(f5500, ["FUNDING_TRUST_IND"], required=False)
    fund_gen_col = pick_col(f5500, ["FUNDING_GEN_ASSET_IND"], required=False)
    welfare_code_col = pick_col(f5500, ["TYPE_WELFARE_BNFT_CODE"], required=False)

    # FILTER 1: HEALTH ONLY
    plan_num_str = f5500[plan_num_col].astype(str)
    plan_name_str = f5500[plan_name_col].astype(str)
    
    health_mask = (
        plan_num_str.str.startswith("5", na=False) &
        (~plan_name_str.str.contains(EXCLUDE_PLAN_TERMS, na=False))
    )
    if welfare_code_col:
        health_mask = health_mask & (f5500[welfare_code_col].astype(str).str.contains("4A", na=False))

    health = f5500[health_mask].copy()
    print(f"   Health Plans Isolated: {len(health)}")

    # FILTER 2: SELF-FUNDED (SAFE NUMERIC PARSING)
    print("   Applying Self-Funded Filter...")
    def check_sf(row):
        # Safe numeric conversion
        ins_raw = pd.to_numeric(row.get(fund_ins_col), errors='coerce') if fund_ins_col else 0
        trust_raw = pd.to_numeric(row.get(fund_trust_col), errors='coerce') if fund_trust_col else 0
        gen_raw = pd.to_numeric(row.get(fund_gen_col), errors='coerce') if fund_gen_col else 0
        
        ins = int(ins_raw) if pd.notna(ins_raw) else 0
        trust = int(trust_raw) if pd.notna(trust_raw) else 0
        gen = int(gen_raw) if pd.notna(gen_raw) else 0

        is_sf = (trust == 1) or (gen == 1)
        is_fully_insured = (ins == 1) and (trust == 0) and (gen == 0)
        return is_sf and not is_fully_insured

    health["_IS_SELF_FUNDED"] = health.apply(check_sf, axis=1)
    health = health[health["_IS_SELF_FUNDED"] == True].copy()
    print(f"   Self-Funded Candidates Found: {len(health)}")

    # Load Schedule C & Merge
    c = pd.read_csv(DOL_SCHED_C_CSV, low_memory=False)
    c_ack = pick_col(c, ["ACK_ID"], required=True)
    
    # Provider Location Columns
    prov_name_col = pick_col(c, ["PROVIDER_NAME", "PROVIDER_ELIGIBLE_NAME", "SRVC_PROV_NAME"], required=True)
    prov_city_col = pick_col(c, ["PROVIDER_ELIGIBLE_US_CITY", "SRVC_PROV_US_CITY", "PROVIDER_CITY"], required=False)
    prov_state_col = pick_col(c, ["PROVIDER_ELIGIBLE_US_STATE", "SRVC_PROV_US_STATE", "PROVIDER_STATE"], required=False)

    print(f"   Using Provider Location Cols: City={prov_city_col}, State={prov_state_col}")

    # Convert Keys
    health[ack_col] = health[ack_col].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    c[c_ack] = c[c_ack].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)

    # Merge
    # Only keep C rows that have valid Provider Name
    c = c.dropna(subset=[prov_name_col])

    merge_cols = [c_ack, prov_name_col]
    if prov_city_col: merge_cols.append(prov_city_col)
    if prov_state_col: merge_cols.append(prov_state_col)

    merged = pd.merge(health, c[merge_cols], left_on=ack_col, right_on=c_ack, how="inner")
    print(f"   Merged Rows: {len(merged)}")

    # Pre-calc normalized columns
    merged["_PROV_NORM"] = merged[prov_name_col].astype(str).map(normalize_text)
    merged["_SPONSOR_STATE_NORM"] = merged[sponsor_state_col].astype(str).map(normalize_text)
    
    if prov_city_col:
        merged["_PROV_CITY_NORM"] = merged[prov_city_col].astype(str).map(normalize_text)
    
    if prov_state_col:
        merged["_PROV_STATE_NORM"] = merged[prov_state_col].astype(str).map(normalize_text)
    
    # Data Quality Log
    if prov_state_col:
        valid_prov_states = merged["_PROV_STATE_NORM"].replace("", pd.NA).count()
        print(f"   Data Quality: {valid_prov_states} / {len(merged)} rows have valid Provider State data.")

    # Lives (CRITICAL CHECK)
    lives_col = None
    for cand in ["TOT_ACT_PARTCP_CNT", "TOT_PARTCP_CNT", "PARTICIPANTS"]:
        if cand in merged.columns:
            lives_col = cand
            break
            
    if lives_col is None:
        raise KeyError("CRITICAL SCHEMA ERROR: No Lives column found (TOT_ACT_PARTCP_CNT / TOT_PARTCP_CNT / PARTICIPANTS). Cannot rank by value proxy.")
    
    print(f"   Using Lives Column: {lives_col}")

    # 5. MATCHING (STATE-FIRST OPTIMIZATION)
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
        pat = office["Firm_Pattern"]
        
        subset = pd.DataFrame()
        source_used = "NONE"

        # Try Provider State First (Best Quality)
        if prov_state_col and "_PROV_STATE_NORM" in merged.columns:
             prov_subset = merged[merged["_PROV_STATE_NORM"] == t_state]
             if not prov_subset.empty:
                 subset = prov_subset
                 source_used = "PROVIDER_STATE"
        
        # Fallback to Sponsor State (Proxy)
        if subset.empty:
             subset = merged[merged["_SPONSOR_STATE_NORM"] == t_state]
             if not subset.empty:
                 source_used = "SPONSOR_STATE"
        
        if subset.empty:
            continue

        # FIRM GATE
        firm_hits = subset[subset["_PROV_NORM"].str.contains(pat, na=False)]
        
        for _, row in firm_hits.iterrows():
            conf = "LOW"
            
            # Refine Confidence
            if source_used == "PROVIDER_STATE":
                row_city = row["_PROV_CITY_NORM"] if prov_city_col and "_PROV_CITY_NORM" in row else ""
                if t_city and row_city == t_city:
                    conf = "HIGH (Provider Address Match)"
                else:
                    conf = "MEDIUM (Provider State Match)"
            
            elif source_used == "SPONSOR_STATE":
                conf = "MEDIUM (Sponsor State Proxy)"

            # Lives Extraction (Already Checked Existence)
            try: lives = int(float(row[lives_col]))
            except: lives = 0
            
            found_city = row[prov_city_col] if prov_city_col and pd.notna(row[prov_city_col]) else "Unknown"

            matches.append({
                "Target_Broker_Firm": t_firm,
                "Target_Broker_Office": f"{t_city}, {t_state}",
                "Broker_Contact_Name": office["Contact_Name"],    
                "Broker_Contact_Email": office["Contact_Email"],
                "Broker_Role": office["Contact_Role"],
                "Client_Company": row.get(employer_col, ""),
                "Plan_Name": row.get(plan_name_col, ""),
                "Member_Count (Value Proxy)": lives,
                "Match_Confidence": conf,
                "Location_Source_Used": source_used,
                "Provider_City_Found": found_city,
                "Self_Funded_Flag": True
            })

    # 6. OUTPUT
    results = pd.DataFrame(matches)
    if not results.empty:
        results = results[results["Member_Count (Value Proxy)"] >= 100]
        results = results.sort_values(by=["Member_Count (Value Proxy)"], ascending=False)
        results = results.drop_duplicates(subset=["Client_Company", "Plan_Name", "Target_Broker_Firm"])
        
    results.to_csv(OUTPUT_FILENAME, index=False)
    
    print("-" * 30)
    print("JAN 6 MASTER HIT LIST GENERATED")
    print(f"Total Matches: {len(results)}")
    print(f"Saved to: {OUTPUT_FILENAME}")
    print("-" * 30)

if __name__ == "__main__":
    execute_jan6_production_v4()
