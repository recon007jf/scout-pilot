import pandas as pd
import os
import re

# TASK: JAN 6 WESTERN SWEEP (V9 - DIAMOND PRODUCTION HARDENED)
# GOAL: Broad lead volume, rigid schemas, pipeline safety.
# CHANGES: 
#   1. Traceback preservation (bare raise).
#   2. Safe Write in finally block (ensures errors aren't masked).
#   3. Preserves strict Hard Fails for data quality issues.
# AG FIXES:
#   - Paths (Scout_Data_Artifacts)
#   - Encoding ('cp1252' for Input CSV)
#   - Lives Column Candidates (Added TOT_PARTCP_BOY_CNT)

def execute_jan6_volume_sweep_v9():
    # 1. SETUP WORKSPACE & CONSTANTS
    OUTPUT_DIR = "pilot_outputs_2021"
    ARTIFACT_DIR = "Scout_Data_Artifacts"
    
    # User's logic uses BASE_PATH for output
    BASE_PATH = OUTPUT_DIR 
    
    print(f">>> SCOUT VOLUME SWEEP V9 INITIATED")
    print(f"    Artifact Source: {os.path.abspath(ARTIFACT_DIR)}")
    print(f"    Output Destination: {os.path.abspath(BASE_PATH)}")

    OUTPUT_SUMMARY = "Jan6_Western_HitList_Summary.csv"
    OUTPUT_RAW = "Jan6_Western_HitList_RAW_AUDIT.csv"

    WESTERN_STATES = ["CA", "WA", "OR", "NV", "AZ", "ID", "MT", "WY", "CO", "NM", "UT", "AK", "HI"]
    EXCLUDE_PLAN_TERMS = re.compile(r"(401K|401\(K\)|PENSION|RETIREMENT|DEFINED BENEFIT|SAVINGS PLAN|PROFIT SHARING)", re.IGNORECASE)
    FIRM_ALLOWLIST = ["MERCER", "AON", "ALLIANT", "GALLAGHER", "HUB", "WTW", "WILLIS", "LOCKTON", "USI", "MARSH"]
    
    # OUTPUT SCHEMAS (Contract)
    RAW_COLUMNS = ["Target_Broker", "Found_Provider", "Client_Company", "Client_State", "Provider_State", "Member_Count", "Suggested_Contact", "Contact_Email", "Self_Funded", "West_Gate_Source", "ACK_ID", "Distinct_Filings_Count"]
    SUMMARY_COLUMNS = ["Target_Broker", "Found_Provider", "Client_Company", "Client_State", "Provider_State", "Member_Count", "Suggested_Contact", "Contact_Email", "Self_Funded", "West_Gate_Source", "Distinct_Filings_Count"]

    # Initialize Empty DFs (Default State)
    df_matches_out = pd.DataFrame(columns=RAW_COLUMNS)
    df_summary_out = pd.DataFrame(columns=SUMMARY_COLUMNS)

    # 2. HELPER FUNCTIONS
    def require_file(path):
        if not os.path.exists(path):
            raise FileNotFoundError(f"CRITICAL: Missing required file: {path}")

    def normalize_text(s):
        s = "" if pd.isna(s) else str(s)
        s = s.strip().upper()
        # Replace non-alphanumeric with SPACE to preserve word boundaries
        s = re.sub(r"[^A-Z0-9\s]", " ", s) 
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def map_columns(path, required_map, optional_map):
        # Read header only
        # AG FIX: Encoding check
        try:
             df_empty = pd.read_csv(path, nrows=0, low_memory=False)
        except UnicodeDecodeError:
             df_empty = pd.read_csv(path, nrows=0, low_memory=False, encoding='cp1252')
             
        cols_upper = {c.upper(): c for c in df_empty.columns}
        
        final_map = {}
        missing_required = []

        for internal_key, candidates in required_map.items():
            found = None
            for cand in candidates:
                if cand.upper() in cols_upper:
                    found = cols_upper[cand.upper()]
                    break
            if found:
                final_map[internal_key] = found
            else:
                missing_required.append(internal_key)
        
        if missing_required:
            raise KeyError(f"CRITICAL: Missing required columns in {os.path.basename(path)}: {missing_required}")

        for internal_key, candidates in optional_map.items():
            found = None
            for cand in candidates:
                if cand.upper() in cols_upper:
                    found = cols_upper[cand.upper()]
                    break
            if found:
                final_map[internal_key] = found
        
        return final_map

    # 3. MAIN LOGIC (Wrapped in Try/Except/Finally)
    try:
        # Files
        INPUT_TARGET_CSV = "Marketing Regional Planning - 2026 - West_Mercer_AON.csv"
        DOL_5500_CSV = "F_5500_2021_latest.csv"
        DOL_SCHED_C_CSV = "F_SCH_C_PART1_ITEM1_2021_latest.csv"

        p_target = os.path.join(BASE_PATH, INPUT_TARGET_CSV)
        p_5500 = os.path.join(ARTIFACT_DIR, DOL_5500_CSV)   # AG FIX
        p_c = os.path.join(ARTIFACT_DIR, DOL_SCHED_C_CSV)   # AG FIX

        require_file(p_target)
        require_file(p_5500)
        require_file(p_c)

        # --- LOAD TARGETS ---
        print("... Loading Targets ...")
        # AG FIX: Encoding
        df_t = pd.read_csv(p_target, low_memory=False, encoding='cp1252', on_bad_lines='skip')
        cols_t = {c.upper(): c for c in df_t.columns}
        
        def get_c(candidates):
            for c in candidates: 
                if c.upper() in cols_t: return cols_t[c.upper()]
            return None

        # MAPPING FIX: Account -> Firm, Location -> City, State
        # We need to reuse the robust logic if possible, or stick to simple columns if file is fixed.
        # The user provided strict columns: FIRM/STATE.
        # But 'Marketing Regional Planning' used ACCOUNT/LOCATION in previous steps.
        # Let's add ACCOUNT and LOCATION to the candidates list to match previous verified logic.
        
        firm_col = get_c(["FIRM", "OFFICE", "COMPETITOR", "BROKER", "FIRM NAME", "ACCOUNT"])
        state_col = get_c(["STATE", "ST", "OFFICE STATE"])
        loc_col = get_c(["LOCATION"]) # fallback
        
        first = get_c(["FIRST NAME", "FIRST"])
        last = get_c(["LAST NAME", "LAST"])
        email = get_c(["EMAIL", "CONTACT EMAIL", "ALT EMAIL"])
        name_col = get_c(["NAME", "CONTACT"])
        
        if not firm_col: raise KeyError("Target CSV missing Firm column")
        # if not state_col: raise KeyError("Target CSV missing State column") # Relax for now, handle in loop

        target_firms = set()
        contact_map = {} 
        
        for _, row in df_t.iterrows():
            raw_firm = normalize_text(row.get(firm_col))
            matched_major = None
            for major in FIRM_ALLOWLIST:
                if major in raw_firm:
                    matched_major = major
                    break
            
            # Geo Parsing
            s_raw = ""
            if state_col and pd.notna(row.get(state_col)):
                s_raw = normalize_text(row.get(state_col))
            elif loc_col and pd.notna(row.get(loc_col)):
                 # Parse "City, State" from Location
                    val = str(row.get(loc_col, ""))
                    if "," in val:
                        parts = val.split(",")
                        s_raw = normalize_text(parts[1])
                    else:
                        clean = normalize_text(val)
                        if len(clean) == 2: s_raw = clean
                        else: s_raw = clean

            if matched_major:
                target_firms.add(matched_major)
                
                # Contact Logic
                c_name = ""
                if name_col and pd.notna(row.get(name_col)):
                    c_name = str(row.get(name_col)).strip()
                else:
                    f = row.get(first, "")
                    l = row.get(last, "")
                    c_name = f"{f} {l}".strip()

                c_email = str(row.get(email, ""))
                
                if c_name:
                    # Key by (Major_Firm, State)
                    contact_map[(matched_major, s_raw)] = {"Name": c_name, "Email": c_email}
        
        print(f"   Active Targets: {sorted(list(target_firms))}")

        # --- LOAD 5500 ---
        print("... Mapping 5500 Columns ...")
        req_5500 = {
            "ACK_ID": ["ACK_ID"],
            "PLAN_NAME": ["PLAN_NAME", "PLAN_NM"],
            "SPONSOR_STATE": ["SPONS_DFE_MAIL_US_STATE", "SPONSOR_US_STATE", "SPONSOR_STATE"],
            "WELFARE_CODE": ["TYPE_WELFARE_BNFT_CODE"],
            "LIVES": ["TOT_ACT_PARTCP_CNT", "TOT_PARTCP_CNT", "PARTICIPANTS", "TOT_PARTCP_BOY_CNT", "TOT_ACTIVE_PARTCP_CNT"] # AG FIX
        }
        opt_5500 = {
            "EMPLOYER": ["SPONSOR_DFE_NAME", "SPONSOR_NAME"],
            "INS_IND": ["FUNDING_INSURANCE_IND"],
            "TRUST_IND": ["FUNDING_TRUST_IND"],
            "GEN_IND": ["FUNDING_GEN_ASSET_IND"]
        }
        col_map_5500 = map_columns(p_5500, req_5500, opt_5500)
        
        print(f"   Loading 5500 (Optimized)...")
        f5500 = pd.read_csv(p_5500, usecols=list(col_map_5500.values()), low_memory=False)

        # Filter Health
        wc = col_map_5500["WELFARE_CODE"]
        pn = col_map_5500["PLAN_NAME"]
        health_mask = (
            f5500[wc].astype(str).str.contains("4A", na=False) &
            (~f5500[pn].astype(str).str.contains(EXCLUDE_PLAN_TERMS, na=False))
        )
        health = f5500[health_mask].copy()

        # Filter Self-Funded
        ins_c = col_map_5500.get("INS_IND")
        trust_c = col_map_5500.get("TRUST_IND")
        gen_c = col_map_5500.get("GEN_IND")
        
        if not (trust_c and gen_c):
            raise KeyError(f"CRITICAL: Missing Funding Trust/Gen columns. Cannot determine self-funded status.")

        def check_sf(row):
            ins = int(pd.to_numeric(row.get(ins_c), errors='coerce')) if ins_c and pd.notna(row.get(ins_c)) else 0
            trust = int(pd.to_numeric(row.get(trust_c), errors='coerce')) if trust_c and pd.notna(row.get(trust_c)) else 0
            gen = int(pd.to_numeric(row.get(gen_c), errors='coerce')) if gen_c and pd.notna(row.get(gen_c)) else 0
            return ((trust == 1) or (gen == 1)) and not ((ins == 1) and (trust == 0) and (gen == 0))

        health["_IS_SELF_FUNDED"] = health.apply(check_sf, axis=1)
        health = health[health["_IS_SELF_FUNDED"] == True].copy()
        print(f"   [Diagnostic] Self-Funded Health Plans: {len(health)}")

        # --- LOAD SCHED C ---
        print("... Mapping Schedule C Columns ...")
        req_c = {
            "ACK_ID": ["ACK_ID"],
            "PROV_NAME": ["PROVIDER_NAME", "PROVIDER_ELIGIBLE_NAME", "SRVC_PROV_NAME"]
        }
        opt_c = {
            "PROV_STATE": ["PROVIDER_ELIGIBLE_US_STATE", "SRVC_PROV_US_STATE", "PROVIDER_STATE"]
        }
        col_map_c = map_columns(p_c, req_c, opt_c)
        
        if "PROV_STATE" not in col_map_c:
            print("\n   WARNING: Provider State column MISSING. Provider-based geo-gating disabled.\n")

        print(f"   Loading Schedule C (Optimized)...")
        c = pd.read_csv(p_c, usecols=list(col_map_c.values()), low_memory=False)

        # --- MERGE ---
        ack_5500 = col_map_5500["ACK_ID"]
        ack_c = col_map_c["ACK_ID"]
        health[ack_5500] = health[ack_5500].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
        c[ack_c] = c[ack_c].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
        
        merged = pd.merge(health, c, left_on=ack_5500, right_on=ack_c, how="inner")
        print(f"   [Diagnostic] Merged Rows: {len(merged)}")

        # --- MATCHING ---
        s_state_col = col_map_5500["SPONSOR_STATE"]
        p_name_col = col_map_c["PROV_NAME"]
        p_state_col = col_map_c.get("PROV_STATE")
        emp_col = col_map_5500.get("EMPLOYER")
        lives_col_real = col_map_5500["LIVES"]

        merged["_PROV_NORM"] = merged[p_name_col].astype(str).map(normalize_text)
        merged["_SPONSOR_STATE_NORM"] = merged[s_state_col].astype(str).map(normalize_text)
        merged["_PROV_STATE_NORM"] = merged[p_state_col].astype(str).map(normalize_text) if p_state_col else ""

        print(f"   Scanning...")
        patterns = {}
        for firm in target_firms:
            # Simple word boundary regex for majors
            f_clean = re.escape(firm)
            patterns[firm] = re.compile(rf"\b{f_clean}\b")

        matches = []
        for _, row in merged.iterrows():
            s_state = row["_SPONSOR_STATE_NORM"]
            p_state = row["_PROV_STATE_NORM"]
            
            gate_source = "NONE"
            if s_state in WESTERN_STATES: gate_source = "SPONSOR_STATE"
            elif p_state in WESTERN_STATES: gate_source = "PROVIDER_STATE"
            if gate_source == "NONE": continue

            p_name = row["_PROV_NORM"]
            for firm, pat in patterns.items():
                if pat.search(p_name):
                    contact = {"Name": "", "Email": ""}
                    # Try contact mapping (Firm + State)
                    if (firm, p_state) in contact_map: contact = contact_map[(firm, p_state)]
                    elif (firm, s_state) in contact_map: contact = contact_map[(firm, s_state)]
                    
                    try: lives = int(float(row[lives_col_real]))
                    except: lives = 0
                    
                    matches.append({
                        "Target_Broker": firm,
                        "Found_Provider": row[p_name_col],
                        "Client_Company": row.get(emp_col, ""),
                        "Client_State": s_state,
                        "Provider_State": p_state,
                        "Member_Count": lives,
                        "Suggested_Contact": contact["Name"],
                        "Contact_Email": contact["Email"],
                        "Self_Funded": True,
                        "West_Gate_Source": gate_source,
                        "ACK_ID": row[ack_5500]
                    })
                    break 
        
        # --- SUCCESS UPDATE ---
        if matches:
            df_matches = pd.DataFrame(matches)
            df_matches = df_matches[df_matches["Member_Count"] >= 100]
            
            df_matches["Distinct_Filings_Count"] = df_matches.groupby(["Client_Company", "Target_Broker"])["ACK_ID"].transform("nunique")
            
            df_sum_temp = df_matches.sort_values(by="Member_Count", ascending=False)
            df_sum_temp = df_sum_temp.drop_duplicates(subset=["Client_Company", "Target_Broker"])
            
            # Enforce Column Order
            df_matches_out = df_matches[RAW_COLUMNS]
            df_summary_out = df_sum_temp[SUMMARY_COLUMNS]

    except Exception as e:
        print(f"\n!!! SCRIPT FAILED: {e} !!!\n")
        raise # Keeps original traceback for debugging
    
    finally:
        # ALWAYS WRITE OUTPUTS
        raw_path = os.path.join(BASE_PATH, OUTPUT_RAW)
        sum_path = os.path.join(BASE_PATH, OUTPUT_SUMMARY)
        
        try:
            os.makedirs(BASE_PATH, exist_ok=True)
            df_matches_out.to_csv(raw_path, index=False)
            df_summary_out.to_csv(sum_path, index=False)
        except Exception as write_err:
             print(f"\n!!! OUTPUT WRITE FAILED: {write_err} !!!\n")
             # Do not re-raise here to avoid masking the original error
        
        print("-" * 30)
        print(f"SWEEP TERMINATED.")
        print(f"Raw Audit Saved: {raw_path} ({len(df_matches_out)} rows)")
        print(f"Summary Saved: {sum_path} ({len(df_summary_out)} rows)")
        print("-" * 30)

if __name__ == "__main__":
    execute_jan6_volume_sweep_v9()
