"""
SCRIPT: build_office_intel_report.py
PURPOSE: Execute Scout Pilot - Match BenefitFlow Contacts to Western Broker Map (Office Registry).
VERSION: 1.0 (Strict / Conservative)
"""

import os
import sys
import duckdb
import pandas as pd
import re
from difflib import SequenceMatcher

# ==========================================
# CONFIGURATION
# ==========================================
PARQUET_FILE = "Western_Broker_Map_SchedC_2021.parquet" # Updated to 2021 Schedule C (Pivot)
REGISTRY_PATH = os.path.join("Scout_Data_Artifacts", PARQUET_FILE)
INPUT_CSV = "pilot_input_14_rows.csv"
OUTPUT_DIR = "pilot_outputs_2021/"

REQUIRED_PARQUET_COLS = {
    'firm': ['TARGET_BROKER_FIRM', 'BROKER_FIRM_NAME', 'BROKER_FIRM'],
    'city': ['TARGET_BROKER_CITY', 'BROKER_CITY'],
    'state': ['TARGET_BROKER_STATE', 'BROKER_STATE'],
    'client_name': ['CLIENT_ACCOUNT_NAME', 'SPONSOR_NAME'],
    'lives': ['CLIENT_LIVES', 'TOT_PARTCP_CNT', 'PARTICIPANT_COUNT']
}

REQUIRED_CSV_COLS = {
    'firm_input': ['firm_name', 'company_name', 'firm', 'company'],
    'city_input': ['office_city', 'city'],
    'state_input': ['office_state', 'state']
}

# Normalization Regex (Suffix Removal)
SUFFIX_REGEX = re.compile(r'\b(INC|LLC|LTD|CORP|CORPORATION|CO|COMPANY)\b', re.IGNORECASE)

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def resolve_column(df_columns, candidates, name):
    """Finds the first matching column from candidates."""
    for cand in candidates:
        if cand in df_columns:
            return cand
    raise ValueError(f"CRITICAL: Could not find required column '{name}'. Checked: {candidates}")

def normalize_text(text):
    """Stage 0 Normalization: Upper, Strip, Remove Suffixes, Remove Punctuation."""
    if not isinstance(text, str):
        return ""
    
    # 1. Upper & Strip
    text = text.upper().strip()
    
    # 2. Remove Punctuation (keep spaces, remove .,; etc)
    text = re.sub(r'[^\w\s]', '', text)
    
    # 3. Remove Legal Suffixes
    text = SUFFIX_REGEX.sub('', text)
    
    # 4. Collapse multiple spaces
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def normalize_geo(val):
    """Normalize City/State for Exact Matching."""
    if pd.isna(val) or val == "":
        return None
    return str(val).upper().strip()

def calculate_similarity(a, b):
    """Stage 2: Sequence Matcher Ratio."""
    return SequenceMatcher(None, a, b).ratio()

# ==========================================
# MAIN EXECUTION
# ==========================================
def main():
    print("AG STATUS: STARTING SCOUT PILOT CONSTRUCTION")
    
    # 1. Validation
    if not os.path.exists(REGISTRY_PATH):
        print(f"   [ERROR] Parquet not found: {REGISTRY_PATH}")
        sys.exit(1)
    
    # Create Output Dir
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 2. Build Office Registry (DuckDB)
    print("   [STEP 1] Building Office Registry from Parquet...")
    con = duckdb.connect(database=":memory:")
    
    # Load Parquet to inspect headers
    try:
        con.execute(f"CREATE VIEW raw_parquet AS SELECT * FROM '{REGISTRY_PATH}'")
        parquet_cols = [c[0] for c in con.execute("DESCRIBE raw_parquet").fetchall()]
    except Exception as e:
        print(f"   [ERROR] Failed to read Parquet: {e}")
        sys.exit(1)
        
    # Resolve Columns
    pq_map = {}
    try:
        for key, candidates in REQUIRED_PARQUET_COLS.items():
            pq_map[key] = resolve_column(parquet_cols, candidates, key)
    except ValueError as e:
        print(f"   [ERROR] Schema Mismatch: {e}")
        sys.exit(1)
        
    print(f"   [INFO] Resolved Parquet Schema: {pq_map}")
    
    # Aggregate Registry
    # Group by Firm + City + State
    registry_query = f"""
        SELECT 
            {pq_map['firm']} AS office_firm,
            {pq_map['city']} AS office_city,
            {pq_map['state']} AS office_state,
            COUNT(DISTINCT {pq_map['client_name']}) AS total_clients,
            SUM(TRY_CAST({pq_map['lives']} AS BIGINT)) AS total_lives,
            array_to_string((array_agg({pq_map['client_name']} ORDER BY TRY_CAST({pq_map['lives']} AS BIGINT) DESC))[1:5], ', ') AS top_clients
        FROM raw_parquet
        WHERE {pq_map['firm']} IS NOT NULL 
        GROUP BY 1, 2, 3
    """
    
    registry_df = con.execute(registry_query).df()
    print(f"   [INFO] Office Registry Built: {len(registry_df)} unique offices.")
    
    # Pre-normalize Registry for Valid Matches
    registry_df['norm_firm'] = registry_df['office_firm'].apply(normalize_text)
    registry_df['norm_city'] = registry_df['office_city'].apply(normalize_geo)
    registry_df['norm_state'] = registry_df['office_state'].apply(normalize_geo)
    
    # Index Registry by (City, State) for fast Stage 1 lookup
    # Creating a dict of list of offices per location
    office_lookup = {}
    firm_lookup = {}
    for idx, row in registry_df.iterrows():
        # Geo Index
        geo_key = (row['norm_city'], row['norm_state'])
        if geo_key not in office_lookup:
            office_lookup[geo_key] = []
        office_lookup[geo_key].append(row)
        
        # Firm Index (Fallback)
        f_key = row['norm_firm']
        if f_key not in firm_lookup:
            firm_lookup[f_key] = []
        firm_lookup[f_key].append(row)
        
    # 3. Process Input CSV
    print("   [STEP 2] Processing Input Contacts...")
    if not os.path.exists(INPUT_CSV):
        print(f"   [ERROR] Input CSV not found: {INPUT_CSV}")
        print("   [HINT] Please place the file at the expected path.")
        sys.exit(1)
        
    try:
        input_df = pd.read_csv(INPUT_CSV)
    except Exception as e:
        print(f"   [ERROR] Failed to read CSV: {e}")
        sys.exit(1)
        
    # Resolve CSV Columns
    # Resolve CSV Columns
    csv_map = {}
    
    # Critical: Firm Name
    try:
        csv_map['firm_input'] = resolve_column(input_df.columns, REQUIRED_CSV_COLS['firm_input'], 'firm_input')
    except ValueError as e:
        print(f"   [ERROR] Input CSV Schema Mismatch: {e}")
        sys.exit(1)

    # Optional: City/State
    try:
        csv_map['city_input'] = resolve_column(input_df.columns, REQUIRED_CSV_COLS['city_input'], 'city_input')
    except ValueError:
        csv_map['city_input'] = None
        print("   [WARN] City column not found. Treating as empty.")

    try:
        csv_map['state_input'] = resolve_column(input_df.columns, REQUIRED_CSV_COLS['state_input'], 'state_input')
    except ValueError:
        csv_map['state_input'] = None
        print("   [WARN] State column not found. Treating as empty.")
        
    print(f"   [INFO] Resolved CSV Schema: {csv_map}")
    
    # 4. Matching Loop
    ready_rows = []
    ambiguous_rows = []
    unmatched_rows = []
    
    for idx, row in input_df.iterrows():
        # Copy input row data
        out_row = row.to_dict()
        
        # Extract & Normalize Inputs
        raw_firm = row[csv_map['firm_input']]
        raw_city = row[csv_map['city_input']] if csv_map['city_input'] else ""
        raw_state = row[csv_map['state_input']] if csv_map['state_input'] else ""
        
        norm_firm = normalize_text(str(raw_firm))
        norm_city = normalize_geo(raw_city)
        norm_state = normalize_geo(raw_state)
        
        # STAGE 1: Strict Geo-Gate
        candidates = []
        is_geo_missing = False
        
        if not norm_city or not norm_state:
            is_geo_missing = True
            # Fallback to Firm Lookup (No Geo)
            candidates = firm_lookup.get(norm_firm, [])
            if not candidates:
                out_row['match_status'] = 'UNMATCHED'
                out_row['reason'] = 'Missing Location - No Firm Match'
                unmatched_rows.append(out_row)
                continue
        else:
            # Strict Geo Lookup
            candidates = office_lookup.get((norm_city, norm_state), [])
        
        if not candidates and not is_geo_missing:
            # RETRY: Fallback to Firm Lookup (Registry might be missing Geo)
            candidates = firm_lookup.get(norm_firm, [])
            if candidates:
                 # Found Firm match despite Geo Mismatch
                 # We will process this, but MUST flag as Ambiguous later
                 pass 
            else:
                out_row['match_status'] = 'UNMATCHED'
                out_row['reason'] = 'Geo-Gate Failed (No Offices in Location) & No Firm Match'
                unmatched_rows.append(out_row)
                continue
            
        # STAGE 2: Entity Resolution
        scored_candidates = []
        for cand in candidates:
            score = calculate_similarity(norm_firm, cand['norm_firm'])
            # Optimization: If exact match after normalization, score 1.0 (SequenceMatcher usually handles this but distinctiveness helps)
            if norm_firm == cand['norm_firm']:
                score = 1.0
            
            scored_candidates.append({
                'office': cand,
                'score': score
            })
            
        # Sort by Score DESC
        scored_candidates.sort(key=lambda x: x['score'], reverse=True)
        
        top = scored_candidates[0]
        top_score = top['score']
        
        # Check Thresholds
        if top_score < 0.70:
            out_row['match_status'] = 'UNMATCHED'
            out_row['reason'] = f"Low Score (Max: {top_score:.2f})"
            unmatched_rows.append(out_row)
            continue
            
        # Check Ambiguity
        is_ambiguous = False
        reason = ""
        
        if top_score < 0.90:
            is_ambiguous = True
            reason = f"Medium Confidence ({top_score:.2f})"
        elif len(scored_candidates) > 1:
            runner_up_score = scored_candidates[1]['score']
            if (top_score - runner_up_score) < 0.10:
                is_ambiguous = True
                reason = f"Close Runner-Up (Gap: {top_score - runner_up_score:.2f})"
                
        # Assignment
        matched_office = top['office']
        
        if is_ambiguous:
            out_row['candidate_1_firm'] = matched_office['office_firm']
            out_row['candidate_1_score'] = top_score
            if len(scored_candidates) > 1:
                out_row['candidate_2_firm'] = scored_candidates[1]['office']['office_firm']
                out_row['candidate_2_score'] = scored_candidates[1]['score']
            out_row['reason'] = reason
            ambiguous_rows.append(out_row)
        else:
            # READY
            out_row['matched_office_firm'] = matched_office['office_firm']
            out_row['matched_office_city'] = matched_office['office_city']
            out_row['matched_office_state'] = matched_office['office_state']
            out_row['estimated_book_size_lives'] = matched_office['total_lives']
            out_row['estimated_client_count'] = matched_office['total_clients']
            out_row['top_3_clients'] = matched_office['top_clients']
            out_row['match_confidence'] = top_score
            ready_rows.append(out_row)

    # 5. write Outputs
    print("   [STEP 3] Writing Reports...")
    
    if ready_rows:
        ready_df = pd.DataFrame(ready_rows)
        # Add Human-in-the-Loop Feedback Columns
        ready_df['Andrew_Reaction_Enum'] = ""
        ready_df['Andrew_Training_Notes'] = ""
        ready_df.to_csv(os.path.join(OUTPUT_DIR, "ready_office_intel.csv"), index=False)
    else:
        # Create empty if needed to prevent downstream errors
        pd.DataFrame(columns=list(input_df.columns) + ['matched_office_firm']).to_csv(os.path.join(OUTPUT_DIR, "ready_office_intel.csv"), index=False)

    if ambiguous_rows:
        pd.DataFrame(ambiguous_rows).to_csv(os.path.join(OUTPUT_DIR, "ambiguous_matches.csv"), index=False)
    else:
        pd.DataFrame().to_csv(os.path.join(OUTPUT_DIR, "ambiguous_matches.csv"), index=False)
        
    if unmatched_rows:
        pd.DataFrame(unmatched_rows).to_csv(os.path.join(OUTPUT_DIR, "unmatched_rows.csv"), index=False)
    else:
         pd.DataFrame().to_csv(os.path.join(OUTPUT_DIR, "unmatched_rows.csv"), index=False)

    # 6. Summary
    print("\n" + "="*40)
    print("SCOUT PILOT MATCHING SUMMARY")
    print("="*40)
    print(f"Ready:     {len(ready_rows)}")
    print(f"Ambiguous: {len(ambiguous_rows)}")
    print(f"Unmatched: {len(unmatched_rows)}")
    print(f"Total:     {len(input_df)}")
    print("-" * 40)
    print(f"Output Directory: {os.path.abspath(OUTPUT_DIR)}")

if __name__ == "__main__":
    main()
