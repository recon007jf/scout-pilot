import pandas as pd
import os

# --- DATA DICTIONARY ---
# Maps Internal Schema -> External Synonyms
SCHEMA_SYNONYMS = {
    'employer_name': ['PLAN_NAME', 'SPONSOR_NAME', 'EMPLOYER', 'COMPANY', 'SPONSOR', 'CLIENT', 'ACCOUNT'],
    'employer_state': ['SPONS_DFE_MAIL_US_STATE', 'SPONS_DFE_LOC_US_STATE', 'STATE', 'ST', 'SPONSOR_ST', 'LOCATION', 'LOC'],
    'employer_ein': ['SPONS_DFE_EIN', 'EIN'],
    'participant_count': ['TOT_ACTIVE_PARTCP_CNT', 'TOT_PARTCP_BOY_CNT', 'PARTICIPANTS', 'LIVES', 'CNT', 'ENROLLED', 'EMPLOYEES'],
    'funding_insurance': ['FUNDING_INSURANCE_IND'],
    'funding_trust': ['FUNDING_TRUST_IND'],
    'funding_gen': ['FUNDING_GEN_ASSET_IND'],
    'broker_firm_name': ['BROKER', 'AGENCY', 'FIRM', 'VENDOR', 'ACCOUNT', 'CARRIER'],
    'broker_human_name': ['PRODUCER', 'NAME', 'CONTACT', 'AGENT', 'ADVISOR', 'CONSULTANT'],
    'broker_email': ['EMAIL', 'CONTACT_EMAIL'],
    'created_date': ['CREATED', 'DATE']
}

def hunt_header(file_path):
    """
    Scans the first 50 lines to find the true header row.
    Returns: (header_row_index, encoding)
    """
    header_row = 0
    encoding = 'utf-8'
    
    # Try Latin-1 first as it covers more chars
    try:
        with open(file_path, 'r', encoding='latin-1') as f:
            lines = [f.readline() for _ in range(50)]
    except:
        return 0, 'utf-8' # Default
        
    best_score = 0
    best_row = 0
    
    # Analyze each line
    for i, line in enumerate(lines):
        if not line: break
        check = line.upper()
        
        # Score based on keyword hits
        score = 0
        keywords = []
        for key, synonyms in SCHEMA_SYNONYMS.items():
            for syn in synonyms:
                if syn in check:
                    score += 1
                    keywords.append(syn)
                    break # Only count one match per schema key
                    
        # Heuristic: Valid header usually matches at least 2 distinct keys
        # e.g. "Employer" + "State" or "Name" + "Account"
        if score >= 2:
            if score > best_score:
                best_score = score
                best_row = i
                
    if best_score >= 2:
        print(f"🎯 Header Hunter: Found Header at Row {best_row} (Score: {best_score})")
        return best_row, 'latin-1'
    
    print("⚠️ Header Hunter: No strong match. Defaulting to Row 0.")
    return 0, 'latin-1'

def normalize_columns(df):
    """
    Renames columns to Internal Schema based on synonyms.
    Returns: DataFrame with renamed columns.
    """
    df.columns = [str(c).upper().strip() for c in df.columns]
    
    new_names = {}
    for col in df.columns:
        # Check against every schema key
        for schema_key, synonyms in SCHEMA_SYNONYMS.items():
            if schema_key in new_names.values(): continue # Already found this key
            
            # Exact or Fuzzy Match
            # We prefer Exact matches first, then containment
            
            # 1. Exact Match Check
            if col in synonyms:
                new_names[col] = schema_key
                break
            
            # 2. Containment Check (if no exact match yet)
            # Be careful not to map "PLAN_NAME" to "broker_human_name" via "NAME"
            # So strict list is better.
            
    # Apply strict matches first
    df = df.rename(columns=new_names)
    
    # Pass 2: Fuzzy Containment for missing keys
    # Only if the key is still missing
    # (Simplified for now: Just standardizing is often enough if we use the synonym keys downstream)
    
    return df

def smart_read_csv(file_path):
    """
    Full wrapper for robust CSV reading.
    Returns: DataFrame
    """
    if not os.path.exists(file_path): return pd.DataFrame()
    
    header_row, encoding = hunt_header(file_path)
    
    try:
        df = pd.read_csv(file_path, header=header_row, encoding=encoding)
    except:
        # Fallback to UTF-8
        df = pd.read_csv(file_path, header=header_row, encoding='utf-8')
        
    df = normalize_columns(df)
    return df

def smart_read_csv_chunks(file_path, chunksize=5000):
    """
    Yields chunks with normalized columns.
    """
    if not os.path.exists(file_path): return
    
    header_row, encoding = hunt_header(file_path)
    
    try:
        iterator = pd.read_csv(file_path, header=header_row, encoding=encoding, chunksize=chunksize, low_memory=False)
    except:
        iterator = pd.read_csv(file_path, header=header_row, encoding='utf-8', chunksize=chunksize, low_memory=False)
        
    for chunk in iterator:
        yield normalize_columns(chunk)

def identify_file_type(df):
    """
    Determines if the CSV is a Broker Roster, Market Map, or DOL File.
    Returns: 'ROSTER', 'MARKET_MAP', 'DOL_5500', or 'UNKNOWN'
    """
    cols = set(df.columns)
    
    # Case A: Broker Roster (Internal Cheat Sheet)
    # Must have Human Name AND (Account OR Firm)
    has_human = 'broker_human_name' in cols
    has_firm = 'broker_firm_name' in cols
    if has_human and has_firm:
        return 'ROSTER'
        
    # Case B: Market Map (BenefitFlow / Schedule A replacement)
    # Must have Employer Name AND Broker Firm
    has_employer = 'employer_name' in cols
    if has_employer and has_firm:
        return 'MARKET_MAP'
        
    # Case C: DOL 5500
    if 'participant_count' in cols and 'funding_insurance' in cols:
        return 'DOL_5500'
        
    return 'UNKNOWN'
