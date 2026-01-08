import pandas as pd
import glob
import os
import re

# ==========================================
# 1. CONFIGURATION
# ==========================================
# The Canonical Input Zone
INPUT_DIR = '/Users/josephlf/.gemini/antigravity/scratch/backend/data/input'

# Artifact Paths
SYSTEM_FILE = 'artifacts/Master_Hunting_List_Production_v3_SYSTEM.csv'
SYSTEM_OUTPUT = 'artifacts/Master_Hunting_List_Production_v3_SYSTEM_ENRICHED.csv'
ANDREW_OUTPUT = 'artifacts/Master_Hunting_List_Production_v3_ANDREW_ENRICHED.csv'

# Blacklist for Generic Emails
GENERIC_ROOTS = [
    'info', 'sales', 'admin', 'support', 'contact', 'office', 'team', 
    'help', 'hello', 'inquiries', 'marketing', 'jobs', 'careers', 'hr', 
    'recruiting', 'service', 'broker', 'agents'
]
GENERIC_REGEX = r'^(' + '|'.join(GENERIC_ROOTS) + r')([0-9_\-\.]|$)'

print(f"=== PRODUCTION-GRADE LINKEDIN HARVESTER ===")

# Validation
if not os.path.exists(INPUT_DIR):
    print(f"! WARNING: Input folder not found at {INPUT_DIR}. Checking current directory...")
    # Fallback to relative path if absolute fails
    if os.path.exists('backend/data/Input'): 
         INPUT_DIR = 'backend/data/Input'
    elif os.path.exists('BF_RAW_Leads.csv'): 
         INPUT_DIR = '.'
    else: 
         # Try creating it or just raising error
         pass # Let the script logic handle it

if not os.path.exists(SYSTEM_FILE):
    # Try local fallback
    if os.path.exists(os.path.basename(SYSTEM_FILE)):
        SYSTEM_FILE = os.path.basename(SYSTEM_FILE)
    else:
        # Check absolute path
        abs_sys = os.path.abspath(SYSTEM_FILE)
        if os.path.exists(abs_sys):
            SYSTEM_FILE = abs_sys
        else:
            raise FileNotFoundError(f"CRITICAL: System Master not found: {SYSTEM_FILE}")

# ==========================================
# 2. SAFETY FUNCTIONS
# ==========================================
def normalize_linkedin_url(url):
    """
    Standardizes LinkedIn URL. Accepts /in/, /pub/, /profile/.
    """
    if not isinstance(url, str): return ''
    u = url.strip().lower()
    # Remove protocol & params
    u = re.sub(r'https?://(www\.)?', '', u)
    if '?' in u: u = u.split('?')[0]
    u = u.rstrip('/')
    
    # PM Fix: Broaden acceptance criteria
    valid_paths = ['linkedin.com/in/', 'linkedin.com/pub/', 'linkedin.com/profile/']
    if any(p in u for p in valid_paths):
        return u
    return ''

def is_valid_email(email):
    """
    PM Fix: Enforce real email structure (must have @ and . after @).
    """
    if not isinstance(email, str): return False
    if '@' not in email: return False
    parts = email.split('@')
    if len(parts) != 2: return False
    if '.' not in parts[1]: return False
    return True

def is_generic_email(email):
    if not isinstance(email, str) or '@' not in email: return True
    user_part = email.split('@')[0].lower()
    return bool(re.match(GENERIC_REGEX, user_part))

def get_first_name_token(full_name):
    """
    Parses first name token for sanity check.
    Handles 'Last, First' and 'Title First Last'.
    """
    if not isinstance(full_name, str): return None
    s = full_name.lower().strip()
    
    # Handle "Last, First"
    if ',' in s:
        parts = s.split(',')
        if len(parts) > 1: s = parts[1].strip()
    
    # Remove titles
    s = re.sub(r'^(mr\.|mrs\.|ms\.|dr\.|prof\.)\s+', '', s)
    
    # Take first token
    tokens = s.split()
    return tokens[0] if tokens else None

# ==========================================
# 3. HARVEST PHASE (Recursive Scan)
# ==========================================
print(f"STEP 1: Surveying {INPUT_DIR}...")

harvested_frames = []

for root, dirs, files in os.walk(INPUT_DIR):
    for f in files:
        if f.endswith('.csv'):
            fpath = os.path.join(root, f)
            try:
                # Read Header
                # Handle potential coding errors
                try:
                    df_head = pd.read_csv(fpath, nrows=0)
                except:
                    continue

                # 1. Detect Email (Strict Priority)
                email_col = next((c for c in df_head.columns if c.upper() in ['CONTACT EMAIL', 'WORK EMAIL', 'EMAIL']), None)
                if not email_col: 
                    # Fuzzy fallback only if 'EMAIL' is in the name
                    email_col = next((c for c in df_head.columns if 'EMAIL' in c.upper()), None)
                
                # 2. Detect LinkedIn
                # Strict: Must contain "LINKEDIN"
                li_col = next((c for c in df_head.columns if 'LINKEDIN' in c.upper()), None)
                
                # Fallback: "Profile URL" etc, but verified by content
                if not li_col:
                    candidates = [c for c in df_head.columns if 'URL' in c.upper() and ('PROFILE' in c.upper() or 'SOCIAL' in c.upper())]
                    for cand in candidates:
                        # Validation Sample (20 rows)
                        try:
                            sample = pd.read_csv(fpath, usecols=[cand], nrows=20)
                            if sample[cand].astype(str).str.contains('linkedin', case=False).any():
                                li_col = cand
                                print(f"  -> Validated fallback column '{cand}' in {f}")
                                break
                        except: pass

                # 3. Detect Name (For Safety Lock)
                # PM Fix: Stricter detection to avoid poisoning
                name_col = next((c for c in df_head.columns if c.upper() in ['CONTACT FULL NAME', 'FULL NAME', 'CONTACT_NAME']), None)

                if email_col and li_col:
                    cols_to_use = [email_col, li_col]
                    if name_col: cols_to_use.append(name_col)
                    
                    df = pd.read_csv(fpath, usecols=cols_to_use, low_memory=False)
                    
                    # Rename for Consistency
                    rename_map = {email_col: 'raw_email', li_col: 'raw_linkedin'}
                    if name_col: rename_map[name_col] = 'raw_name'
                    df = df.rename(columns=rename_map)
                    
                    # Basic Clean
                    df = df.dropna(subset=['raw_email', 'raw_linkedin'])
                    harvested_frames.append(df)
            except Exception as e:
                pass

if not harvested_frames:
    print("❌ NO VALID ENRICHMENT SOURCES FOUND.")
    # Exit gracefully?
else:
    df_pool = pd.concat(harvested_frames, ignore_index=True)
    print(f"  -> Consolidated Pool: {len(df_pool)} candidates")

    # ==========================================
    # 4. ENGINEERING SAFETY GATES
    # ==========================================
    print("STEP 2: Applying Safety Gates...")

    # Gate 1: Normalize & Validate Email
    df_pool['clean_email'] = df_pool['raw_email'].astype(str).str.lower().str.strip()
    df_pool['is_valid'] = df_pool['clean_email'].apply(is_valid_email)
    df_pool = df_pool[df_pool['is_valid']] # Drop junk emails

    # Gate 2: Normalize LinkedIn
    df_pool['clean_linkedin'] = df_pool['raw_linkedin'].apply(normalize_linkedin_url)
    df_pool = df_pool[df_pool['clean_linkedin'] != ''] # Drop invalid URLs

    # Gate 3: Block Generics
    df_pool['is_generic'] = df_pool['clean_email'].apply(is_generic_email)
    df_pool = df_pool[~df_pool['is_generic']]

    # Gate 4: The Highlander Rule (Ambiguity Check)
    # One Email -> Exactly One Normalized URL
    ambiguity_map = df_pool.groupby('clean_email')['clean_linkedin'].nunique()
    bad_emails = ambiguity_map[ambiguity_map > 1].index
    df_safe = df_pool[~df_pool['clean_email'].isin(bad_emails)].copy()

    print(f"  -> Dropped {len(bad_emails)} ambiguous email keys.")

    # Deduplicate
    df_enrich = df_safe.drop_duplicates(subset=['clean_email'])

    # Build Lookup Map
    if 'raw_name' in df_enrich.columns:
        enrich_map = df_enrich.set_index('clean_email')[['clean_linkedin', 'raw_name']].to_dict('index')
    else:
        enrich_map = df_enrich.set_index('clean_email')[['clean_linkedin']].to_dict('index')

    print(f"  -> Final Safe Lookup Size: {len(enrich_map)}")

    # ==========================================
    # 5. MERGE PHASE (with Name Lock)
    # ==========================================
    print("STEP 3: Patching System Master...")

    df_sys = pd.read_csv(SYSTEM_FILE, low_memory=False)

    # Identify System Keys
    sys_email_col = next((c for c in df_sys.columns if c in ['Contact Email', 'Work_Email', 'Email']), None)
    sys_name_col = next((c for c in df_sys.columns if c in ['Contact Full Name', 'Broker_Name', 'Contact_Name']), None)

    if not sys_email_col: 
        print(f"Columns available: {df_sys.columns.tolist()}")
        raise ValueError("CRITICAL: System missing Email column")

    # Initialize LinkedIn Column
    if 'LinkedIn_URL' not in df_sys.columns: df_sys['LinkedIn_URL'] = ""

    initial_count = df_sys['LinkedIn_URL'].astype(str).str.contains('linkedin', case=False).sum()

    def safe_enrich(row):
        # 1. Protect Existing Data
        curr = str(row.get('LinkedIn_URL', ''))
        if 'linkedin.com' in curr.lower(): return curr
        
        # 2. Match Email (Strict)
        email = str(row.get(sys_email_col, '')).lower().strip()
        if not is_valid_email(email): return curr
        
        match = enrich_map.get(email)
        
        if match:
            url = match['clean_linkedin']
            
            # 3. Name Sanity Lock
            # Only applies if we have a name in Source AND System
            if sys_name_col and 'raw_name' in match:
                sys_name_val = str(row.get(sys_name_col, ''))
                src_name_val = str(match['raw_name'])
                
                # Skip check if either name is too short/empty
                if len(sys_name_val) > 2 and len(src_name_val) > 2:
                    sys_n = get_first_name_token(sys_name_val)
                    src_n = get_first_name_token(src_name_val)
                    
                    # If both parsed and differ, BLOCK IT
                    if sys_n and src_n and sys_n != src_n:
                        # Allow fuzzy match (Rob vs Robert)
                        if not (sys_n in src_n or src_n in sys_n):
                            return curr # Reject mismatch
            
            return f"https://www.{url}"
            
        return curr

    df_sys['LinkedIn_URL'] = df_sys.apply(safe_enrich, axis=1)

    final_count = df_sys['LinkedIn_URL'].astype(str).str.contains('linkedin', case=False).sum()
    print(f"  -> Enriched {final_count - initial_count} new rows.")
    print(f"  -> Final Coverage: {final_count} / {len(df_sys)}")

    df_sys.to_csv(SYSTEM_OUTPUT, index=False)
    print(f"  -> SAVED SECURE SYSTEM COPY: {SYSTEM_OUTPUT}")

    # ==========================================
    # 6. EXPORT ANDREW VIEW
    # ==========================================
    print("STEP 4: Regenerating Andrew View...")

    df_andrew = pd.DataFrame()

    def get_col(df, candidates):
        for c in candidates:
            if c in df.columns: return df[c]
        return ""

    # PM Fix: Map 'Broker_Firm' to the human readable raw name
    df_andrew['Broker_Firm'] = get_col(df_sys, ['firm_name_raw', 'Company Name', 'PROVIDER_NAME_NORM', 'Target_Firm'])
    df_andrew['Contact_Name'] = get_col(df_sys, ['Contact Full Name', 'Broker_Name'])
    df_andrew['Title'] = get_col(df_sys, ['Contact Job Title', 'Job_Title', 'Title'])
    df_andrew['Email'] = get_col(df_sys, ['Contact Email', 'Work_Email'])
    df_andrew['LinkedIn'] = df_sys['LinkedIn_URL']
    df_andrew['Client_Firm'] = get_col(df_sys, ['SPONSOR_NAME', 'Primary_Client'])
    df_andrew['Lives'] = get_col(df_sys, ['LIVES', 'Lives'])
    df_andrew['Funding_Status'] = get_col(df_sys, ['Funding_Status_Est', 'Funding_Status'])
    df_andrew['Funding_Confidence'] = get_col(df_sys, ['Funding_Confidence'])
    df_andrew['Broker_State'] = get_col(df_sys, ['firm_state', 'PROVIDER_STATE'])
    df_andrew['Data_Source'] = get_col(df_sys, ['Data_Provenance', 'Data_Source'])

    df_andrew.to_csv(ANDREW_OUTPUT, index=False)
    print(f"  -> SAVED SALES VIEW: {ANDREW_OUTPUT}")

    print("\n=== MISSION COMPLETE ===")
