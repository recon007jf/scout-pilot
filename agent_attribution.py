import pandas as pd
import streamlit as st
import os
import io
import csv
import zipfile 
from modules.db_client import DBClient
from modules.ui_components import render_message

# --- ROBUST FILE PRE-CLEANER ---
def clean_and_decode(uploaded_file):
    """
    1. Detects Encoding (UTF-8, Latin-1, CP1252).
    2. Removes Null Bytes.
    3. Returns clean StringIO object ready for pandas.
    """
    try:
        # Get raw bytes
        if isinstance(uploaded_file, str): # File Path
            if not os.path.exists(uploaded_file): return None
            with open(uploaded_file, 'rb') as f:
                content_bytes = f.read()
        else: # Streamlit UploadedFile
            # Reset pointer first
            uploaded_file.seek(0)
            content_bytes = uploaded_file.read()
        
        # 1. Encoding Detection (Heuristic)
        encodings = ['utf-8', 'cp1252', 'latin-1', 'utf-16', 'ISO-8859-1']
        decoded_text = None
        
        for enc in encodings:
            try:
                decoded_text = content_bytes.decode(enc)
                # If we get here without error, it might be right, but check for weird chars?
                # Usually decode error triggers first if wrong.
                break 
            except UnicodeDecodeError:
                continue
        
        if decoded_text is None:
            render_message("Critical Error: Could not detect file encoding. Please save as UTF-8 CSV.", "error")
            return None
            
        # 2. Null Byte Removal and Newline Normalization
        # Replace common excel quirks
        decoded_text = decoded_text.replace('\0', '')
        decoded_text = decoded_text.replace('\r\n', '\n').replace('\r', '\n')
            
        # 3. Return Clean Stream
        return io.StringIO(decoded_text)
        
    except Exception as e:
        render_message(f"Preprocessing Error: {e}", "error")
        return None

def smart_read_csv(uploaded_file):
    """
    STEP 1: THE HEADER HUNTER (Robust Version)
    Wrapper around clean_and_decode -> pandas.
    """
    # Pre-process first
    clean_stream = clean_and_decode(uploaded_file)
    if clean_stream is None: return None
    
    try:
        # Detect Delimiter (Sniffer)
        # Peek at the first few lines
        sample = clean_stream.getvalue()[:2048]
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=',;\t|')
            sep = dialect.delimiter
        except:
            sep = None # Fallback to python engine auto-detection
            
        # Reset stream
        clean_stream.seek(0)
        
        # Read first 50 rows to find header
        # Added on_bad_lines='skip' to ignore malformed rows
        df_raw = pd.read_csv(clean_stream, header=None, nrows=50, sep=sep, engine='python', on_bad_lines='skip')
        
        header_row_index = -1
        keywords = ["name", "account", "employer", "broker", "lives", "title", "location", "firm", "company", "client"]

        for i, row in df_raw.iterrows():
            row_str = " ".join(row.astype(str)).lower()
            hit_count = sum(1 for k in keywords if k in row_str)
            if hit_count >= 2:
                header_row_index = i
                break

        if header_row_index == -1:
            render_message("Could not find valid headers in first 50 rows. Please clean file.", "orange")
            return None
        
        # Re-read from header index
        clean_stream.seek(0)
        df = pd.read_csv(clean_stream, header=header_row_index, sep=sep, engine='python', on_bad_lines='skip')
        
        # Strip whitespace from column names
        df.columns = [str(c).strip() for c in df.columns]
        
        render_message("Loaded CSV Successfully.", "success")
        return df
    except Exception as e:
        render_message(f"Read Error: {e}", "error")
        return None

def normalize_columns(df, file_type):
    """
    STEP 3: THE ADAPTIVE NORMALIZER (Strict Synonyms)
    """
    cols = df.columns
    rename_map = {}
    
    # Helper to find col case-insensitive
    def find_col(candidates):
        for c in cols:
            if str(c).lower().strip() in candidates: return c
        return None

    # Synonyms (Expanded)
    h_synonyms = ['name', 'producer', 'broker name', 'agent', 'broker', 'contact']
    f_synonyms = ['account', 'firm', 'agency', 'broker firm', 'company']
    e_synonyms = ['employer', 'company', 'client', 'sponsor', 'group']
    c_synonyms = ['location', 'city', 'address']

    if file_type == 'ROSTER':
        h = find_col(h_synonyms)
        if h: rename_map[h] = 'broker_human_name'
        
        f = find_col(f_synonyms)
        if f: rename_map[f] = 'broker_firm_name'
        
        c = find_col(c_synonyms)
        if c: rename_map[c] = 'broker_city'
        
    elif file_type == 'MARKET_MAP':
        e = find_col(e_synonyms)
        if e: rename_map[e] = 'employer_name'
        
        f = find_col(f_synonyms)
        # Special case: 'Broker' column in generic map could be Human or Firm. 
        # Usually Firm in Market Maps.
        if f: rename_map[f] = 'broker_firm_name'
        
    if rename_map:
        df = df.rename(columns=rename_map)
        
    return df

def identify_type(df):
    cols_lower = [str(c).lower().strip() for c in df.columns]
    
    # Rule A: Roster
    # Name + Account/Firm
    has_name = any(k in cols_lower for k in ['name', 'producer', 'broker', 'contact'])
    has_firm = any(k in cols_lower for k in ['account', 'firm', 'agency'])
    
    if has_name and has_firm: return 'ROSTER'
    
    # Rule B: Market Map
    # Employer + Broker/Firm
    has_emp = any(k in cols_lower for k in ['employer', 'client', 'company', 'sponsor'])
    has_bro = any(k in cols_lower for k in ['broker', 'firm', 'agency'])
    
    if has_emp and has_bro: return 'MARKET_MAP'
    
    return 'UNKNOWN'

# --- SIGNAL SCORER ---
def calculate_signal_score(employer_anchors, broker_row, source_type):
    score = 50 
    emp_state = str(employer_anchors.get('state', '')).upper()
    bro_city = str(broker_row.get('broker_city', '')).upper()
    if bro_city: score += 50
    return score

# --- THE ENGINE (RUN SCOUT LOGIC) ---
def run_attribution_pipeline(uploaded_file, dol_path, sched_a_path, sched_c_path, log_container):
    status_msg = st.empty()
    log_event = lambda message: log_container.markdown(f"- {message}") # Helper for verbose logging
    
    # 0. Check Files
    missing = []
    if not os.path.exists(dol_path): missing.append("DOL 5500")
    if not os.path.exists(sched_a_path): missing.append("Sched A")
    
    if missing:
        return False, f"Missing Base Data: {', '.join(missing)}"
    
    log_event("🚀 Starting Attribution Pipeline...")
    
    # 1. Ingest User File
    if uploaded_file:
        df_user = smart_read_csv(uploaded_file)
        if df_user is None: return False, "Failed to read Uploaded File."
        
        f_type = identify_type(df_user)
        status_msg.info(f"📂 Detected {f_type}. Normalizing...")
        df_user = normalize_columns(df_user, f_type)
        
        roster_map = {} 
        market_map = {} 
        
        if f_type == 'ROSTER':
            for _, row in df_user.iterrows():
                if 'broker_firm_name' in row and 'broker_human_name' in row:
                    key = str(row['broker_firm_name']).upper().strip()
                    if key not in roster_map: roster_map[key] = []
                    roster_map[key].append(row.to_dict())
        
        elif f_type == 'MARKET_MAP':
            for _, row in df_user.iterrows():
                if 'employer_name' in row and 'broker_firm_name' in row:
                    key = str(row['employer_name']).upper().strip()
                    market_map[key] = row['broker_firm_name']
    else:
        df_user = pd.DataFrame()
        f_type = "NONE"
        roster_map = {}
        market_map = {}

    # 2. Build Anchors (Form 5500)
    status_msg.info("⚙️ Building Employer Anchors (DOL 5500)...")
    
    dol_zip = dol_path.replace(".csv", ".zip")
    if not os.path.exists(dol_zip): return False, f"DOL 5500 File Missing: {dol_zip}"
    
    anchors = []
    
    try:
        with zipfile.ZipFile(dol_zip, 'r') as z:
            csv_files = [f for f in z.namelist() if f.endswith('.csv')]
            if not csv_files: return False, "No CSV found in DOL ZIP."
            target_file = max(csv_files, key=lambda x: z.getinfo(x).file_size)
            
            with z.open(target_file) as f:
                with io.TextIOWrapper(f, encoding='latin-1', newline='') as text_file:
                    reader = csv.reader(text_file)
                    
                    # 1. Robust Header Scan
                    headers = None
                    try:
                        for _ in range(50):
                            row = next(reader)
                            t_row = [str(c).upper().strip() for c in row]
                            if any('SPONS_DFE_MAIL_US_STATE' in c for c in t_row) or any('STATE' in c for c in t_row):
                                headers = t_row
                                break
                    except StopIteration:
                        pass
                        
                    if not headers:
                         return False, "Could not find DOL Headers in first 50 lines."
                    
                    # 2. Find Indices
                    try:
                        state_idx = next(i for i, h in enumerate(headers) if 'SPONS_DFE_MAIL_US_STATE' in h or 'STATE' in h)
                        part_idx = next(i for i, h in enumerate(headers) if 'TOT_ACTIVE_PARTCP_CNT' in h or 'PARTCP' in h)
                        name_idx = next(i for i, h in enumerate(headers) if 'SPONS_DFE_OBJ_NAME' in h or 'SPONSOR_NAME' in h or 'PLAN_NAME' in h)
                        ack_idx = next((i for i, h in enumerate(headers) if 'ACK_ID' in h), None)
                    except StopIteration:
                         return False, f"Missing Cols in DOL. Found: {headers}"

                    # 3. Stream Rows
                    row_count = 0
                    west_states = {'CA', 'WA', 'OR', 'CO', 'AZ', 'NV'}
                    
                    for row in reader:
                        row_count += 1
                        if row_count > 5000: # SMOKE TEST LIMIT
                            log_event("🛑 SMOKE TEST: Stopping DOL scan at 5,000 rows.")
                            break
                            
                        if row_count % 1000 == 0:
                             log_event(f"   ... Scanning DOL (Row {row_count})...")
                        
                        try:
                            val_state = row[state_idx]
                            if val_state not in west_states: continue
                            
                            val_name = row[name_idx]
                            
                            try:
                                val_lives = int(float(row[part_idx]))
                            except:
                                val_lives = 0
                            
                            if val_lives < 100: continue
                            
                            val_ack = row[ack_idx] if ack_idx is not None else ''
                            
                            anchors.append({
                                'employer_name': val_name,
                                'state': val_state,
                                'lives': val_lives,
                                'ack_id': str(val_ack)
                            })
                            
                            if len(anchors) >= 500:
                                log_event(f"🛑 Pilot Limit Reached (500 candidates) at {row_count} rows.")
                                break
                        except: continue
                            
    except Exception as e:
        return False, f"DOL ZIP Error: {e}"
            
    # 3. Schedule A (Commissions)
    status_msg.info(f"🔗 Scanning Sched A for {len(anchors)} Anchors...")
    target_acks = set(a['ack_id'] for a in anchors)
    sched_a_map = {}
    sched_a_zip = sched_a_path.replace(".csv", ".zip")
    
    if os.path.exists(sched_a_zip):
         try:
             with zipfile.ZipFile(sched_a_zip, 'r') as z:
                csv_files = [f for f in z.namelist() if f.endswith('.csv')]
                if csv_files:
                    target_file = max(csv_files, key=lambda x: z.getinfo(x).file_size)
                    
                    with z.open(target_file) as f:
                        with io.TextIOWrapper(f, encoding='latin-1', newline='') as text_file:
                            reader = csv.reader(text_file)
                            
                            headers = None
                            try:
                                for _ in range(50):
                                    row = next(reader)
                                    t_row = [str(c).upper().strip() for c in row]
                                    if any('ACK_ID' in c for c in t_row) or any('ACK' in c for c in t_row):
                                        headers = t_row
                                        break
                            except StopIteration: pass
                                
                            if not headers:
                                log_event("   ⚠️ Sched A Headers Not Found.")
                            else:
                                try:
                                    ack_idx = next(i for i, h in enumerate(headers) if 'ACK_ID' in h or 'ACK' in h)
                                    firm_idx = next((i for i, h in enumerate(headers) if 'BROKER_FIRM' in h or 'FIRM' in h or 'AGENT' in h), None)
                                except StopIteration:
                                    log_event(f"   ⚠️ Sched A Missing Columns. Found: {headers}")
                                    ack_idx = None
                                    
                                if ack_idx is not None and firm_idx is not None:
                                    row_count = 0
                                    for row in reader:
                                        row_count += 1
                                        if row_count > 5000: # SMOKE TEST LIMIT
                                            log_event("🛑 SMOKE TEST: Stopping Sched A scan at 5,000 rows.")
                                            break
                                            
                                        if row_count % 1000 == 0: 
                                            log_event(f"   ... Scanning Sched A (Row {row_count})...")
                                        
                                        try:
                                            val_ack = row[ack_idx]
                                            if val_ack in target_acks:
                                                val_firm = row[firm_idx]
                                                sched_a_map[val_ack] = val_firm
                                        except: continue
         except Exception as e:
             log_event(f"   ⚠️ Sched A ZIP Error: {e}")

    # 4. Schedule C (Fees/Consultants) - NEW
    status_msg.info("⚙️ Scanning Schedule C (Service Providers)...")
    sched_c_map = {} # ack_id -> [list of potential brokers]
    
    if os.path.exists(sched_c_path):
        try:
            with zipfile.ZipFile(sched_c_path, 'r') as z:
                csv_files = [f for f in z.namelist() if f.endswith('.csv')]
                if csv_files:
                    target_file = max(csv_files, key=lambda x: z.getinfo(x).file_size)
                    
                    with z.open(target_file) as f:
                        with io.TextIOWrapper(f, encoding='latin-1', newline='') as text_file:
                            reader = csv.reader(text_file)
                            
                            headers = None
                            try:
                                for _ in range(50):
                                    row = next(reader)
                                    t_row = [str(c).upper().strip() for c in row]
                                    if any('SERVICE_CODE' in c for c in t_row) or any('PROVIDER_NAME' in c for c in t_row):
                                        headers = t_row
                                        break
                            except StopIteration: pass
                                
                            if not headers:
                                log_event("   ⚠️ Sched C Headers Not Found.")
                            else:
                                try:
                                    ack_idx_c = next(i for i, h in enumerate(headers) if 'ACK_ID' in h or 'ACK' in h)
                                    name_idx_c = next(i for i, h in enumerate(headers) if 'PROVIDER_NAME' in h or 'NAME' in h)
                                    code_idx_c = next((i for i, h in enumerate(headers) if 'SERVICE_CODE' in h or 'CODE' in h), None)
                                except StopIteration:
                                    log_event(f"   ⚠️ Sched C Missing Key Cols. Found: {headers}")
                                    ack_idx_c = None
                                
                                if ack_idx_c is not None:
                                    row_count = 0
                                    for row in reader:
                                        row_count += 1
                                        if row_count > 5000: # SMOKE TEST LIMIT
                                             break
                                             
                                        try:
                                            val_ack = row[ack_idx_c]
                                            if val_ack in target_acks:
                                                val_name = row[name_idx_c]
                                                val_code = row[code_idx_c] if code_idx_c is not None else ''
                                                
                                                if val_ack not in sched_c_map: sched_c_map[val_ack] = []
                                                sched_c_map[val_ack].append({'name': val_name, 'code': val_code})
                                        except: continue
        except Exception as e:
            log_event(f"   ⚠️ Sched C Error: {e}")

    # 5. Triangulate
    status_msg.info("🔗 Triangulating Matches...")
    matches = 0
    valid_leads = []
    
    # Load Roster
    try:
        import json
        with open('biz_dev_roster.json', 'r') as f:
            static_roster = json.load(f)
    except:
        static_roster = []
    
    for anchor in anchors:
        ack = anchor['ack_id']
        broker_raw = sched_a_map.get(ack)
        source = "Sched A"
        
        # Fallback to Sched C
        if not broker_raw and ack in sched_c_map:
             candidates = sched_c_map[ack]
             if candidates:
                 broker_raw = candidates[0]['name']
                 source = "Sched C (Fee)"
        
        if broker_raw:
            broker_clean = str(broker_raw).upper().replace(".", "").replace(",", "").strip()
            match_found = False
            roster_matches = []
            
            if broker_clean in roster_map:
                match_found = True
                roster_matches = roster_map[broker_clean]
            else:
                 for r_firm in roster_map:
                     if r_firm in broker_clean or broker_clean in r_firm: 
                         match_found = True
                         roster_matches = roster_map[r_firm]
                         break
            
            # Also check static roster list for exact match on firm logic
            if not match_found:
                 # Fuzzy logic from previous version
                 f_key = broker_clean
                 if "GALLAGHER" in f_key: f_key = "GALLAGHER"
                 if "HUB" in f_key: f_key = "HUB INTERNATIONAL"
                 if "LOCKTON" in f_key: f_key = "LOCKTON"
                 if "ALLIANT" in f_key: f_key = "ALLIANT"
                 
                 firm_candidates = [p for p in static_roster if str(p['firm']).upper() in f_key or f_key in str(p['firm']).upper()]
                 if firm_candidates:
                     match_found = True
                     # Just take first for now or match state
                     roster_matches = firm_candidates # Assign list
            
            human_name = "Unknown"
            if match_found:
                 matches += 1
                 if matches % 10 == 0: log_event(f"   🎯 MATCH: {anchor['employer_name']} -> {broker_raw} ({source})")
                 
                 # Basic Human Logic
                 if isinstance(roster_matches, list) and len(roster_matches) > 0:
                      # Try state match
                      best = next((r for r in roster_matches if r.get('state') == anchor['state']), roster_matches[0])
                      # Use .get because dict keys vary between roster logic vs map
                      human_name = best.get('person_name', best.get('broker_human_name', 'Unknown'))
                 
                 # Create Lead
                 valid_leads.append({
                     "employer_name": anchor['employer_name'],
                     "broker_firm": broker_clean,
                     "broker_human_name": human_name,
                     "state": anchor['state'],
                     "lives_count": anchor['lives'],
                     "assets_amount": 0,
                     "verification_status": "VERIFIED" if human_name != "Unknown" else "UNVERIFIED",
                     "source": source,
                     "confidence_score": 100 if source == "Sched A" else 80,
                     "draft_email_text": "",
                     "andrew_feedback_score": "PENDING"
                 })

    # 6. Save & Report
    if valid_leads:
        try:
             db = DBClient()
             count = db.store_leads(valid_leads)
             log_event(f"✅ SAVED {count} Leads to Database.")
        except Exception as e:
             log_event(f"❌ DB Save Error: {e}")
             
    log_event(f"🏁 Pipeline Complete. {matches} Matches found from {len(anchors)} Anchors.")
    return True, f"Found {matches} Matches."
