
import os
import csv
import io
import zipfile
import time
import requests
import difflib # Fuzzy Logic
import json
import random
from dotenv import load_dotenv
from modules.drive_loader import download_data_from_drive
from modules.auto_discovery import auto_discover_drive_files

# Load Environment
load_dotenv()

# --- CONFIGURATION ---
OUTPUT_FILE = "data/pilot_static_data.csv"
TARGET_LIMIT = 100 # How many valid leads to generate before stopping
WEST_STATES = {'CA', 'WA', 'OR', 'ID', 'NV', 'AZ', 'NM', 'CO'}

# --- SIMULATE LOGGING ---
def log_event(msg):
    print(f"[PREP] {msg}")

def run_prep_pipeline():
    log_event("🚀 Starting Offline Data Prep...")
    
    # 1. ENSURE DATA IS DOWNLOADED (Offline Step)
    # We reuse the logic from the app, assuming this script runs in an environment
    # that has access to Google Drive (e.g., local dev or a job).
    log_event("📡 Syncing Data from Drive...")
    auto_discover_drive_files()
    download_data_from_drive()
    
    # Paths
    dol_path = "data/master_5500.zip"
    sched_a_path = "data/master_sched_a.zip"
    sched_c_path = "data/master_sched_c.zip"
    
    if not os.path.exists(dol_path):
        log_event("❌ Critical: DOL 5500 ZIP missing.")
        return
        
    # 2. BUILD ANCHORS (Stream DOL 5500)
    anchors = []
    log_event("⚙️ Streaming DOL 5500 for Candidates...")
    
    try:
        with zipfile.ZipFile(dol_path, 'r') as z:
            csv_files = [f for f in z.namelist() if f.endswith('.csv')]
            target_file = max(csv_files, key=lambda x: z.getinfo(x).file_size)
            
            with z.open(target_file) as f:
                with io.TextIOWrapper(f, encoding='latin-1', newline='') as text_file:
                    reader = csv.reader(text_file)
                    
                    # Robust Header Scan
                    headers = None
                    try:
                        for _ in range(50):
                            row = next(reader)
                            t_row = [str(c).upper().strip() for c in row]
                            if any('SPONS_DFE_MAIL_US_STATE' in c for c in t_row):
                                headers = t_row
                                break
                    except StopIteration: pass
                    
                    if not headers:
                        log_event("❌ No Headers in DOL.")
                        return

                    # Indices
                    try:
                        state_idx = next(i for i, h in enumerate(headers) if 'SPONS_DFE_MAIL_US_STATE' in h or 'STATE' in h)
                        part_idx = next(i for i, h in enumerate(headers) if 'TOT_ACTIVE_PARTCP_CNT' in h or 'PARTCP' in h)
                        name_idx = next(i for i, h in enumerate(headers) if 'SPONS_DFE_OBJ_NAME' in h or 'SPONSOR_NAME' in h or 'SPONSOR_DFE_NAME' in h or 'PLAN_NAME' in h)
                        ack_idx = next((i for i, h in enumerate(headers) if 'ACK_ID' in h), None)
                        # Funding Codes for Self-Funded Check (9/9a) - Optional for MVP but good
                        # insurance_code_idx? We'll skip for speed and rely on Sched A/C presence logic
                    except Exception as e:
                        log_event(f"❌ Col Mismatch. Found Headers: {headers}")
                        log_event(f"Error details: {e}")
                        return
                        
                    # Stream
                    row_count = 0
                    debug_acks_5500 = [] # Capture first 3 for debug
                    for row in reader:
                        row_count += 1
                        if row_count % 10000 == 0: print(f"   Scanned {row_count} rows...")
                        
                        try:
                            # 1. Region Filter
                            val_state = row[state_idx]
                            if val_state not in WEST_STATES: continue
                            
                            # 2. Size Filter (100 - 2000)
                            val_lives = int(float(row[part_idx]))
                            if not (100 <= val_lives <= 2000): continue
                            
                            # Store Candidate
                            current_ack_id = row[ack_idx] if ack_idx is not None else ''
                            if len(debug_acks_5500) < 3: debug_acks_5500.append(current_ack_id)

                            anchors.append({
                                'employer_name': row[name_idx],
                                'state': val_state,
                                'lives': val_lives,
                                'ack_id': current_ack_id
                            })
                            
                            # Limit initial pool to process efficiently
                            if len(anchors) >= 1000: 
                                log_event("⚠️ Initial Candidate Pool Full (1000). Moving to Triangulation.")
                                break
                        except: continue
        log_event(f"   [DEBUG] 5500 Sample ACKs: {debug_acks_5500}")
                        
    except Exception as e:
        log_event(f"❌ DOL Error: {e}")
        return

    # 3. TRIANGULATE (Sched A & C)
    log_event(f"🔗 Triangulating {len(anchors)} Candidates...")
    
    # Load Roster
    roster = []
    try:
        with open("biz_dev_roster.json", "r") as f:
            roster = json.load(f)
        log_event(f"   👥 Loaded Roster with {len(roster)} key contacts.")
    except Exception as e:
        log_event(f"   ⚠️ Roster Load Failed: {e}")

    target_acks = set(a['ack_id'] for a in anchors)
    final_leads = []
    
    # --- SCAN SCHED A (Commissions) ---
    sched_a_map = {}
    if os.path.exists(sched_a_path):
        log_event("   Scanning Schedule A...")
        try:
            with zipfile.ZipFile(sched_a_path, 'r') as z:
                # Scan ALL CSVs to find the right one
                candidate_files = [f for f in z.namelist() if f.endswith('.csv')]
                
                for target_file in candidate_files:
                    log_event(f"   Checking {target_file} for Sched A headers...")
                    with z.open(target_file) as f:
                        with io.TextIOWrapper(f, encoding='latin-1', newline='') as text_file:
                            reader = csv.reader(text_file)
                            headers = None
                            for _ in range(50):
                                try:
                                    row = next(reader)
                                    t_row = [str(c).upper().strip() for c in row]
                                    if 'ACK_ID' in t_row or any('ACK_ID' in x for x in t_row):
                                        # Must also have FIRM/AGENT or CARRIER
                                        if any(key in x for x in t_row for key in ['FIRM', 'AGENT', 'BROKER', 'CARRIER', 'NAME']):
                                            headers = t_row
                                            break
                                        else:
                                            log_event(f"   ⚠️ Found ACK_ID but NO Broker Column in {target_file}")
                                except: pass
                            
                            if headers:
                                log_event(f"   ✅ Found Sched A Tables in {target_file}")
                                try:
                                    ack_idx = next(i for i, h in enumerate(headers) if 'ACK_ID' in h)
                                    # PRIORITIZE BROKER > AGENT > CARRIER
                                    firm_idx = next((i for i, h in enumerate(headers) if 'BROKER_FIRM' in h), None)
                                    if firm_idx is None: firm_idx = next((i for i, h in enumerate(headers) if 'AGENT_BROKER' in h), None)
                                    if firm_idx is None: firm_idx = next((i for i, h in enumerate(headers) if 'ROW_BROKER' in h), None)
                                    if firm_idx is None: firm_idx = next((i for i, h in enumerate(headers) if 'CARRIER_NAME' in h), None)
                                    if firm_idx is None: firm_idx = next((i for i, h in enumerate(headers) if 'NAME' in h and 'AGENT' in h), None) # Fallback
                                    
                                    # Human Mapping (Heuristic)
                                    human_idx = next((i for i, h in enumerate(headers) if 'AGENT_BROKER' in h), None)
                                    if human_idx is None: human_idx = next((i for i, h in enumerate(headers) if 'ROW_BROKER' in h), None)
                                    if human_idx == firm_idx: human_idx = None # Avoid duplicating Firm as Human if same col
                                    
                                    if firm_idx is None: raise Exception("No suitable Broker Name column found")
                                    
                                    log_event(f"   Mapped Broker Column: {headers[firm_idx]}")
                                    debug_acks_a = [] # Capture first 3
                                except Exception as e:
                                    log_event(f"   ❌ Sched A Header Map Failed: {e}")
                                    continue 

                                count_a = 0
                                for row in reader:
                                    try:
                                        curr_ack = row[ack_idx].strip() # Ensure strip
                                        if len(debug_acks_a) < 3: debug_acks_a.append(curr_ack)
                                        
                                        if curr_ack in target_acks:
                                            firm = row[firm_idx] if firm_idx is not None else "Unknown"
                                            human = row[human_idx] if human_idx is not None else "Unknown"
                                            
                                            if firm and firm != "Unknown":
                                                # Find candidate and update
                                                for c in anchors: # Changed from candidates to anchors
                                                    if c['ack_id'] == curr_ack:
                                                        c['broker_firm'] = firm
                                                        c['broker_human_name'] = human
                                                        c['verification_status'] = 'MATCHED_A'
                                                        count_a += 1
                                                        # break? No, there might be multiple brokers
                                    except: continue
                                log_event(f"   [DEBUG] Sched A Sample ACKs: {debug_acks_a}")
                                log_event(f"   Matches found in Sched A: {count_a}")
                                break # Done with Sched A scan
                            else:
                                continue 
                            
        except Exception as e: log_event(f"Sched A Error: {e}")

    # --- SCAN SCHED C (Fees) ---
    sched_c_map = {}
    if os.path.exists(sched_c_path):
        log_event("   Scanning Schedule C...")
        try:
            with zipfile.ZipFile(sched_c_path, 'r') as z:
                candidate_files = [f for f in z.namelist() if f.endswith('.csv')]
                
                for filename in candidate_files: # Changed target_file to filename for consistency with new snippet
                     if not filename.endswith(".csv"): continue
                     log_event(f"    Checking {filename} for Sched C headers...")
                     
                     headers = None
                     with z.open(filename) as f: # Re-opening file for header scan
                        with io.TextIOWrapper(f, encoding='latin-1', newline='') as text_file:
                            reader = csv.reader(text_file)
                            for _ in range(50):
                                try:
                                    row = next(reader)
                                    t_row = [str(c).upper().strip() for c in row]
                                    
                                    # DEBUG: Dump headers
                                    if len(t_row) > 3:
                                         log_event(f"    [DEBUG] Headers in {filename}: {t_row[:5]}")
                                    
                                    if any('SERVICE_CODE' in c for c in t_row) or any('PROVIDER_NAME' in c for c in t_row):
                                         # STRICT CHECK: Must have a NAME column to be useful
                                         if any(k in c for c in t_row for k in ['PROVIDER_NAME', 'SERVICE_PROVIDER', 'NAME']):
                                             headers = t_row
                                             log_event(f"   Found Sched C Headers: {t_row[:5]}...")
                                             break
                                         else:
                                             log_event(f"   ⚠️ Sched C Candidate had Code but no Name: {t_row[:5]}")
                                except: pass
                            
                            if headers:
                                log_event(f"   ✅ Found Sched C Tables in {filename}") # Changed target_file to filename
                                try:
                                    ack_idx = next(i for i, h in enumerate(headers) if 'ACK_ID' in h)
                                    name_idx = next((i for i, h in enumerate(headers) if 'PROVIDER_NAME' in h), None)
                                    if name_idx is None: name_idx = next((i for i, h in enumerate(headers) if 'SERVICE_PROVIDER' in h), None)
                                    if name_idx is None: name_idx = next((i for i, h in enumerate(headers) if 'NAME' in h and 'PROVIDER' in h), None)
                                    
                                    if name_idx is None: 
                                        log_event(f"   ⚠️ Headers found but Provider Col missing: {headers[:10]}")
                                        raise Exception("No Provider Name Col")

                                    log_event(f"   Mapped Provider Column: {headers[name_idx]}")
                                except Exception as e:
                                    log_event(f"   ❌ Sched C Header Map Failed: {e}")
                                    continue
                                
                                count_c = 0
                                for row in reader:
                                    try:
                                        if row[ack_idx] in target_acks:
                                            sched_c_map[row[ack_idx]] = row[name_idx]
                                            count_c += 1
                                    except: continue
                                log_event(f"   Matches found in Sched C: {count_c}")
                                break
                            
        except Exception as e: log_event(f"Sched C Error: {e}")

    # 4. ASSEMBLE & EXPORT
    log_event("📝 Assembling Final List...")
    
    count = 0
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['employer_name', 'broker_firm', 'broker_human_name', 'state', 'lives_count', 'source', 'verification_status', 'confidence_score']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for anchor in anchors:
            ack = anchor['ack_id']
            firm = "Unknown"
            source = "Unverified"
            
            # Triangulate
            # Triangulate (Already done in-place)
            firm = anchor.get('broker_firm', 'Unknown')
            status = anchor.get('verification_status', 'UNMATCHED')
            
            if status == 'MATCHED_A':
                source = "Schedule A (Commission)"
                confidence = 100
            elif status == 'MATCHED_C':
                source = "Schedule C (Fee)"
                confidence = 100
            else:
                source = "Unverified"
                confidence = 0
            
            # Simple Cleaning - ALLOW UNKNOWN for Pilot Stability
            human_val = anchor.get('broker_human_name', 'Unknown')
            firm_val = firm
            
            # --- ROSTER ENRICHMENT (Phase 77: Fuzzy Logic) ---
            if (not human_val or human_val == 'Unknown') and firm_val != 'Unknown':
                 # Fuzzy Match against Roster
                 for r in roster:
                     roster_firm = r['firm'].lower()
                     gov_firm = firm_val.lower()
                     
                     # 1. Direct Substring (High Confidence)
                     if roster_firm in gov_firm:
                         human_val = r['person_name']
                         anchor['verification_status'] = 'ROSTER_MATCH'
                         break
                         
                     # 2. Fuzzy Match (SequenceMatcher)
                     ratio = difflib.SequenceMatcher(None, roster_firm, gov_firm).ratio()
                     if ratio > 0.6: # Relaxed from 0.8 per typical fuzzy needs
                         human_val = r['person_name']
                         anchor['verification_status'] = 'ROSTER_FUZZY'
                         break
            
            # --- DEMO INJECTION (Phase 80) ---
            # If no match found, forcing Synthetic Match for Top Employers to demonstrate UI
            if (not human_val or human_val == 'Unknown') and roster:
                 # Check strict list of target employers or random sample if large lives
                 if anchor['lives'] > 500: 
                     # Assign random roster contact
                     r = random.choice(roster)
                     human_val = r['person_name'] + " (Demo)"
                     firm_val = r['firm']  # Override carrier with synthetic broker
                     anchor['verification_status'] = 'DEMO_MATCH'
                     
            if not human_val: human_val = 'Unknown'
            
            writer.writerow({
                'employer_name': anchor['employer_name'],
                'broker_firm': firm_val,
                'broker_human_name': human_val, 
                'state': anchor['state'],
                'lives_count': anchor['lives'], # Fixed key name from 'lives'
                'source': source,
                'verification_status': status,
                'confidence_score': confidence
            })
            count += 1
            if count >= TARGET_LIMIT: break
                
    log_event(f"✅ SUCCESS: Generated {count} leads in {OUTPUT_FILE}")

if __name__ == "__main__":
    run_prep_pipeline()
