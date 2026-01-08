import pandas as pd
import requests
import json
import os
import time
import argparse
import re
import hashlib
import glob
from datetime import datetime
from urllib.parse import urlparse

# --- CONFIGURATION ---
# Fail Fast on Missing Keys
SERPER_API_KEY = os.getenv("SERPER_API_KEY")
PDL_API_KEY = os.getenv("PDL_API_KEY")

# Robust Path Resolution
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))

# Canonical Paths
INPUT_DIR = os.path.join(PROJECT_ROOT, "backend", "data", "input")
CACHE_DIR = os.path.join(PROJECT_ROOT, ".cache")
SUPPRESSION_DIR = os.path.join(PROJECT_ROOT, "suppression")
ARTIFACTS_DIR = os.path.join(PROJECT_ROOT, "artifacts")
LEDGER_PATH = os.path.join(SUPPRESSION_DIR, "global_allocation_ledger.csv")
LOCK_FILE = LEDGER_PATH + ".lock"

# Constraints
MAX_BROKERS_PER_CLIENT = 2
MAX_CLIENTS_PER_FIRM = 5
MAX_PDL_CALLS = 80
LOCK_TIMEOUT_SECONDS = 10
LOCK_STALE_AGE_SECONDS = 600
API_TIMEOUT = 30 

# Broker Column Allowlist (Priority Order)
BROKER_COL_ALLOWLIST = [
    'Contact Full Name',
    'CONTACT_FULL_NAME',
    'Contact_Full_Name',
    'Broker Name',
    'BROKER_NAME',
    'PROVIDER_CONTACT_NAME',
    'CONTACT_NAME'
]

# Canonical Firm Mapping
CANONICAL_FIRMS = {
    'GALLAGHER': ['Gallagher', 'Arthur J. Gallagher', 'GBS', 'Gallagher Benefit Services'],
    'LOCKTON': ['Lockton', 'Lockton Companies', 'Lockton Dunning'],
    'ALLIANT': ['Alliant', 'Alliant Insurance', 'Alliant Insurance Services'],
    'HUB': ['HUB', 'HUB International', 'Hub International'],
    'USI': ['USI', 'USI Insurance', 'USI Insurance Services'],
    'MERCER': ['Mercer', 'Mercer HR', 'Mercer Health & Benefits'],
    'MARSH': ['Marsh', 'Marsh McLennan', 'MMA', 'Marsh & McLennan Agency'],
    'WTW': ['WTW', 'Willis Towers Watson', 'Willis', 'Willis of'],
    'AON': ['Aon', 'Aon Risk', 'Aon Hewitt']
}

# --- UTILS ---
def normalize_key(text):
    if not isinstance(text, str): return ""
    return re.sub(r'[^a-zA-Z0-9]', '', text.upper())

def clean_linkedin_url(url):
    if not url: return "UNKNOWN"
    try:
        parsed = urlparse(url)
        clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        return clean.lower().strip('/')
    except:
        return url

def get_canonical_firm(firm_name):
    if not isinstance(firm_name, str): return None
    firm_upper = firm_name.upper()
    for key, aliases in CANONICAL_FIRMS.items():
        if key in firm_upper: return key
        for alias in aliases:
            if alias.upper() in firm_upper: return key
    return None

def generate_lead_id(sponsor, firm, person_url):
    raw = f"{normalize_key(sponsor)}_{normalize_key(firm)}_{clean_linkedin_url(person_url)}"
    return hashlib.sha1(raw.encode()).hexdigest()

def find_broker_col(df):
    """Finds the first valid broker name column from allowlist."""
    for col in BROKER_COL_ALLOWLIST:
        if col in df.columns:
            return col
    return None

# --- INFRASTRUCTURE ---

class FileLock:
    def __init__(self, lockfile):
        self.lockfile = lockfile

    def __enter__(self):
        start = time.time()
        while True:
            try:
                self.fd = os.open(self.lockfile, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                return self
            except FileExistsError:
                try:
                    if os.path.exists(self.lockfile):
                        mtime = os.path.getmtime(self.lockfile)
                        if time.time() - mtime > LOCK_STALE_AGE_SECONDS:
                            print("  [WARN] Removing stale lock file.")
                            os.remove(self.lockfile)
                            continue
                except FileNotFoundError:
                    continue
                
                if time.time() - start > LOCK_TIMEOUT_SECONDS:
                    raise TimeoutError(f"Could not acquire lock: {self.lockfile}")
                time.sleep(0.1)

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            os.close(self.fd)
            os.remove(self.lockfile)
        except Exception:
            pass

class SuppressionEngine:
    def __init__(self):
        self.clients = set()
        self.dnc_emails = set()
        self.dnc_domains = set()
        self._load()

    def _load(self):
        c_path = os.path.join(SUPPRESSION_DIR, "client_suppression.csv")
        if os.path.exists(c_path):
            try:
                df = pd.read_csv(c_path)
                for _, row in df.iterrows():
                    if pd.notna(row.get('Client Name')): 
                        self.clients.add(normalize_key(row['Client Name']))
            except Exception: pass

        d_path = os.path.join(SUPPRESSION_DIR, "dnc_list.csv")
        if os.path.exists(d_path):
            try:
                df = pd.read_csv(d_path)
                for _, row in df.iterrows():
                    if pd.notna(row.get('Email')): self.dnc_emails.add(row['Email'].strip().lower())
                    if pd.notna(row.get('Domain')): self.dnc_domains.add(row['Domain'].strip().lower())
            except Exception: pass

    def is_client(self, sponsor):
        return normalize_key(sponsor) in self.clients

    def is_dnc(self, email):
        if not email: return False
        email = email.strip().lower()
        if email in self.dnc_emails: return True
        if email.split('@')[-1] in self.dnc_domains: return True
        return False

class GlobalLedger:
    def __init__(self):
        if not os.path.exists(LEDGER_PATH):
            pd.DataFrame(columns=['Lead_ID', 'Broker_ID', 'Allocated_At', 'Sponsor']).to_csv(LEDGER_PATH, index=False)

    def atomic_allocate(self, lead_id, broker_id, sponsor_name):
        try:
            with FileLock(LOCK_FILE):
                try:
                    df = pd.read_csv(LEDGER_PATH)
                    current_ids = set(df['Lead_ID'].astype(str).tolist())
                except:
                    current_ids = set()

                if lead_id in current_ids: return False

                new_row = pd.DataFrame([{
                    'Lead_ID': lead_id, 
                    'Broker_ID': broker_id, 
                    'Allocated_At': datetime.now().isoformat(),
                    'Sponsor': sponsor_name
                }])
                new_row.to_csv(LEDGER_PATH, mode='a', header=False, index=False)
                return True
        except TimeoutError:
            print("  [WARN] Ledger Lock Timeout.")
            return False

# --- API CLIENTS ---
class SerperClient:
    def __init__(self):
        os.makedirs(CACHE_DIR, exist_ok=True)
        self.cache_path = os.path.join(CACHE_DIR, "serper_results.json")
        self.cache = self._load_cache()

    def _load_cache(self):
        if os.path.exists(self.cache_path):
            with open(self.cache_path, 'r') as f: return json.load(f)
        return {}
    
    def _save_cache(self):
        with open(self.cache_path, 'w') as f: json.dump(self.cache, f)

    def find_profile_url(self, name, firm_name):
        query = f"{name} {firm_name} LinkedIn"
        if query in self.cache: return self.cache[query]

        url = "https://google.serper.dev/search"
        payload = json.dumps({"q": query})
        headers = {'X-API-KEY': SERPER_API_KEY, 'Content-Type': 'application/json'}

        try:
            response = requests.post(url, headers=headers, data=payload, timeout=API_TIMEOUT)
            results = response.json()
            found_url = None
            if 'organic' in results:
                for res in results['organic']:
                    if 'linkedin.com/in/' in res.get('link', ''):
                        found_url = res['link']
                        break
            
            res_data = {'url': clean_linkedin_url(found_url), 'profile_found': bool(found_url)}
            self.cache[query] = res_data
            self._save_cache()
            time.sleep(0.5) 
            return res_data
        except Exception as e:
            print(f"Serper Error: {e}")
            return {'url': None, 'profile_found': False}

class PDLClient:
    def __init__(self):
        os.makedirs(CACHE_DIR, exist_ok=True)
        self.cache_path = os.path.join(CACHE_DIR, "pdl_results.json")
        self.cache = self._load_cache()
        self.call_count = 0

    def _load_cache(self):
        if os.path.exists(self.cache_path):
            with open(self.cache_path, 'r') as f: return json.load(f)
        return {}

    def _save_cache(self):
        with open(self.cache_path, 'w') as f: json.dump(self.cache, f)

    def enrich(self, linkedin_url, expected_firm_key):
        if self.call_count >= MAX_PDL_CALLS: return {'is_valid': False, 'status': "BUDGET_CAP"}
        
        # Hardened Cache Key (URL + Firm)
        cache_key = f"{linkedin_url}|{expected_firm_key}"
        if cache_key in self.cache: return self.cache[cache_key]

        url = "https://api.peopledatalabs.com/v5/person/enrich"
        params = {'profile': linkedin_url, 'pretty': True}
        headers = {'X-Api-Key': PDL_API_KEY}

        try:
            self.call_count += 1
            response = requests.get(url, headers=headers, params=params, timeout=API_TIMEOUT)
            
            if response.status_code == 200:
                data = response.json().get('data', {})
                email = data.get('work_email')
                canonical_pdl = get_canonical_firm(data.get('job_company_name', ''))
                
                is_match = (canonical_pdl == expected_firm_key)
                status = "PDL_MATCH_UNVERIFIED" if (email and is_match) else "MISMATCH_OR_NO_EMAIL"
                
                result = {
                    'email': email,
                    'title': data.get('job_title'),
                    'name': data.get('full_name'),
                    'is_valid': (email and is_match),
                    'status': status
                }
            else:
                result = {'is_valid': False, 'status': f"API_{response.status_code}"}

            self.cache[cache_key] = result
            self._save_cache()
            return result
        except Exception:
            return {'is_valid': False, 'status': "ERROR"}

# --- MAIN ---
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker-id", required=True)
    parser.add_argument("--target-yield", type=int, default=50)
    args = parser.parse_args()

    print(f"--- SCOUT v4 FUSION ENGINE ---")
    print(f"Project Root: {PROJECT_ROOT}")

    # 1. API KEY CHECK
    if not SERPER_API_KEY or not PDL_API_KEY:
        print("CRITICAL: SERPER_API_KEY or PDL_API_KEY not set in environment.")
        return

    # 2. Setup Dirs
    for d in [INPUT_DIR, CACHE_DIR, SUPPRESSION_DIR, ARTIFACTS_DIR]:
        os.makedirs(d, exist_ok=True)

    # 3. Init Systems
    ledger = GlobalLedger()
    suppression = SuppressionEngine()
    serper = SerperClient()
    pdl = PDLClient()
    
    # 4. Strict Input Loading
    bf_files = glob.glob(os.path.join(INPUT_DIR, "BF_RAW_*.csv"))
    if not bf_files:
        print(f"CRITICAL: No BF_RAW_*.csv files found in {INPUT_DIR}.")
        print(f"Please copy them manually from /mnt/data/ or uploads.")
        return

    print(f"Ingesting {len(bf_files)} raw files...")
    
    df_list = []
    for f in bf_files:
        try:
            tmp = pd.read_csv(f)
            # Schema Validation with Flexible Broker Column
            broker_col = find_broker_col(tmp)
            
            required = ['SPONSOR_NAME', 'LIVES', 'PROVIDER_NAME_NORM', 'PROVIDER_STATE']
            missing = [c for c in required if c not in tmp.columns]
            
            if missing:
                print(f"CRITICAL: File {os.path.basename(f)} missing columns: {missing}")
                return
            if not broker_col:
                print(f"CRITICAL: File {os.path.basename(f)} missing any valid broker column (checked {BROKER_COL_ALLOWLIST}).")
                return

            # Normalize Broker Column
            tmp['SC_BROKER_RAW'] = tmp[broker_col]
            df_list.append(tmp)

        except Exception as e:
            print(f"Error reading {f}: {e}")
            return
    
    if not df_list: return
    df = pd.concat(df_list, ignore_index=True)

    # 5. Filter & Prep
    target_states = ['CA', 'WA', 'OR', 'ID', 'NV', 'AZ', 'NM', 'CO']
    df = df[df['PROVIDER_STATE'].isin(target_states)].copy()
    
    df['Canonical_Firm'] = df['PROVIDER_NAME_NORM'].apply(get_canonical_firm)
    df = df[df['Canonical_Firm'].notna()]
    
    df['LIVES'] = pd.to_numeric(df['LIVES'], errors='coerce').fillna(0)
    grouped = df.sort_values(by='LIVES', ascending=False).groupby('SPONSOR_NAME')
    
    output_rows = []
    firm_counts = {}
    
    print(f"Starting Scout Loop [Broker: {args.broker_id}]...")
    
    for sponsor, group in grouped:
        if len(output_rows) >= args.target_yield: break
        
        # A. Client Suppression
        if suppression.is_client(sponsor): continue

        firm_key = group.iloc[0]['Canonical_Firm']
        if firm_counts.get(firm_key, 0) >= MAX_CLIENTS_PER_FIRM: continue
        
        candidates = group.head(MAX_BROKERS_PER_CLIENT)
        sponsor_yield = 0
        
        for _, row in candidates.iterrows():
            if sponsor_yield >= MAX_BROKERS_PER_CLIENT: break
            
            broker_raw_name = row['SC_BROKER_RAW']
            if pd.isna(broker_raw_name) or str(broker_raw_name).strip().lower() in ['unknown', 'nan', '']:
                 continue # Skip bad data

            human_firm = row['PROVIDER_NAME_NORM']
            
            # B. Serper (Discovery)
            serper_res = serper.find_profile_url(broker_raw_name, human_firm)
            if not serper_res['profile_found']: continue
            
            # C. ID Generation
            lead_id = generate_lead_id(sponsor, firm_key, serper_res['url'])
            
            # D. PDL (Enrichment)
            pdl_res = pdl.enrich(serper_res['url'], firm_key)
            if not pdl_res['is_valid']: continue
            
            # E. DNC Check
            if suppression.is_dnc(pdl_res['email']): continue

            # F. Atomic Allocation
            if not ledger.atomic_allocate(lead_id, args.broker_id, sponsor):
                continue
            
            # G. Success
            lives = int(row['LIVES'])
            plan_name = row.get('PLAN_NAME', 'Unknown Plan') 
            
            out_row = {
                'Lead_ID': lead_id,
                'Broker_Allocation': args.broker_id,
                'Primary_Client': sponsor,
                'Plan_Name': plan_name,
                'Lives': lives,
                'Sales_Angle': "TPA Replacement" if lives > 500 else "Conversion Opportunity",
                'Target_Firm': firm_key,
                'Broker_Name': pdl_res['name'] or broker_raw_name,
                'Job_Title': pdl_res['title'],
                'Work_Email': pdl_res['email'],
                'Deliverability': "Test Required",
                'LinkedIn_URL': serper_res['url'],
                'Verification_Status': pdl_res['status'],
                'Data_Source': 'BenefitFlow'
            }
            output_rows.append(out_row)
            sponsor_yield += 1
            print(f"[+] Acquired: {out_row['Broker_Name']} @ {sponsor}")
            
        if sponsor_yield > 0:
            firm_counts[firm_key] = firm_counts.get(firm_key, 0) + 1

    # 6. Save Output
    timestamp = datetime.now().strftime("%Y%m%d")
    out_file = os.path.join(ARTIFACTS_DIR, f"Scout_Fused_{args.broker_id}_{timestamp}.csv")
    
    col_order = ['Lead_ID', 'Broker_Allocation', 'Primary_Client', 'Plan_Name', 'Lives', 
                 'Sales_Angle', 'Target_Firm', 'Broker_Name', 'Job_Title', 'Work_Email', 
                 'Deliverability', 'LinkedIn_URL', 'Verification_Status', 'Data_Source']
    
    pd.DataFrame(output_rows)[col_order].to_csv(out_file, index=False)
    print(f"\nFusion Complete. Generated: {out_file}")

if __name__ == "__main__":
    main()
