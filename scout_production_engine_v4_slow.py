import os
import requests
import csv
import time
import datetime
import re
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
# Production Budget: 500 Credits (Approx $50). Increase if processing >330 rows.
CREDITS_BUDGET_CAP = int(os.getenv("SCOUT_BUDGET_CAP", 500))

# FULL RUN: Process all rows
CANARY_LIMIT = None 

# API CONFIG
PDL_API_KEY = os.getenv("PDL_API_KEY") 
PDL_COMPANY_URL = "https://api.peopledatalabs.com/v5/company/enrich"
PDL_PERSON_URL = "https://api.peopledatalabs.com/v5/person/search"

# HOSTILE DATASET (Fallback)
HOSTILE_ROWS = [
    {"name": "SPACE EXPLORATION TECHNOLOGIES CORP", "lives": 12000, "funding": 4, "state": "CA"},
    {"name": "VALLEY IRON WORKS INC", "lives": 85, "funding": 4, "state": "CA"},
    {"name": "LEGACY HEALTH SYSTEM", "lives": 15000, "funding": 1, "state": "OR"},
    {"name": "THE SMITH FAMILY TRUST", "lives": 12, "funding": 4, "state": "ID"},
    {"name": "ACME HOLDINGS LLC DBA ACME MFG", "lives": 250, "funding": 4, "state": "NV"},
    {"name": "GLOBAL ENTERPRISE HOLDINGS INC", "lives": 5, "funding": 4, "state": "DE"}
]

class ScoutEngine:
    def __init__(self):
        if not PDL_API_KEY:
            raise ValueError("❌ CRITICAL: PDL_API_KEY not found in environment.")
        self.total_credits_used = 0.0
        self.rate_limit_hits = 0

    def extract_state(self, address_str):
        if not address_str: return ""
        match = re.search(r'(?:,|^|\s)([A-Z]{2})(?:,|\s|\d|$)', address_str)
        if match: return match.group(1)
        return ""

    def normalize_row(self, row):
        name = row.get('name') or row.get('SPONSOR_NAME') or row.get('sponsor_name') or "Unknown"
        lives_val = row.get('lives') or row.get('PARTICIPANT_COUNT') or row.get('participant_count') or 0
        try:
            if isinstance(lives_val, str): lives_val = lives_val.replace(',', '')
            lives = int(lives_val)
        except: lives = 0
            
        funding_val = row.get('funding') or row.get('FUNDING_CODE') or row.get('funding_code') or 4
        try: funding = int(funding_val)
        except: funding = 4 

        state = row.get('state') or ""
        if not state:
            raw_addr = row.get('SPONSOR_ADDRESS') or row.get('sponsor_address') or ""
            state = self.extract_state(raw_addr)
        
        return {"name": name, "lives": lives, "funding": funding, "state": state}

    def safe_request(self, method, url, **kwargs):
        """Wrapper to handle 429 Rate Limits with Aggressive Backoff"""
        retries = 5 # Maximum resilience
        backoff = 2 # Start at 2 seconds
        resp = None 
        
        for i in range(retries):
            try:
                if method.upper() == "GET":
                    resp = requests.get(url, **kwargs)
                else:
                    resp = requests.post(url, **kwargs)
                
                # Check for Rate Limit
                if resp.status_code == 429:
                    self.rate_limit_hits += 1
                    retry_after = resp.headers.get("Retry-After")
                    
                    if retry_after:
                        try:
                            wait_time = int(retry_after) + 1
                            print(f"      ⚠️ Rate Limit (429). Server requested wait: {wait_time}s")
                        except ValueError:
                            wait_time = backoff
                            print(f"      ⚠️ Rate Limit (429). Backing off {backoff}s...")
                            backoff *= 2 
                    else:
                        print(f"      ⚠️ Rate Limit (429). Backing off {backoff}s...")
                        wait_time = backoff
                        backoff *= 2 

                    time.sleep(wait_time)
                    continue 
                
                return resp 
            
            except requests.exceptions.RequestException as e:
                print(f"      ⚠️ Transport Error (Attempt {i+1}/{retries}): {e}. Retrying...")
                time.sleep(2) 
        
        if resp: return resp
        raise requests.exceptions.ConnectionError("Max retries exceeded with no response.")

    def process_row(self, raw_row):
        row = self.normalize_row(raw_row)
        result = {
            "Client": row['name'], "Enriched": "N/A", "PDL_ID": "N/A", "Target": "N/A",
            "Action": "PENDING", "Reason": "N/A", "Cost": 0.0,
            "Comp_Status": "N/A", "Pers_Status": "N/A", "Error_Log": ""
        }

        print(f"   Processing: {row['name']}...")

        # --- GATE 0: SANITY CHECK ---
        if row['name'] == "Unknown" or len(row['name']) < 3:
            result["Action"] = "HOLD_INVALID_ENTITY"
            result["Reason"] = "Name Invalid/Unknown"
            return result

        # --- GATE 1: CREDIT CHECK ---
        if self.total_credits_used >= CREDITS_BUDGET_CAP:
            result["Action"] = "SKIPPED_BUDGET"
            result["Reason"] = "Cap Hit"
            return result

        # --- GATE 2: COMPANY ENRICHMENT ---
        params = {"name": row['name'], "pretty": False}
        if row['state'] and len(row['state']) == 2: params["location"] = row['state']
        headers = {"X-Api-Key": PDL_API_KEY, "Content-Type": "application/json"}
        
        c_credits = 0.0
        p_credits = 0.0
        pdl_company_id = None
        
        try:
            c_resp = self.safe_request("GET", PDL_COMPANY_URL, params=params, headers=headers, timeout=10)
            c_status = c_resp.status_code
            result["Comp_Status"] = c_status
            
            used = float(c_resp.headers.get('X-Call-Credits-Spent', 0.0))
            c_credits += used
            self.total_credits_used += used

            if c_status == 429:
                result["Action"] = "ERROR_RATE_LIMIT"
                result["Reason"] = "Company API Throttled"
                result["Cost"] = c_credits
                return result

            if c_status == 404:
                if (self.total_credits_used + 1.0) > CREDITS_BUDGET_CAP:
                    print("      -> 404 Strict. Skipping Retry (Budget).")
                else:
                    print(f"      -> 404 Strict. Retrying Relaxed...")
                    params_relaxed = {"name": row['name'], "pretty": False}
                    c_resp = self.safe_request("GET", PDL_COMPANY_URL, params=params_relaxed, headers=headers, timeout=10)
                    used = float(c_resp.headers.get('X-Call-Credits-Spent', 0.0))
                    c_credits += used
                    self.total_credits_used += used
                    c_status = c_resp.status_code
                    result["Comp_Status"] = c_status
                    
                    if c_status == 429:
                        result["Action"] = "ERROR_RATE_LIMIT"
                        result["Reason"] = "Company API Throttled (Retry)"
                        result["Cost"] = c_credits
                        return result

            if c_status == 200:
                data = c_resp.json()
                result["Enriched"] = data.get('name', 'Unknown')
                pdl_company_id = data.get('id')
                result["PDL_ID"] = pdl_company_id
                website = data.get('website')
                if not website: website = None
                
                is_shell_suspect = ("HOLDINGS" in row['name'].upper() or "TRUST" in row['name'].upper()) and row['lives'] < 50
                if not website and is_shell_suspect:
                    result["Action"] = "HOLD_INVALID_ENTITY"
                    result["Reason"] = "Shell Suspect (No Website)"
            else:
                result["Action"] = "HOLD_API_ERROR"
                result["Reason"] = f"Company Status {c_status}"

        except Exception as e:
            result["Action"] = "ERROR_TRANSPORT"
            result["Reason"] = str(e)

        # --- GATE 3: VALUE FILTER ---
        if result["Action"] == "PENDING":
             if row['lives'] < 50:
                result["Action"] = "HOLD_LOW_VALUE"
                result["Reason"] = "Lives < 50"
             elif row['funding'] == 1 and row['lives'] < 1000:
                result["Action"] = "HOLD_LOW_VALUE"
                result["Reason"] = "Fully Insured & Small"

        # --- GATE 4: PERSON SEARCH (ID-BASED SQL) ---
        if result["Action"] == "PENDING":
            if not pdl_company_id:
                result["Action"] = "REVIEW"
                result["Reason"] = "Company Found but ID Missing"
            else:
                # The Proven Path: SQL with ID (Exact Match + SELECT * + Single Line + Size Param)
                sql_query = f"SELECT * FROM person WHERE job_company_id = '{pdl_company_id}' AND (job_title='CFO' OR job_title='Chief Financial Officer' OR job_title='VP HR' OR job_title='Vice President Human Resources' OR job_title='Chief People Officer' OR job_title='Head of People' OR job_title='Director of HR')"
                
                params = {"sql": sql_query, "size": 1, "pretty": False}
                
                try:
                    p_resp = self.safe_request("GET", PDL_PERSON_URL, params=params, headers=headers, timeout=10)
                    result["Pers_Status"] = p_resp.status_code
                    
                    used = float(p_resp.headers.get('X-Call-Credits-Spent', 0.0))
                    p_credits += used
                    self.total_credits_used += used

                    if p_resp.status_code == 429:
                        result["Action"] = "ERROR_RATE_LIMIT"
                        result["Reason"] = "Person API Throttled"
                        result["Cost"] = c_credits + p_credits
                        return result

                    if p_resp.status_code == 200:
                        p_data = p_resp.json()
                        if p_data.get('data'):
                            person = p_data['data'][0]
                            result["Target"] = person.get('full_name', 'Unknown')
                            result["Action"] = "SEND"
                            result["Reason"] = f"Found {person.get('job_title')}"
                        else:
                            result["Action"] = "REVIEW"
                            result["Reason"] = "Company ID Matched, No Target Title"
                    else:
                        result["Action"] = "REVIEW"
                        result["Reason"] = f"Person Error {p_resp.status_code}"
                        result["Error_Log"] = p_resp.text[:200]

                except Exception as e:
                    result["Action"] = "ERROR_TRANSPORT"
                    result["Reason"] = str(e)

        result["Cost"] = c_credits + p_credits
        return result

    def run_batch(self, input_rows, output_file):
        print(f"🚀 SCOUT ENGINE FINAL v4 (Slow & Steady). Budget: {CREDITS_BUDGET_CAP}")
        results = []
        
        for raw_row in input_rows:
            try:
                res = self.process_row(raw_row)
                results.append(res)
                print(f"   [{res['Action']}] {res['Client']} -> {res['Reason']} (Cost: {res['Cost']})")
            except Exception as e:
                print(f"   [CRITICAL ERROR] Row Failed: {str(e)}")
            
            time.sleep(2.0) # Baseline polite delay

        keys = ["Client", "Enriched", "PDL_ID", "Target", "Action", "Reason", "Cost", "Comp_Status", "Pers_Status", "Error_Log"]
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(results)
        
        print(f"\n✅ BATCH COMPLETE. Results saved to {output_file}")
        print(f"📉 Total Session Credits: {self.total_credits_used}")
        print(f"🛡️ Total Rate Limit Hits (Caught & Retried): {self.rate_limit_hits}")

if __name__ == "__main__":
    engine = ScoutEngine()
    
    real_data_file = "dol_batch.csv"
    if os.path.exists(real_data_file):
        print(f"📂 Found Real Data: {real_data_file}")
        try:
            with open(real_data_file, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                if not rows: print("⚠️ Empty CSV!")
                else:
                    if CANARY_LIMIT:
                        print(f"🐤 CANARY RUN: Processing first {CANARY_LIMIT} rows only...")
                        rows = rows[:CANARY_LIMIT]
                    else:
                        print(f"🔥 FULL RUN: Processing {len(rows)} rows...")
                    
                    engine.run_batch(rows, f"production_results_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        except Exception as e: print(f"❌ Error: {e}")
    else:
        print("⚠️ No input file found. Running HOSTILE VERIFICATION...")
        engine.run_batch(HOSTILE_ROWS, "hostile_verification_results.csv")
