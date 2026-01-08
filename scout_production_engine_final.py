import os
import requests
import csv
import time
import datetime
import re
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
# Default to 50 for safety. Production can override via ENV.
CREDITS_BUDGET_CAP = int(os.getenv("SCOUT_BUDGET_CAP", 50))
# Canary Limit: Process only first N rows of real data for safety check.
# Set to None to process all.
CANARY_LIMIT = 25 

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

    def extract_state(self, address_str):
        """Attempts to pull a 2-letter state code from a generic address string."""
        if not address_str: return ""
        # Look for 2 uppercase letters surrounded by spaces/commas, usually near end
        match = re.search(r'(?:,|^|\s)([A-Z]{2})(?:,|\s|\d|$)', address_str)
        if match:
            return match.group(1)
        return ""

    def normalize_row(self, row):
        """Maps diverse CSV headers to internal schema."""
        # Name
        name = row.get('name') or row.get('SPONSOR_NAME') or row.get('sponsor_name') or "Unknown"
        
        # Lives
        lives_val = row.get('lives') or row.get('PARTICIPANT_COUNT') or row.get('participant_count') or 0
        try:
            # Handle commas in CSV numbers (e.g. "1,200")
            if isinstance(lives_val, str):
                lives_val = lives_val.replace(',', '')
            lives = int(lives_val)
        except:
            lives = 0
            
        # Funding
        funding_val = row.get('funding') or row.get('FUNDING_CODE') or row.get('funding_code') or 4
        try:
            funding = int(funding_val)
        except:
            funding = 4 

        # State (Explicit or Parsed)
        state = row.get('state') or ""
        if not state:
            raw_addr = row.get('SPONSOR_ADDRESS') or row.get('sponsor_address') or ""
            state = self.extract_state(raw_addr)
        
        return {
            "name": name,
            "lives": lives,
            "funding": funding,
            "state": state
        }

    def process_row(self, raw_row):
        row = self.normalize_row(raw_row)
        
        # Default Result State
        result = {
            "Client": row['name'],
            "Enriched": "N/A",
            "PDL_ID": "N/A",
            "Target": "N/A",
            "Action": "PENDING",
            "Reason": "N/A",
            "Cost": 0.0,
            "Comp_Status": "N/A",
            "Pers_Status": "N/A",
            "Error_Log": ""
        }

        print(f"   Processing: {row['name']}...")

        # --- GATE 0: SANITY CHECK ---
        # Don't send garbage to API
        if row['name'] == "Unknown" or len(row['name']) < 3:
            result["Action"] = "HOLD_INVALID_ENTITY"
            result["Reason"] = "Name Invalid/Unknown"
            return result

        # --- GATE 1: CREDIT CHECK ---
        if self.total_credits_used >= CREDITS_BUDGET_CAP:
            result["Action"] = "SKIPPED_BUDGET"
            result["Reason"] = "Cap Hit"
            return result

        # --- GATE 2: COMPANY ENRICHMENT (WATERFALL) ---
        # Only add location if valid (Avoids over-constraint)
        params = {"name": row['name'], "pretty": False}
        if row['state'] and len(row['state']) == 2:
            params["location"] = row['state']

        headers = {"X-Api-Key": PDL_API_KEY, "Content-Type": "application/json"}
        
        c_credits = 0.0
        p_credits = 0.0
        pdl_company_id = None
        
        try:
            # Attempt 1: Strict
            c_resp = requests.get(PDL_COMPANY_URL, params=params, headers=headers, timeout=10)
            c_status = c_resp.status_code
            result["Comp_Status"] = c_status
            
            used = float(c_resp.headers.get('X-Call-Credits-Spent', 0.0))
            c_credits += used
            self.total_credits_used += used

            # Attempt 2: Relaxed (If 404 & Budget allows 1 more credit)
            if c_status == 404:
                # Check for +1 credit headroom
                if (self.total_credits_used + 1.0) > CREDITS_BUDGET_CAP:
                    print("      -> 404 Strict. Skipping Retry (Budget).")
                else:
                    print(f"      -> 404 Strict. Retrying Relaxed (Name Only)...")
                    params_relaxed = {"name": row['name'], "pretty": False}
                    c_resp = requests.get(PDL_COMPANY_URL, params=params_relaxed, headers=headers, timeout=10)
                    
                    used = float(c_resp.headers.get('X-Call-Credits-Spent', 0.0))
                    c_credits += used
                    self.total_credits_used += used
                    
                    c_status = c_resp.status_code
                    result["Comp_Status"] = c_status # Update status

            if c_status == 200:
                data = c_resp.json()
                result["Enriched"] = data.get('name', 'Unknown')
                pdl_company_id = data.get('id')
                result["PDL_ID"] = pdl_company_id
                website = data.get('website')
                
                # Logic: Shell Check (Strict check on empty string)
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
                # print(f"DEBUG SQL: {sql_query}")
                
                # Standard SQL param structure for PDL Search
                params = {"sql": sql_query, "size": 1, "pretty": False}
                
                try:
                    p_resp = requests.get(PDL_PERSON_URL, params=params, headers=headers, timeout=10)
                    result["Pers_Status"] = p_resp.status_code
                    
                    used = float(p_resp.headers.get('X-Call-Credits-Spent', 0.0))
                    p_credits += used
                    self.total_credits_used += used

                    if p_resp.status_code == 200:
                        p_data = p_resp.json()
                        if p_data.get('data'):
                            person = p_data['data'][0]
                            # Clean up newlines/tabs in result string
                            job_t = person.get('job_title', 'Unknown') or ""
                            result["Target"] = person.get('full_name', 'Unknown')
                            result["Action"] = "SEND"
                            result["Reason"] = f"Found {job_t}"
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
        print(f"🚀 SCOUT ENGINE FINAL. Budget: {CREDITS_BUDGET_CAP} Credits")
        results = []
        
        for raw_row in input_rows:
            try:
                res = self.process_row(raw_row)
                results.append(res)
                print(f"   [{res['Action']}] {res['Client']} -> {res['Reason']} (Cost: {res['Cost']})")
            except Exception as e:
                # Catch-all to prevent batch crash
                print(f"   [CRITICAL ERROR] Row Failed: {str(e)}")
            
            time.sleep(0.5) 

        keys = ["Client", "Enriched", "PDL_ID", "Target", "Action", "Reason", "Cost", "Comp_Status", "Pers_Status", "Error_Log"]
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(results)
        
        print(f"\n✅ BATCH COMPLETE. Results saved to {output_file}")
        print(f"📉 Total Session Credits: {self.total_credits_used}")

if __name__ == "__main__":
    engine = ScoutEngine()
    
    real_data_file = "dol_batch.csv"
    if os.path.exists(real_data_file):
        print(f"📂 Found Real Data: {real_data_file}")
        try:
            with open(real_data_file, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                if not rows:
                    print("⚠️ Input CSV is empty!")
                else:
                    if CANARY_LIMIT:
                        print(f"🐤 CANARY RUN: Processing first {CANARY_LIMIT} rows only...")
                        rows = rows[:CANARY_LIMIT]
                    
                    engine.run_batch(rows, f"production_results_{datetime.datetime.now().strftime('%Y%m%d')}.csv")
        except Exception as e:
            print(f"❌ Error reading CSV: {e}")
    else:
        print("⚠️ No input file found. Running HOSTILE VERIFICATION...")
        engine.run_batch(HOSTILE_ROWS, "hostile_verification_results.csv")
