import os
import requests
import csv
import time
import datetime
import re
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
# Formula: Rows * 1.65.
CREDITS_BUDGET_CAP = int(os.getenv("SCOUT_BUDGET_CAP", 500))
# Set to integer (e.g. 5) for acceptance test, or None for full run
CANARY_LIMIT = None 

# API CONFIG
PDL_API_KEY = os.getenv("PDL_API_KEY") 
PDL_COMPANY_URL = "https://api.peopledatalabs.com/v5/company/enrich"
PDL_PERSON_URL = "https://api.peopledatalabs.com/v5/person/search"

# HOSTILE DATASET (Fallback)
HOSTILE_ROWS = [{"name": "SPACE EXPLORATION TECHNOLOGIES CORP", "lives": 12000, "funding": 4, "state": "CA", "ein": "00-0000000"}]

class ScoutEngine:
    def __init__(self):
        if not PDL_API_KEY: raise ValueError("CRITICAL: PDL_API_KEY not found.")
        self.total_credits_used = 0.0
        self.rate_limit_hits = 0

    def extract_state(self, address_str):
        if not address_str: return ""
        match = re.search(r'(?:,|^|\s)([A-Z]{2})(?:,|\s|\d|$)', address_str)
        if match: return match.group(1)
        return ""

    def normalize_row(self, row):
        # Flatten keys
        r = {k.lower().strip(): v for k, v in row.items()}
        
        # 1. NAME
        name = r.get('name') or r.get('sponsor_name') or r.get('plan_sponsor_name') or "Unknown"
        
        # 2. LIVES (Robust Parsing)
        lives_val = r.get('lives') or r.get('participant_count') or r.get('total_participant_count') or r.get('total_participants') or r.get('participants') or 0
        try:
            if isinstance(lives_val, str): lives_val = lives_val.replace(',', '')
            lives = int(lives_val)
        except: lives = 0
            
        # 3. FUNDING
        funding_val = r.get('funding') or r.get('funding_code') or r.get('plan_funding_code') or 4
        try: funding = int(funding_val)
        except: funding = 4 

        # 4. ADDRESS (Join non-empty parts)
        parts = [
            r.get('address') or r.get('sponsor_address') or r.get('sponsor_address_line_1') or r.get('us_address_line_1'),
            r.get('city') or r.get('sponsor_city') or r.get('us_city_name'),
            r.get('state') or r.get('sponsor_state') or r.get('us_state_code'),
            r.get('zip') or r.get('sponsor_zip') or r.get('us_zip_code')
        ]
        # Filter None/Empty and join
        full_address = ", ".join([str(p).strip() for p in parts if p and str(p).strip()])
        
        # Fallback state extraction
        state = parts[2]
        if not state and full_address:
            state = self.extract_state(full_address)
            
        # 5. EIN & PLAN YEAR (Robust Fallbacks)
        ein = r.get('ein') or r.get('sponsor_ein') or r.get('ein_number') or r.get('employer_identification_number') or "N/A"
        plan_year = r.get('plan_year') or r.get('plan_year_begin_date') or r.get('plan_eff_date') or r.get('plan_year_begin') or "N/A"

        return {
            "name": name, "lives": lives, "funding": funding, "state": state,
            "address": full_address, "ein": ein, "plan_year": plan_year
        }

    def safe_request(self, method, url, **kwargs):
        retries = 5; backoff = 2; resp = None 
        for i in range(retries):
            try:
                if method.upper() == "GET": resp = requests.get(url, **kwargs)
                else: resp = requests.post(url, **kwargs)
                
                if resp.status_code == 429:
                    self.rate_limit_hits += 1
                    retry_after = resp.headers.get("Retry-After")
                    wait_time = int(retry_after) + 1 if retry_after else backoff
                    print(f"      Rate Limit (429). Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    if not retry_after: backoff *= 2
                    continue 
                return resp 
            except requests.exceptions.RequestException as e:
                print(f"      Transport Error: {e}. Retrying..."); time.sleep(2)
        if resp: return resp
        raise requests.exceptions.ConnectionError("Max retries exceeded.")

    def search_person_waterfall(self, company_id, headers):
        """
        Executes a 3-stage waterfall to find the BEST buyer.
        Returns: (person_dict, tier_name, total_cost_credits, last_status_code)
        """
        # Common exclusion filter (Moved from SQL LIKE to Python check if needed, or omitted if exact match implies exclusion)
        # With exact matching, we don't need "NOT LIKE '%Assistant%'" because "Assistant CFO" won't match "CFO".
        
        # Bucket 1: The Chiefs (High Authority) - EXACT MATCHES ONLY
        titles_t1 = [
            'CFO', 'Chief Financial Officer', 
            'CHRO', 'Chief Human Resources Officer', 
            'CPO', 'Chief People Officer', 
            'Chief Administrative Officer'
        ]
        sql_tier_1 = f"SELECT * FROM person WHERE job_company_id = '{company_id}' AND job_title IN ({', '.join([repr(t) for t in titles_t1])})"
        
        # Bucket 2: The Heads/VPs (Strategic) - EXACT MATCHES ONLY
        titles_t2 = [
            'VP HR', 'Vice President Human Resources', 'Vice President of Human Resources',
            'VP People', 'Vice President People',
            'Head of People', 'Head of HR', 'Head of Human Resources',
            'Director of Benefits', 'Director of Compensation and Benefits',
            'Head of Total Rewards', 'VP Total Rewards'
        ]
        sql_tier_2 = f"SELECT * FROM person WHERE job_company_id = '{company_id}' AND job_title IN ({', '.join([repr(t) for t in titles_t2])})"
        
        # Bucket 3: The Managers (Tactical) - EXACT MATCHES ONLY
        titles_t3 = [
            'Benefits Manager', 'Manager of Benefits',
            'Compensation and Benefits Manager',
            'Human Resources Manager', 'HR Manager'
        ]
        sql_tier_3 = f"SELECT * FROM person WHERE job_company_id = '{company_id}' AND job_title IN ({', '.join([repr(t) for t in titles_t3])})"

        waterfall = [
            ("Tier 1 (Chiefs)", sql_tier_1),
            ("Tier 2 (VPs/Directors)", sql_tier_2),
            ("Tier 3 (Managers)", sql_tier_3)
        ]

        total_cost = 0.0
        last_status = "N/A"

        for tier_name, sql in waterfall:
            # Added 'size' param back as per proven v4 configuration
            params = {"sql": sql, "size": 1, "pretty": False}
            try:
                resp = self.safe_request("GET", PDL_PERSON_URL, params=params, headers=headers, timeout=10)
                last_status = resp.status_code
                
                # Accumulate Cost
                used = float(resp.headers.get('X-Call-Credits-Spent', 0.0))
                total_cost += used
                
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get('data'):
                        # FOUND MATCH
                        return data['data'][0], tier_name, total_cost, last_status
                elif resp.status_code == 429:
                    return None, "RATE_LIMIT", total_cost, last_status
                    
            except Exception as e:
                print(f"      [Waterfall Error] {e}")
        
        return None, "NO_MATCH", total_cost, last_status

    def process_row(self, raw_row):
        row = self.normalize_row(raw_row)
        
        result = {
            "Client": row['name'], 
            "DOL_Lives": row['lives'], "DOL_Funding": row['funding'],
            "DOL_EIN": row['ein'], "DOL_PlanYear": row['plan_year'], "DOL_Address": row['address'],
            "Lives_Source": "DOL", "Funding_Source": "DOL",
            "Enriched_Company": "N/A", "PDL_ID": "N/A", 
            "Target_Name": "N/A", "Target_Title": "N/A", "Target_Email": "N/A", "Target_LinkedIn": "N/A",
            "Contact_Source": "N/A", "Action": "PENDING", "Reason": "N/A", "Cost": 0.0,
            "Comp_Status": "N/A", "Pers_Status": "N/A"
        }
        print(f"   Processing: {row['name']}...")

        # --- GATE 1: SANITY ---
        if row['name'] == "Unknown" or len(row['name']) < 3:
            result["Action"] = "HOLD_INVALID_ENTITY"; result["Reason"] = "Name Invalid"; return result
        if self.total_credits_used >= CREDITS_BUDGET_CAP:
            result["Action"] = "SKIPPED_BUDGET"; result["Reason"] = "Cap Hit"; return result

        # --- GATE 2: VALUE FILTER ---
        if row['lives'] < 50:
            result["Action"] = "HOLD_LOW_VALUE"; result["Reason"] = "Lives < 50"; return result
        if row['funding'] == 1 and row['lives'] < 1000:
            result["Action"] = "HOLD_LOW_VALUE"; result["Reason"] = "Fully Insured & Small"; return result

        # --- GATE 3: COMPANY ENRICHMENT ---
        params = {"name": row['name'], "pretty": False}
        if row['state'] and len(row['state']) == 2: params["location"] = row['state']
        headers = {"X-Api-Key": PDL_API_KEY, "Content-Type": "application/json"}
        
        c_credits = 0.0; p_credits = 0.0; pdl_company_id = None
        
        try:
            c_resp = self.safe_request("GET", PDL_COMPANY_URL, params=params, headers=headers, timeout=10)
            c_status = c_resp.status_code; result["Comp_Status"] = c_status
            
            used = float(c_resp.headers.get('X-Call-Credits-Spent', 0.0))
            c_credits += used
            self.total_credits_used += used # Update Global

            if c_status == 429: result["Action"] = "ERROR_RATE_LIMIT"; result["Cost"] = c_credits; return result
            
            if c_status == 404:
                if (self.total_credits_used + 1.0) <= CREDITS_BUDGET_CAP:
                    print(f"      -> 404 Strict. Retrying Relaxed...")
                    params_relaxed = {"name": row['name'], "pretty": False}
                    c_resp = self.safe_request("GET", PDL_COMPANY_URL, params=params_relaxed, headers=headers, timeout=10)
                    used = float(c_resp.headers.get('X-Call-Credits-Spent', 0.0))
                    c_credits += used
                    self.total_credits_used += used # Update Global
                    c_status = c_resp.status_code; result["Comp_Status"] = c_status

            if c_status == 200:
                data = c_resp.json()
                result["Enriched_Company"] = data.get('name', 'Unknown')
                pdl_company_id = data.get('id'); result["PDL_ID"] = pdl_company_id
                
                website = data.get('website')
                is_shell = ("HOLDINGS" in row['name'].upper() or "TRUST" in row['name'].upper()) and row['lives'] < 50
                if not website and is_shell: 
                    result["Action"] = "HOLD_INVALID_ENTITY"; result["Reason"] = "Shell Suspect"; result["Cost"] = c_credits; return result
            else:
                result["Action"] = "HOLD_API_ERROR"; result["Reason"] = f"Company Status {c_status}"; result["Cost"] = c_credits; return result

        except Exception as e: result["Action"] = "ERROR_TRANSPORT"; result["Reason"] = str(e); return result

        # --- GATE 4: PERSON SEARCH (WATERFALL) ---
        if pdl_company_id:
            person, tier, cost, p_status = self.search_person_waterfall(pdl_company_id, headers)
            
            p_credits = cost
            self.total_credits_used += cost # Update Global with total waterfall cost
            result["Pers_Status"] = p_status
            
            if tier == "RATE_LIMIT":
                result["Action"] = "ERROR_RATE_LIMIT"; result["Reason"] = "Person API Throttled"
            elif person:
                result["Target_Name"] = person.get('full_name', 'Unknown')
                result["Target_Title"] = person.get('job_title', 'N/A')
                result["Target_Email"] = person.get('work_email', 'N/A')
                result["Target_LinkedIn"] = person.get('linkedin_url', 'N/A')
                result["Contact_Source"] = "PDL"
                result["Action"] = "SEND"
                result["Reason"] = f"Found {person.get('job_title')} ({tier})"
            else:
                result["Action"] = "REVIEW"; result["Reason"] = "Company Match, No Target (Waterfall Empty)"
        else:
            result["Action"] = "REVIEW"; result["Reason"] = "Company ID Missing"

        result["Cost"] = c_credits + p_credits
        return result

    def run_batch(self, input_rows, output_file):
        print(f"🚀 SCOUT ENGINE v7.4 (FINAL). Budget: {CREDITS_BUDGET_CAP}")
        results = []
        for raw_row in input_rows:
            try:
                res = self.process_row(raw_row)
                results.append(res)
                print(f"   [{res['Action']}] {res['Client']} -> {res['Reason']} ({res['Cost']} Credits)")
            except Exception as e: print(f"   [CRITICAL] {e}")
            time.sleep(2.0)
        
        keys = ["Client", "DOL_Lives", "DOL_Funding", "DOL_EIN", "DOL_PlanYear", "DOL_Address", "Lives_Source", "Funding_Source", 
                "Enriched_Company", "PDL_ID", "Target_Name", "Target_Title", "Target_Email", "Target_LinkedIn", "Contact_Source",
                "Action", "Reason", "Cost", "Comp_Status", "Pers_Status"]
        
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=keys); writer.writeheader(); writer.writerows(results)
        print(f"\n✅ BATCH COMPLETE. Output: {output_file}")
        print(f"📉 Total Credits Used: {self.total_credits_used}")
        print(f"🛡️ Rate Limit Hits: {self.rate_limit_hits}")

if __name__ == "__main__":
    engine = ScoutEngine()
    if os.path.exists("dol_batch.csv"):
        print("📂 Reading dol_batch.csv...")
        with open("dol_batch.csv", 'r', encoding='utf-8-sig') as f:
            rows = list(csv.DictReader(f))
            if CANARY_LIMIT: 
                print(f"🐤 CANARY MODE: Processing {CANARY_LIMIT} rows.")
                rows = rows[:CANARY_LIMIT]
            engine.run_batch(rows, f"scout_v7_final_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    else: engine.run_batch(HOSTILE_ROWS, "hostile_v7_results.csv")
