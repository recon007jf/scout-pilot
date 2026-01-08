import os
import requests
import csv
import time
import datetime
import json
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
# We use CREDITS, not DOLLARS. (Approx 10-20 rows max).
CREDITS_BUDGET_CAP = 50 

# API CONFIG
PDL_API_KEY = os.getenv("PDL_API_KEY") 
PDL_COMPANY_URL = "https://api.peopledatalabs.com/v5/company/enrich"
PDL_PERSON_URL = "https://api.peopledatalabs.com/v5/person/search"

# HOSTILE DATASET (6 Rows)
INPUT_ROWS = [
    {"name": "SPACE EXPLORATION TECHNOLOGIES CORP", "lives": 12000, "funding": 4, "state": "CA"},
    {"name": "VALLEY IRON WORKS INC", "lives": 85, "funding": 4, "state": "CA"},
    {"name": "LEGACY HEALTH SYSTEM", "lives": 15000, "funding": 1, "state": "OR"},
    {"name": "THE SMITH FAMILY TRUST", "lives": 12, "funding": 4, "state": "ID"},
    {"name": "ACME HOLDINGS LLC DBA ACME MFG", "lives": 250, "funding": 4, "state": "NV"},
    {"name": "GLOBAL ENTERPRISE HOLDINGS INC", "lives": 5, "funding": 4, "state": "DE"}
]

def run_probe():
    if not PDL_API_KEY:
        print("❌ CRITICAL: PDL_API_KEY not found in environment.")
        return

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"scout_probe_{timestamp}.csv"
    
    print(f"🕵️ STARTING FORENSIC PROBE v3. Output: {filename}")
    print(f"💳 Credit Cap: {CREDITS_BUDGET_CAP}")

    total_credits_used = 0.0
    results = []

    for row in INPUT_ROWS:
        # State Variables
        decision = "PENDING"
        reason = "N/A"
        
        # Forensics
        c_status = "N/A"
        c_credits = "N/A"
        c_req_id = "N/A"
        p_status = "N/A"
        p_credits = "N/A"
        
        # Data Captured
        enriched_name = "N/A"
        website = "N/A"
        likelihood = "N/A"
        target_person = "N/A"
        target_title = "N/A"

        print(f"Processing: {row['name']}...")

        # --- GATE 1: CREDIT CHECK ---
        if total_credits_used >= CREDITS_BUDGET_CAP:
            decision = "SKIPPED_BUDGET"
            reason = "Credit Cap Hit"
            results.append({
                "Input_Name": row['name'], "Action": decision, "Reason": reason
            })
            continue

        # --- GATE 2: COMPANY ENRICHMENT ---
        params = {"name": row['name'], "location": row['state'], "pretty": False}
        headers = {"X-Api-Key": PDL_API_KEY, "Content-Type": "application/json"}
        
        try:
            c_resp = requests.get(PDL_COMPANY_URL, params=params, headers=headers, timeout=10)
            c_status = c_resp.status_code
            c_req_id = c_resp.headers.get('X-Request-Id', 'N/A')
            
            # RAW HEADER CAPTURE
            raw_creds = c_resp.headers.get('X-Credits-Charged')
            if raw_creds is not None:
                try:
                    val = float(raw_creds)
                    c_credits = val
                    total_credits_used += val
                except:
                    c_credits = f"PARSE_ERR({raw_creds})"
            else:
                c_credits = "MISSING"

            if c_status == 200:
                data = c_resp.json()
                enriched_name = data.get('name', 'Unknown')
                website = data.get('website', 'N/A')
                likelihood = data.get('likelihood', 'N/A') # Just logging, not gating
                
                # Logic: Shell Check (Heuristic Only)
                is_shell_suspect = ("HOLDINGS" in row['name'].upper() or "TRUST" in row['name'].upper()) and row['lives'] < 50
                if website == "N/A" and is_shell_suspect:
                    decision = "HOLD_INVALID_ENTITY"
                    reason = "Shell Suspect (No Website + Name)"
            else:
                decision = "HOLD_API_ERROR"
                reason = f"Company Status {c_status}"

        except Exception as e:
            decision = "ERROR_TRANSPORT"
            reason = str(e)

        # --- GATE 3: VALUE FILTER ($0.00) ---
        if decision == "PENDING":
             if row["lives"] < 50:
                decision = "HOLD_LOW_VALUE"
                reason = "Lives < 50"
             elif row["funding"] == 1 and row["lives"] < 1000:
                decision = "HOLD_LOW_VALUE"
                reason = "Fully Insured & Small"

        # --- GATE 4: PERSON SEARCH ---
        if decision == "PENDING":
            # Safe Query Construction (Elasticsearch Style)
            # We filter by website if we have it (stronger), else company name.
            must_clauses = []
            if website != "N/A":
                must_clauses.append({"term": {"job_company_website": website}})
            else:
                must_clauses.append({"term": {"job_company_name": enriched_name}})
            
            # Title Matches
            title_should = [
                {"match": {"job_title": "CFO"}},
                {"match": {"job_title": "Chief Financial Officer"}},
                {"match": {"job_title": "VP HR"}},
                {"match": {"job_title": "Vice President Human Resources"}}
            ]
            
            query = {
                "query": {
                    "bool": {
                        "must": must_clauses,
                        "should": title_should,
                        "minimum_should_match": 1
                    }
                },
                "size": 1
            }

            try:
                p_resp = requests.post(PDL_PERSON_URL, json=query, headers=headers, timeout=10)
                p_status = p_resp.status_code
                
                # RAW HEADER CAPTURE
                raw_p_creds = p_resp.headers.get('X-Credits-Charged')
                if raw_p_creds is not None:
                    try:
                        val = float(raw_p_creds)
                        p_credits = val
                        total_credits_used += val
                    except:
                         p_credits = f"PARSE_ERR({raw_p_creds})"
                else:
                    p_credits = "MISSING"

                if p_status == 200:
                    p_data = p_resp.json()
                    if p_data.get('data'):
                        person = p_data['data'][0]
                        target_person = person.get('full_name', 'Unknown')
                        target_title = person.get('job_title', 'Unknown')
                        decision = "SEND"
                        reason = f"Found Target"
                    else:
                        decision = "REVIEW"
                        reason = "Company Found, No Target Title"
                else:
                    decision = "REVIEW" 
                    reason = f"Person API Status {p_status}"

            except Exception as e:
                decision = "ERROR_TRANSPORT"
                reason = f"Person Fail: {str(e)}"

        # Record Output
        results.append({
            "Input_Name": row['name'],
            "Enriched_Name": enriched_name,
            "Website": website,
            "Likelihood": likelihood,
            "Target": target_person,
            "Title": target_title,
            "Action": decision,
            "Reason": reason,
            "Comp_Status": c_status,
            "Comp_Credits": c_credits,
            "Pers_Status": p_status,
            "Pers_Credits": p_credits,
            "Req_ID": c_req_id
        })
        
        time.sleep(1)

    # WRITE CSV
    keys = ["Input_Name", "Enriched_Name", "Website", "Likelihood", "Target", "Title", "Action", "Reason", "Comp_Status", "Comp_Credits", "Pers_Status", "Pers_Credits", "Req_ID"]
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(results)

    print(f"\n✅ Probe Complete. Saved to {filename}")
    print(f"📉 Total Credits Used: {total_credits_used}")

if __name__ == "__main__":
    run_probe()
