import os
import requests
import csv
import time
import datetime
import json
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
CREDITS_BUDGET_CAP = 50 

# API CONFIG
PDL_API_KEY = os.getenv("PDL_API_KEY") 
PDL_COMPANY_URL = "https://api.peopledatalabs.com/v5/company/enrich"
PDL_PERSON_URL = "https://api.peopledatalabs.com/v5/person/search"

# HOSTILE DATASET
INPUT_ROWS = [
    {"name": "SPACE EXPLORATION TECHNOLOGIES CORP", "lives": 12000, "funding": 4, "state": "CA"},
    {"name": "VALLEY IRON WORKS INC", "lives": 85, "funding": 4, "state": "CA"},
    {"name": "LEGACY HEALTH SYSTEM", "lives": 15000, "funding": 1, "state": "OR"},
    {"name": "THE SMITH FAMILY TRUST", "lives": 12, "funding": 4, "state": "ID"},
    {"name": "ACME HOLDINGS LLC DBA ACME MFG", "lives": 250, "funding": 4, "state": "NV"},
    {"name": "GLOBAL ENTERPRISE HOLDINGS INC", "lives": 5, "funding": 4, "state": "DE"}
]

def clean_domain(url):
    """Extracts 'google.com' from 'https://www.google.com/about'"""
    if not url or url == "N/A": return "N/A"
    # Basic clean
    u = url.lower()
    if not u.startswith('http'): u = 'http://' + u
    try:
        parsed = urlparse(u)
        domain = parsed.netloc
        if domain.startswith('www.'): domain = domain[4:]
        return domain
    except:
        return url

def run_probe():
    if not PDL_API_KEY:
        print("❌ CRITICAL: PDL_API_KEY not found.")
        return

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"scout_probe_v5_{timestamp}.csv"
    
    print(f"🕵️ SHADOW MODE v5 (Definitive). Output: {filename}")
    print(f"💳 Credit Cap: {CREDITS_BUDGET_CAP}")

    total_credits_used = 0.0
    results = []

    for row in INPUT_ROWS:
        # State
        decision = "PENDING"
        reason = "N/A"
        
        # Data
        enriched_name = "N/A"
        pdl_id = "N/A"
        website = "N/A"
        domain = "N/A"
        target_person = "N/A"
        target_title = "N/A"
        
        # Metrics
        c_status = "N/A"
        c_credits = 0.0
        p_status = "N/A"
        p_credits = 0.0
        p_error = "N/A"
        match_method = "N/A"

        print(f"Processing: {row['name']}...")

        # --- GATE 1: CREDIT CHECK ---
        if total_credits_used >= CREDITS_BUDGET_CAP:
            results.append({"Input_Name": row['name'], "Action": "SKIPPED_BUDGET", "Reason": "Cap Hit"})
            continue

        # --- GATE 2: COMPANY ENRICHMENT (WATERFALL) ---
        # Attempt 1: Strict (Name + Location)
        params = {"name": row['name'], "location": row['state'], "pretty": False}
        headers = {"X-Api-Key": PDL_API_KEY, "Content-Type": "application/json"}
        
        try:
            c_resp = requests.get(PDL_COMPANY_URL, params=params, headers=headers, timeout=10)
            c_status = c_resp.status_code
            
            # TRACK COST (Attempt 1)
            used = float(c_resp.headers.get('X-Call-Credits-Spent', 0.0))
            c_credits += used
            total_credits_used += used

            if c_status == 200:
                match_method = "Strict (Name+State)"
            
            # Attempt 2: Relaxed (Name Only) if 404 AND Budget Allows
            if c_status == 404:
                if total_credits_used >= CREDITS_BUDGET_CAP:
                     print("   -> Skipping Retry (Budget Cap)")
                else:
                    print(f"   -> 404 Strict. Retrying Relaxed (Name Only)...")
                    params_relaxed = {"name": row['name'], "pretty": False}
                    c_resp = requests.get(PDL_COMPANY_URL, params=params_relaxed, headers=headers, timeout=10)
                    
                    # TRACK COST (Attempt 2)
                    used = float(c_resp.headers.get('X-Call-Credits-Spent', 0.0))
                    c_credits += used
                    total_credits_used += used
                    
                    c_status = c_resp.status_code
                    if c_status == 200:
                        match_method = "Relaxed (Name Only)"

            # PROCESS RESULT
            if c_status == 200:
                data = c_resp.json()
                enriched_name = data.get('name', 'Unknown')
                pdl_id = data.get('id', 'N/A')
                website = data.get('website', 'N/A')
                domain = clean_domain(website) # Normalize for Person Search
                
                # Logic: Shell Check
                is_shell_suspect = ("HOLDINGS" in row['name'].upper() or "TRUST" in row['name'].upper()) and row['lives'] < 50
                if website == "N/A" and is_shell_suspect:
                    decision = "HOLD_INVALID_ENTITY"
                    reason = "Shell Suspect (No Website)"
            else:
                decision = "HOLD_API_ERROR"
                reason = f"Company Status {c_status}"

        except Exception as e:
            decision = "ERROR_TRANSPORT"
            reason = str(e)

        # --- GATE 3: VALUE FILTER ---
        if decision == "PENDING":
             if row["lives"] < 50:
                decision = "HOLD_LOW_VALUE"
                reason = "Lives < 50"
             elif row["funding"] == 1 and row["lives"] < 1000:
                decision = "HOLD_LOW_VALUE"
                reason = "Fully Insured & Small"

        # --- GATE 4: PERSON SEARCH ---
        if decision == "PENDING":
            # Switch to SQL (Proven to work with ID)
            if pdl_id and pdl_id != "N/A":
                company_clause = f"job_company_id='{pdl_id}'"
            elif domain != "N/A":
                company_clause = f"job_company_website='{domain}'"
            else:
                 # Fallback (likely matches nothing if ID failed)
                 company_clause = f"job_company_name='{enriched_name.replace("'", "''")}'"

            title_clause = "(job_title='CFO' OR job_title='Chief Financial Officer' OR job_title='VP HR' OR job_title='Vice President Human Resources')"
            
            sql_query = f"SELECT * FROM person WHERE {company_clause} AND {title_clause}"
            
            params = {
                "sql": sql_query,
                "size": 1,
                "pretty": False
            }

            try:
                # Use GET for SQL as per docs/successful tests
                p_resp = requests.get(PDL_PERSON_URL, params=params, headers=headers, timeout=10)
                p_status = p_resp.status_code
                
                # TRACK COST
                used = float(p_resp.headers.get('X-Call-Credits-Spent', 0.0))
                p_credits = used
                total_credits_used += used

                if p_status == 200:
                    p_data = p_resp.json()
                    if p_data.get('data'):
                        person = p_data['data'][0]
                        target_person = person.get('full_name', 'Unknown')
                        target_title = person.get('job_title', 'Unknown')
                        decision = "SEND"
                        reason = f"Found Target"
                    else:
                        decision = "Review"
                        reason = "Company Found, No Target Title"
                else:
                    decision = "REVIEW" # Or Retry
                    reason = f"Person Status {p_status}"
                    p_error = p_resp.text[:300] # Log error snippet

            except Exception as e:
                decision = "ERROR_TRANSPORT"
                reason = f"Person Fail: {str(e)}"

        # Record Output
        results.append({
            "Input_Name": row['name'],
            "Enriched_Name": enriched_name,
            "Domain": domain,
            "Match_Method": match_method,
            "Target": target_person,
            "Action": decision,
            "Reason": reason,
            "Comp_Credits": c_credits,
            "Pers_Credits": p_credits,
            "Total_Credits": c_credits + p_credits,
            "Pers_Error": p_error
        })
        
        time.sleep(1)

    # WRITE CSV
    keys = ["Input_Name", "Enriched_Name", "Domain", "Match_Method", "Target", "Action", "Reason", "Comp_Credits", "Pers_Credits", "Total_Credits", "Pers_Error"]
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(results)

    print(f"\n✅ Probe Complete. Saved to {filename}")
    print(f"📉 Total Credits Used: {total_credits_used}")

if __name__ == "__main__":
    run_probe()
