import os
import time
import json
import re
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
from recon_agent import generate_content_with_retry, search_serper, get_google_sheet_client, GOOGLE_SHEET_NAME, WORKSHEET_NAME

# Load environment variables
load_dotenv()

def verify_current_role(name, target_firm, linkedin_url):
    """
    Uses Gemini to verify if the person is CURRENTLY at the target firm based on a fresh search.
    Returns: (is_match, current_firm_detected, reason)
    """
    print(f"   🕵️ Verifying role for {name} at {target_firm}...")
    
    # 1. Targeted Search
    if linkedin_url:
        query = f'site:{linkedin_url} "Present" OR "Current"'
    else:
        query = f'site:linkedin.com/in/ "{name}" "{target_firm}" "Present"'
        
    search_text = search_serper(query)
    
    # 2. AI Verification
    prompt = f"""
    Verify if {name} is CURRENTLY working at {target_firm}.
    
    SEARCH RESULTS:
    {search_text[:1000]}
    
    INSTRUCTIONS:
    1. Look for "Present", "Current", or date ranges like "2020 - Present".
    2. Ignore past roles.
    3. Return JSON:
    {{
        "is_match": true/false,
        "current_firm": "Name of firm they are currently at",
        "reason": "Explanation based on search snippets"
    }}
    """
    
    try:
        response = generate_content_with_retry(prompt)
        text = response.text.strip()
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            data = json.loads(match.group(0))
            return data.get('is_match'), data.get('current_firm'), data.get('reason')
    except Exception as e:
        print(f"   ⚠️ Error verifying: {e}")
        
    return False, "Unknown", "Verification failed"

def run_integrity_audit():
    print("--- 🛡️ Starting Data Integrity Audit ---")
    
    client = get_google_sheet_client()
    if not client: return
    
    sheet = client.open(GOOGLE_SHEET_NAME).worksheet(WORKSHEET_NAME)
    data = sheet.get_all_records()
    headers = sheet.row_values(1)
    
    # Add "Integrity Status" column if missing
    if "Integrity Status" not in headers:
        sheet.update_cell(1, len(headers) + 1, "Integrity Status")
        headers.append("Integrity Status")
        
    status_col = headers.index("Integrity Status") + 1
    
    issues_found = 0
    
    for i, row in enumerate(data, start=2):
        name = f"{row.get('First Name', '')} {row.get('Last Name', '')}"
        firm = row.get('Firm', '')
        linkedin = row.get('LinkedIn URL', '')
        
        # Skip if already verified (optional, but good for speed)
        # current_status = row.get('Integrity Status', '')
        # if current_status == "Verified": continue
        
        is_match, detected_firm, reason = verify_current_role(name, firm, linkedin)
        
        if is_match:
            status = "Verified"
            print(f"   ✅ {name}: Verified at {firm}")
        else:
            status = f"MISMATCH: Found at {detected_firm}"
            issues_found += 1
            print(f"   ❌ {name}: Mismatch! Found at {detected_firm}. Reason: {reason}")
            
        # Update Sheet
        sheet.update_cell(i, status_col, status)
        time.sleep(1) # Rate limit
        
    print(f"\n🏁 Audit Complete. Issues Found: {issues_found}")
    return issues_found

if __name__ == "__main__":
    run_integrity_audit()
