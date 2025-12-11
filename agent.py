import csv
import os
import time
import json
import re
import requests
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
INPUT_FILE = 'leads.csv'
GOOGLE_SHEET_NAME = 'Scout Leads'
WORKSHEET_NAME = 'Sheet1'
CREDS_FILE = 'credentials.json'
SERPER_API_KEY = os.getenv("SERPER_API_KEY")

def get_google_sheet_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    if not os.path.exists(CREDS_FILE):
        print(f"❌ ERROR: Could not find '{CREDS_FILE}'.")
        return None
    try:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        print(f"❌ Error authenticating with Google: {e}")
        return None

def write_to_google_sheet(results):
    client = get_google_sheet_client()
    if not client: return
    print(f"\n📝 Connecting to Google Sheet: '{GOOGLE_SHEET_NAME}'...")
    try:
        sheet = client.open(GOOGLE_SHEET_NAME).worksheet(WORKSHEET_NAME)
        
        # Clear existing data to ensure a clean list
        sheet.clear()
        
        # Add Headers
        headers = ["First Name", "Last Name", "Firm", "Found Email", "LinkedIn URL", "Status", "Notes"]
        sheet.append_row(headers)
        
        rows_to_add = []
        for result in results:
            row = [
                result.get('First Name', ''),
                result.get('Last Name', ''),
                result.get('Firm', ''),
                result.get('Found Email', ''),
                result.get('LinkedIn URL', ''),
                result.get('Status', ''),
                result.get('Notes', '')
            ]
            rows_to_add.append(row)
        sheet.append_rows(rows_to_add)
        print(f"✅ Success! Overwrote sheet and uploaded {len(rows_to_add)} rows to '{GOOGLE_SHEET_NAME}'.")
    except Exception as e:
        print(f"❌ Error writing to Google Sheets: {e}")

def read_leads_from_csv(filename):
    leads = []
    if not os.path.exists(filename):
        print(f"❌ ERROR: Could not find '{filename}'.")
        return []
    try:
        with open(filename, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                clean_row = {k.strip(): v.strip() for k, v in row.items() if k}
                leads.append(clean_row)
        print(f"✅ Successfully loaded {len(leads)} leads from {filename}.")
        return leads
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return []

def search_google(query):
    if not SERPER_API_KEY:
        print("⚠️ Warning: SERPER_API_KEY not found in .env")
        return {}
    
    url = "https://google.serper.dev/search"
    payload = json.dumps({"q": query})
    headers = {
        'X-API-KEY': SERPER_API_KEY,
        'Content-Type': 'application/json'
    }
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        return response.json()
    except Exception as e:
        print(f"❌ Error calling Serper API: {e}")
        return {}

def validate_email(email, first_name, last_name, firm):
    if not email: return False
    
    local_part = email.split('@')[0].lower()
    domain = email.split('@')[1].lower()
    
    # 1. Length Check
    if len(local_part) < 3:
        return False
        
    # 2. Junk Filter
    junk_terms = ['info', 'contact', 'admin', 'support', 'hello', 'sales', 'office', 'career', 'jobs']
    if local_part in junk_terms:
        # Only allow if it matches firm domain exactly (rare for personal email, but maybe ok)
        # Actually, for this sales bot, we want PERSONAL emails. So reject generic ones.
        return False
        
    # 3. Name Match
    # Check if email contains parts of the name
    fn_clean = re.sub(r'[^a-z]', '', first_name.lower())
    ln_clean = re.sub(r'[^a-z]', '', last_name.lower())
    
    # Check for free providers
    free_providers = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com']
    is_free = domain in free_providers
    
    if is_free:
        # Stricter rules for free emails: Must contain full first OR full last name
        if fn_clean not in local_part and ln_clean not in local_part:
            return False
    else:
        # For corporate domains, be a bit more lenient, but still require some match
        # Check for at least 3 consecutive chars from first or last name
        # OR initials
        
        # Simple check: does it look like it belongs to them?
        # If the domain matches the firm, we trust it more.
        firm_clean = re.sub(r'[^a-z]', '', firm.lower())
        domain_clean = re.sub(r'[^a-z]', '', domain.split('.')[0])
        
        # If domain matches firm, we are 90% there. Just ensure it's not a random string.
        if firm_clean in domain_clean or domain_clean in firm_clean:
            return True
            
        # If domain doesn't match firm (e.g. parent company), enforce name match
        if fn_clean not in local_part and ln_clean not in local_part:
             # Check for initials (e.g. jdoe)
             if not (local_part.startswith(fn_clean[0]) and ln_clean in local_part):
                 return False

    return True

def process_lead(lead):
    first_name = lead.get('First Name', 'Unknown')
    last_name = lead.get('Last Name', 'Unknown')
    firm = lead.get('Firm', 'Unknown')
    notes = lead.get('Notes', '')

    print(f"🔎 Processing: {first_name} {last_name} from {firm}...")
    
    # --- Step 1: Find Email ---
    email_query = f"{first_name} {last_name} {firm} email address contact"
    email_results = search_google(email_query)
    
    found_email = None
    if 'organic' in email_results:
        for item in email_results['organic']:
            snippet = item.get('snippet', '')
            title = item.get('title', '')
            
            # Extract all potential emails
            emails = re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', snippet + " " + title)
            
            for email in emails:
                # Validate!
                if validate_email(email, first_name, last_name, firm):
                    found_email = email
                    break
            if found_email:
                break
    
    status = ""
    if found_email:
        print(f"   -> 📧 Found Email: {found_email}")
        status = "Found via Search"
    else:
        print(f"   -> ❌ No email found in search.")
        # NO GUESSING allowed.

    # --- Step 2: Find LinkedIn ---
    # Broader search query to handle name variations and firm abbreviations
    linkedin_query = f"{first_name} {last_name} {firm} linkedin"
    linkedin_results = search_google(linkedin_query)
    
    found_linkedin = "Not Found"
    if 'organic' in linkedin_results:
        for item in linkedin_results['organic']:
            link = item.get('link', '')
            if 'linkedin.com/in/' in link:
                found_linkedin = link
                break
    
    if found_linkedin != "Not Found":
        print(f"   -> 🔗 Found LinkedIn: {found_linkedin}")
        
        # --- Step 3: LinkedIn X-Ray (If email not found yet) ---
        if not found_email:
            print(f"   -> 🩻 Attempting LinkedIn X-Ray...")
            # Extract profile ID/slug from URL for cleaner search
            # e.g. https://www.linkedin.com/in/johndoe/ -> johndoe
            # Actually, using the full URL in site: might be too strict if there are params.
            # Let's just use the URL as is, but strip params.
            clean_linkedin_url = found_linkedin.split('?')[0]
            
            xray_query = f'site:{clean_linkedin_url} "@"'
            xray_results = search_google(xray_query)
            
            if 'organic' in xray_results:
                for item in xray_results['organic']:
                    snippet = item.get('snippet', '')
                    title = item.get('title', '')
                    
                    emails = re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', snippet + " " + title)
                    for email in emails:
                        if validate_email(email, first_name, last_name, firm):
                            found_email = email
                            status = "Found via LinkedIn X-Ray"
                            print(f"   -> 🎯 Found via X-Ray: {found_email}")
                            break
                    if found_email: break
    else:
        print(f"   -> ❌ LinkedIn Not Found")
        
    if not found_email:
        status = "Not Found"

    time.sleep(0.5) 

    return {
        "First Name": first_name,
        "Last Name": last_name,
        "Firm": firm,
        "Found Email": found_email,
        "LinkedIn URL": found_linkedin,
        "Status": status,
        "Notes": notes
    }

def main():
    print("--- 🤖 Starting Sales Bot (Email + LinkedIn) ---")
    leads = read_leads_from_csv(INPUT_FILE)
    if not leads: return

    # Process all leads
    leads_to_process = leads
    print(f"Processing all {len(leads_to_process)} leads...")

    results = []
    for lead in leads_to_process:
        try:
            result = process_lead(lead)
            results.append(result)
        except Exception as e:
            print(f"   ⚠️ Error processing {lead.get('First Name', 'Unknown')}: {e}")

    if results:
        write_to_google_sheet(results)
    else:
        print("No results generated.")

if __name__ == "__main__":
    main()
