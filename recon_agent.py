import os
import time
import json
import requests
import gspread
import google.generativeai as genai
from google.oauth2.service_account import Credentials
import smtplib
import socket
# Note: dnspython is required for MX lookup. If not present, we fall back to search.
try:
    import dns.resolver
except ImportError:
    dns = None

def verify_email_smtp(email):
    """
    Performs an SMTP 'Handshake' to check if the email exists.
    Returns: (bool, reason)
    """
    if not dns: return False, "DNS Lib Missing"
    
    domain = email.split('@')[-1]
    try:
        # 1. Get MX Record
        records = dns.resolver.resolve(domain, 'MX')
        mx_record = str(records[0].exchange)
        
        # 2. Connect (Port 25 is often blocked on Cloud, so we catch this)
        server = smtplib.SMTP(timeout=3)
        server.set_debuglevel(0)
        
        # Try connecting
        server.connect(mx_record, 25)
        server.ehlo()
        
        # 3. Simulate Send
        server.mail('test@example.com')
        code, message = server.rcpt(email)
        server.quit()
        
        # 250 = OK
        if code == 250:
            return True, "SMTP Verify: Passed"
        else:
            return False, f"SMTP Verify: Failed ({code})"
            
    except Exception as e:
        return False, f"SMTP Error/Block: {str(e)}"

def hunter_search(first, last, firm, domain):
    """
    Generates permutations and verifies them via SMTP or Search.
    """
    permutations = [
        f"{first}.{last}@{domain}",
        f"{first[0]}{last}@{domain}",
        f"{first}{last}@{domain}",
        f"{first}_{last}@{domain}"
    ]
    
    # Try Permutations
    for email in permutations:
        email = email.lower()
        print(f"   🏹 Hunter Testing: {email}...")
        
        # A. SMTP Check
        is_valid, reason = verify_email_smtp(email)
        if is_valid:
            return email, "SMTP_VERIFIED"
            
        # B. Search Verification (Fallback)
        query = f'"{email}" "{firm}"'
        snippet = search_serper(query)
        if email in snippet and first in snippet:
             return email, "SEARCH_VERIFIED"
             
    return None, "All permutations failed"
from dotenv import load_dotenv
import re

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
GOOGLE_SHEET_NAME = 'Scout Leads'
WORKSHEET_NAME = 'Sheet1'
CREDS_FILE = 'credentials.json'
SERPER_API_KEY = os.getenv("SERPER_API_KEY")
SERPER_API_KEY = os.getenv("SERPER_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
SAFETY_LIMIT = 50 # 🛑 Max leads to process per run to prevent accidental overage

# Configure Gemini
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.0-flash')

def generate_content_with_retry(prompt, retries=5, base_delay=10):
    """Wraps model.generate_content with exponential backoff for 429 errors."""
    for attempt in range(retries):
        try:
            return model.generate_content(prompt)
        except Exception as e:
            if "429" in str(e) or "quota" in str(e).lower():
                wait_time = base_delay * (2 ** attempt)  # Exponential backoff: 10, 20, 40, 80...
                print(f"   ⚠️ Rate Limit Hit. Waiting {wait_time}s before retry {attempt+1}/{retries}...")
                time.sleep(wait_time)
            else:
                raise e # Re-raise other errors
    raise Exception("Max retries exceeded for Gemini API.")

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

def search_serper(query):
    if not SERPER_API_KEY:
        print("⚠️ Warning: SERPER_API_KEY not found")
        return ""
    
    url = "https://google.serper.dev/search"
    payload = json.dumps({"q": query})
    headers = {
        'X-API-KEY': SERPER_API_KEY,
        'Content-Type': 'application/json'
    }
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        data = response.json()
        snippets = []
        if 'organic' in data:
            for item in data['organic']:
                snippets.append(item.get('snippet', ''))
        return " ".join(snippets)
    except Exception as e:
        print(f"❌ Error calling Serper API: {e}")
        return ""

def search_images(query):
    if not SERPER_API_KEY: return ""
    url = "https://google.serper.dev/images"
    payload = json.dumps({"q": query})
    headers = {
        'X-API-KEY': SERPER_API_KEY,
        'Content-Type': 'application/json'
    }
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        data = response.json()
        if 'images' in data and len(data['images']) > 0:
            return data['images'][0].get('imageUrl', '')
    except Exception as e:
        print(f"❌ Error calling Serper Images API: {e}")
    return ""


from schema import (
    VerificationStatus, 
    EmailStatus, 
    DataSource,
    COL_VERIFICATION_STATUS, 
    COL_VALIDATION_LOG, 
    COL_DATA_SOURCE,
    COL_EMAIL_STATUS,
    COL_CLIENT_NAME,
    COL_FUNDING_TYPE
)

def gather_intel(name, firm, existing_linkedin_url=None):
    print(f"   🕵️ Gathering Intel for {name}...")
    
    # 1. Verification Logic (Double-Key Check)
    # We need to confirm the person actually exists at the firm before trusting any data.
    
    # Use existing URL if valid, otherwise search
    if existing_linkedin_url and 'linkedin.com/in/' in existing_linkedin_url:
        bio_query = f'site:{existing_linkedin_url}'
    else:
        # Strict search for CURRENT role at TARGET firm
        bio_query = f'site:linkedin.com/in/ "{name}" "{firm}" "Present"'
    
    bio_text = search_serper(bio_query)
    
    # Double-Key Verification:
    # Does the snippet actually contain the Name AND the Firm?
    # We do a simple case-insensitive check.
    name_parts = name.lower().split()
    lastname = name_parts[-1] if name_parts else ""
    
    firm_simple = firm.lower().replace("inc.", "").replace("llc", "").strip()
    
    # Check 1: Does the search result mention the Last Name?
    has_name = lastname in bio_text.lower()
    
    # Check 2: Does the search result mention the Firm?
    has_firm = firm_simple in bio_text.lower()
    
    validation_log = {
        "check": "Double-Key Verification",
        "name_match": has_name,
        "firm_match": has_firm,
        "query": bio_query
    }
    
    verification_status = VerificationStatus.RAW.value
    
    if has_name and has_firm:
        verification_status = VerificationStatus.VERIFIED.value
        print(f"   ✅ Double-Key Verified: Found {lastname} + {firm_simple}")
    else:
        verification_status = VerificationStatus.NOT_VERIFIED.value
        print(f"   🛑 failed Double-Key Check. Marking as NOT_VERIFIED.")
        # If strict check fails, we return limited intel and the fail status
        return "", "", verification_status, validation_log

    # If Verified, continue gathering deep intel
    
    # 2. Recent Activity
    posts_query = f'site:linkedin.com/posts/ "{name}" "{firm}"'
    posts_text = search_serper(posts_query)
    
    # 3. Articles
    pulse_query = f'site:linkedin.com/pulse/ "{name}"'
    pulse_text = search_serper(pulse_query)
    
    # 4. Profile Image
    image_query = f'site:linkedin.com/in/ "{name}" "{firm}" profile picture'
    image_url = search_images(image_query)
    
    # 5. Deep Web (Bernays Protocol)
    # Podcasts / Interviews
    podcast_query = f'site:youtube.com OR site:spotify.com OR site:apple.com/podcasts "{name}" "{firm}" interview'
    podcast_text = search_serper(podcast_query)
    
    # News / PR
    news_query = f'"{name}" "{firm}" press release OR announced OR award OR speaker'
    news_text = search_serper(news_query)

    # 6. Social (Twitter/X)
    social_query = f'site:twitter.com OR site:x.com "{name}" "{firm}"'
    social_text = search_serper(social_query)
    
    raw_intel = f"BIO:\n{bio_text}\n\nPOSTS:\n{posts_text}\n\nARTICLES:\n{pulse_text}\n\nPODCASTS/INTERVIEWS:\n{podcast_text}\n\nNEWS/PR:\n{news_text}\n\nSOCIAL:\n{social_text}"
    
    return raw_intel, image_url, verification_status, validation_log

def gather_intel_lite(name, firm, city=""):
    """
    Performs exactly ONE unauthenticated Google Search (Bernays Lite).
    Query: "[Name] [Firm] [City] profile"
    """
    query = f'"{name}" "{firm}" {city} profile'
    print(f"   🕵️ Bernays Lite Search: {query}")
    
    snippet = search_serper(query)
    
    # We treat the snippet as "Bio" for the analysis
    raw_intel = f"BIO / SEARCH RESULT:\n{snippet}"
    
    # Return matched format
    return raw_intel, "", "LITE_CHECK", {}


def analyze_lead(raw_intel, name, firm, client_context=None, role_title="Unknown"):
    print(f"   🧠 Analyzing Lead (Bernays V3.5)...")
    
    # --- LAYER 1: LOW DATA TRIGGER (Safety Valve) ---
    word_count = len(raw_intel.split())
    # Lowered threshold for "Bernays Lite" (Pilot Mode) which relies on snippets (~30 words)
    if word_count < 10:
        print(f"      📉 Low Data Detect ({word_count} words). Engaging Safe Mode.")
        return {
            'psych_profile': 'Executive Pragmatist',
            'Unconscious_Desire': 'Efficiency & Results',
            'Archetype_Evidence': 'Low Digital Footprint (Safe Mode Triggered)',
            'Hook': 'I know you prioritize results over noise.',
            'Pain_Points': ['Wasted administrative time', 'Opague vendor pricing'],
            'Podcast_Name': None,
            'Podcast_URL': None
        }



def write_email(analysis, first_name, client_context=None):
    print(f"   ✍️ Writing Email...")
    
    # Andrew Oram Voice Guidelines:
    # 1. Tone: Consultative, Peer-to-Peer, Confident.
    # 2. Anti-Bot: NEVER say "Hope this finds you well". Start with Context.
    # 3. Structure: Context Anchor -> Psyche Hook -> Value Prop -> CTA.
    
    # --- CONTEXTUAL TEMPLATE: GENERAL ASSETS (5500) ---
    if client_context and client_context.get('source') == 'F5500_General_Assets':
        client_name = client_context.get('client_name', 'your client')
        print(f"      📝 Using 'General Assets' Template for {client_name}")
        
        prompt = f"""
        You are Andrew Oram, the founder of Point C. Write a short, punchy cold email to {first_name}.
        
        CONTEXT ANCHOR:
        - "I was reviewing some 5500 data and saw you handle the self-funded plan for {client_name}."
        - "The filing shows 'General Assets' (Line 9a), which means the employer pays claims from their own account."
        
        PSYCHOGRAPHIC HOOK (Integrate this naturally):
        - Their Archetype: {analysis.get('psych_profile', 'Unknown')}
        - Their Hook: "{analysis.get('Hook', 'N/A')}"
        
        THE PIVOT (Value Prop):
        - "General Assets" setups are black boxes.
        - Point C strips out hidden admin fees to show clients where the money goes.
        - "We're beating UMR on transparency."
        
        CALL TO ACTION:
        - "Open to comparing notes?"
        
        TONE RULES:
        - Consultative. Not salesy.
        - NO fluff. NO "I hope you are well".
        - Use terms: "General Assets", "Stop-loss true-up", "Black box".
        """
        
    else:
        # --- V3.5 ARCHETYPE TEMPLATES ---
        archetype = analysis.get('psych_profile', 'Relational')
        
        # PUBLIC SECTOR OVERRIDE
        # If the client is Public Sector, we ALWAYS use a Safety/Budget tone, regardless of personality
        if client_context and ("Public" in client_context.get('client_industry', '') or "Gov" in client_context.get('client_industry', '')):
             prompt = f"""
             You are Andrew Oram (Point C). Write a cold email to {first_name} (Broker for Public Sector Client).
             STRATEGY: PUBLIC SECTOR / FIDUCIARY (Safety First).
             Subject: Fiduciary Audit for [Client Name]
             Message: "Municipal plans are under the microscope. We provide the fiduciary data trail UMR hides. Safe, compliant, transparent."
             Tone: Serious, Protective.
             """
             
        elif archetype == 'Executive Pragmatist':
             # PRAGMATIST (The V3 New Archetype)
             prompt = f"""
             You are Andrew Oram (Point C). Write a cold email to {first_name} (Executive Broker).
             STRATEGY: EXECUTIVE PRAGMATIST (Brief, Commercial).
             Subject: TPA Optionality for [Client]
             
             BODY:
             "Neil, keeping this brief. We are a boutique TPA alternative to UMR/Meritain for your self-funded block. 
             We own our data and networks (Cigna/Aetna). 
             If you need a secondary option for an upcoming renewal, I'm available to compare specs. -Andrew"
             
             RULES:
             - Zero fluff.
             - No "Hook" or "Research".
             - Pure business value.
             """

        else:
            # STANDARD 4 ARCHETYPES
            prompt = f"""
            You are Andrew Oram, founder of Point C. Write a cold email to {first_name} (Broker).
            
            ARCHETYPE STRATEGY: '{archetype}'
            
            1. IF 'Guardian' (Compliance/Risk):
               - Focus: Fiduciary Control, Eliminating Hidden Fees, Compliance.
               - Message: "Protect your client from PBM spread pricing lawsuits."
               
            2. IF 'Visionary' (Innovator):
               - Focus: Disruption, Next-Gen Tech, Breaking the Status Quo.
               - Message: "The BUCA model is broken. We built the fix."
               
            3. IF 'Competitor' (Winner):
               - Focus: Winning Renewals, Elite Performance, Speed.
               - Message: "Give your team the edge to win against the big houses."
               
            4. IF 'Relational' (People):
               - Focus: Service Culture, Responsiveness, Partnership.
               - Message: "We actually answer the phone. We treat your clients like family."
               
            STRICT RULES:
            - Start with: "Hi {first_name},"
            - OPENER: Immediate context. NO "Hope you are doing well".
            - HOOK: "{analysis.get('Hook', 'I noticed your work in the industry.')}"
            - LENGTH: Under 120 words.
            - CTA: "Open to comparing notes?"
            """

    try:
        response = generate_content_with_retry(prompt)
        return response.text.strip()
    except Exception as e:
        print(f"❌ Error writing email: {e}")
        return ""

def guess_email_logic(name, firm, intel):
    """
    If email is missing, ask Gemini to guess it and explain why.
    """
    print(f"   🤔 Guessing Email for {name}...")
    prompt = f"""
    The email address for {name} at {firm} was NOT found in public searches.
    
    Based on the firm's likely domain and standard corporate patterns, provide:
    1. A Best Guess Email (e.g. first.last@company.com)
    2. A Reasonable Explanation for why it wasn't found (e.g. "Strict spam filters", "New role", "Small digital footprint", "Uses parent company domain").
    
    INTEL CONTEXT:
    {intel[:500]}...
    
    OUTPUT FORMAT:
    Return ONLY a JSON object:
    {{
        "guess": "name@company.com",
        "reason": "Explanation here..."
    }}
    """
    try:
        response = generate_content_with_retry(prompt)
        text = response.text.strip()
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            return json.loads(match.group(0))
    except Exception as e:
        print(f"❌ Error guessing email: {e}")
    


def verify_email_identity(email, full_name):
    """
    Checks if an email address appears to match the user's name.
    Returns: (is_match: bool, reason: str)
    """
    if not email or "@" not in email:
        return False, "Invalid Format"
        
    user_part = email.split("@")[0].lower()
    full_name_lower = full_name.lower()
    first_name = full_name_lower.split()[0] if full_name_lower else ""
    last_name = full_name_lower.split()[-1] if full_name_lower else ""
    
    # Check 1: Exact First or Last name in user_part
    match_first = first_name in user_part
    match_last = last_name in user_part
    
    # Check 2: Initial + Last (e.g. jdoe)
    initial_last = f"{first_name[0]}{last_name}" if first_name else "xyz"
    match_initial = initial_last in user_part
    
    if match_first or match_last or match_initial:
        return True, "Identity Match"
        
    return False, f"Email '{email}' does not contain parts of name '{full_name}'"

def explain_failure(name, firm):
    """
    Generates a brief hypothesis on why verification failed.
    """
    prompt = f"""
    I searched for '{name}' at '{firm}' and found NO public verification.
    Provide a single, short sentence hypothesis (max 15 words) explaining why.
    Examples:
    - "Subject likely uses a nickname or maiden name."
    - "Company may operate under a holding group name."
    - "Subject has zero digital footprint (Ghost)."
    - "Name spelling appears phonetically incorrect."
    
    Hypothesis:
    """
    try:
        response = generate_content_with_retry(prompt)
        return response.text.strip()
    except:
        return "No digital footprint found."

def process_single_lead(i, row, headers, sheet):
    """
    Processes a single lead row.
    Returns True if successful, False otherwise.
    """
    try:
        first_name = row.get('First Name', '')
        last_name = row.get('Last Name', '')
        firm = row.get('Firm', '')
        full_name = f"{first_name} {last_name}"
        linkedin_url = row.get('LinkedIn URL', '')
        
        if not linkedin_url:
            print(f"⏩ Skipping {full_name} (No LinkedIn URL)")
            return False

        # Name Quality Gate (Phase 2 Safeguard)
        if len(last_name) < 2:
            print(f"⏩ Skipping {full_name} (Incomplete Name: '{last_name}')")
            try:
                # Mark as Incomplete
                email_col = headers.index("Found Email") + 1
                valid_col = headers.index("Verification Status") + 1
                
                sheet.update_cell(i, valid_col, VerificationStatus.NOT_VERIFIED.value)
                
                # Only label email if empty or raw
                curr_email = row.get('Found Email', '')
                if not curr_email or "INCOMPLETE" not in curr_email:
                     sheet.update_cell(i, email_col, f"{curr_email}\n⚠️ [INCOMPLETE DATA]")
            except: pass
            return False

        print(f"\nProcessing Row {i}: {full_name}")
        
        # Step A.1: Check Existing Data (Phase 2 Logic)
        found_email = row.get('Found Email', '')
        is_imported = found_email and "GUESS" not in found_email and "[FOUND]" not in found_email and "Not Found" not in found_email
        
        verification_status = VerificationStatus.RAW.value
        data_source = DataSource.IMPORTED.value if is_imported else DataSource.GUESSED.value
        validation_log = {}

        if is_imported:
            # PHASE 2: Verify Imported Email
            is_valid, reason = verify_email_identity(found_email, full_name)
            validation_log['email_identity_check'] = reason
            
            if is_valid:
                verification_status = VerificationStatus.VERIFIED.value
                print(f"   ✅ Imported Email Verified: {found_email}")
            else:
                verification_status = VerificationStatus.NOT_VERIFIED.value
                print(f"   ⚠️ Imported Email Mismatch: {found_email} ({reason})")
                # We DO NOT delete imported data. just flag it.

            # Save Status immediately
            try:
                valid_col = headers.index("Verification Status") + 1
                source_col = headers.index("Data Source") + 1
                sheet.update_cell(i, valid_col, verification_status)
                sheet.update_cell(i, source_col, data_source)
            except: pass

        # Step A.2: Intel Gathering (Double-Key)
        # We still run this to get firm info/verification, even if email exists
        raw_intel, image_url, intel_status, intel_log = gather_intel(full_name, firm, linkedin_url)
        
        # Merge Logs
        if intel_log: validation_log.update(intel_log)
        
        # STATUS RECONCILLIATION (Phase 2)
        final_status = VerificationStatus.RAW.value
        
        if verification_status == VerificationStatus.VERIFIED.value:
            # We have a Verified Imported Email.
            if intel_status == VerificationStatus.VERIFIED.value:
                final_status = VerificationStatus.VERIFIED.value # Strongest signal
            else:
                final_status = VerificationStatus.SUSPECT.value # Email looks real, but firm search failed.
                print(f"   ⚠️ Status Downgrade: Email Verified, but Web Presence Weak.")
        
        elif verification_status == VerificationStatus.NOT_VERIFIED.value:
             # Identity Mismatch on Imported Data
             final_status = VerificationStatus.NOT_VERIFIED.value
        
        else:
             # No imported data (RAW), rely on Intel Check
             final_status = intel_status

        # Validation Check using FINAL status
        if final_status == VerificationStatus.NOT_VERIFIED.value: # Replaces QUARANTINED logic
            print(f"   🛑 Skipping Deep Analysis for {full_name} (NOT VERIFIED)")
            
            # Save Audit Trail
            try:
                # We need to ensure columns exist. Main() adds them, but let's be safe
                valid_col = headers.index("Verification Status") + 1
                log_col = headers.index("Validation Log") + 1
                
                sheet.update_cell(i, valid_col, final_status)
                sheet.update_cell(i, log_col, json.dumps(validation_log))
            except Exception as e:
                print(f"   ⚠️ Could not save verification status: {e}")
                
            return False
        
        # Step A.3: Missing Email Logic (The Hunter)
        found_email = row.get('Found Email', '')
        
        # Trigger if empty or "Not Found" logic
        needs_email = not found_email or "Not Found" in found_email or "GUESS" in found_email
        
        if needs_email:
             # Only hunt if the person is verified (Double-Key)
             # or if we are in Search Pending mode.
             if final_status == VerificationStatus.NOT_VERIFIED.value:
                  print(f"   🛑 Skipping Hunter for {full_name} (Identity Not Verified)")
             else:
                 # Try Hunter
                 first = first_name.lower().replace(" ", "")
                 last = last_name.lower().replace(" ", "")
                 # Simple domain guess from firm or use existing
                 domain = "gmail.com" # Placeholder, ideally we extract from search
                 if firm: domain = f"{firm.lower().replace(' ', '').replace(',', '')}.com"
                 
                 found_addr, method = hunter_search(first, last, firm, domain)
                 
                 if found_addr:
                     # SUCCESS
                     new_val = f"[FOUND] {found_addr}\n(Via: {method})"
                     sheet.update_cell(i, headers.index("Found Email")+1, new_val)
                     sheet.update_cell(i, headers.index("Data Source")+1, DataSource.FOUND.value)
                     sheet.update_cell(i, headers.index("Verification Status")+1, VerificationStatus.VERIFIED.value)
                     print(f"   🎯 Hunter Success: {found_addr}")
                 else:
                     # FALLBACK GUESS
                     guess_data = guess_email_logic(full_name, firm, raw_intel)
                     guess = guess_data.get('guess', 'N/A')
                     new_val = f"[GUESS] {guess}"
                     sheet.update_cell(i, headers.index("Found Email")+1, new_val)
                     sheet.update_cell(i, headers.index("Data Source")+1, DataSource.GUESSED.value)
                     # Status remains RAW or what it was

        # Step B: Analysis
        # Extract Client Context EARLY for Sector Weighting
        client_context = None
        if row.get(COL_DATA_SOURCE) == 'F5500_General_Assets' or row.get(COL_CLIENT_NAME):
             client_context = {
                 'source': 'F5500_General_Assets',
                 'client_name': row.get(COL_CLIENT_NAME, 'Unspecified Client'),
                 'funding_type': row.get(COL_FUNDING_TYPE, 'General Assets'),
                 'client_industry': row.get('Industry', 'Unknown') # Assuming this col exists or will be added
             }
        
        # Pass Context to Analysis
        analysis = analyze_lead(raw_intel, full_name, firm, client_context, row.get('Role', 'Broker'))
        
        # Helper to clean text
        def clean(t): return str(t).replace('**', '').replace('__', '')
        
        # Convert JSON to readable text for the sheet
        dossier_text = f"""Archetype: {clean(analysis.get('psych_profile', 'Unknown'))}
Driver: {clean(analysis.get('Unconscious_Desire', 'Unknown'))}
Evidence: {clean(analysis.get('Archetype_Evidence', 'N/A'))}
Hook: {clean(analysis.get('Hook', 'N/A'))}
Pain Points:
- {clean(analysis.get('Pain_Points', [''])[0])}
- {clean(analysis.get('Pain_Points', ['',''])[1])}"""
        
        # Step C: Copywriting
        # (Client Context already extracted above)
             
        email_draft = write_email(analysis, first_name, client_context)
        
        # Step D: Save
        # Update Dossier
        dossier_col = headers.index("Dossier Summary") + 1
        sheet.update_cell(i, dossier_col, dossier_text)
        
        # Update Email
        email_col = headers.index("Draft Email") + 1
        sheet.update_cell(i, email_col, email_draft)
        
        # Update Image
        if image_url:
            img_col = headers.index("Profile Image") + 1
            sheet.update_cell(i, img_col, image_url)
            
        # Update Podcast Info
        podcast_name = analysis.get('Podcast_Name')
        podcast_url = analysis.get('Podcast_URL')
        
        if podcast_name:
            try:
                pod_name_col = headers.index("Podcast Name") + 1
                sheet.update_cell(i, pod_name_col, podcast_name)
            except ValueError: pass
    
        if podcast_url:
            try:
                pod_url_col = headers.index("Podcast URL") + 1
                sheet.update_cell(i, pod_url_col, podcast_url)
            except ValueError: pass
            
        # Update Verification Status (VERIFIED)
        try:
            valid_col = headers.index("Verification Status") + 1
            log_col = headers.index("Validation Log") + 1
            
            sheet.update_cell(i, valid_col, verification_status)
            sheet.update_cell(i, log_col, json.dumps(validation_log))
        except ValueError: pass
        
        print(f"   ✅ Saved Dossier, Draft, and Image for {full_name}")
        return True

        
    except Exception as e:
        print(f"❌ Error processing {full_name}: {e}")
        return False

def main():
    print("--- 🦅 Starting Deep Recon Agent ---")
    
    client = get_google_sheet_client()
    if not client: return
    
    try:
        sheet = client.open(GOOGLE_SHEET_NAME).worksheet(WORKSHEET_NAME)
        data = sheet.get_all_records()
        headers = sheet.row_values(1)
        
        # Add columns if missing
        if "Dossier Summary" not in headers:
            print("   ➕ Adding 'Dossier Summary' column...")
            sheet.update_cell(1, len(headers) + 1, "Dossier Summary")
            headers.append("Dossier Summary")
            
        if "Draft Email" not in headers:
            print("   ➕ Adding 'Draft Email' column...")
            sheet.update_cell(1, len(headers) + 1, "Draft Email")
            headers.append("Draft Email")
            
        if "Profile Image" not in headers:
            print("   ➕ Adding 'Profile Image' column...")
            sheet.update_cell(1, len(headers) + 1, "Profile Image")
            headers.append("Profile Image")

        if "Podcast Name" not in headers:
            print("   ➕ Adding 'Podcast Name' column...")
            sheet.update_cell(1, len(headers) + 1, "Podcast Name")
            headers.append("Podcast Name")
            
        if "Podcast URL" not in headers:
            print("   ➕ Adding 'Podcast URL' column...")
            sheet.update_cell(1, len(headers) + 1, "Podcast URL")
            headers.append("Podcast URL")

        processed_count = 0
        
        for i, row in enumerate(data, start=2):
            # Safety Check
            if processed_count >= SAFETY_LIMIT:
                print(f"\n🛑 SAFETY LIMIT REACHED: Stopped after {processed_count} leads to protect budget.")
                break

            # Filter: LinkedIn URL NOT empty
            linkedin_url = row.get('LinkedIn URL', '')
            if not linkedin_url: continue
                
            # Call the helper function
            if process_single_lead(i, row, headers, sheet):
                processed_count += 1
                
            time.sleep(1) # Rate limiting
            
    except Exception as e:
        print(f"❌ Error accessing sheet: {e}")


def run_forensic_audit():
    """
    Scans the entire database and re-verifies every row using Phase 2 Hunter Logic.
    """
    print("--- 🕵️ Starting Forensic Audit (Phase 2) ---")
    client = get_google_sheet_client()
    if not client: return 0
    
    issues_found = 0
    
    try:
        sheet = client.open(GOOGLE_SHEET_NAME).worksheet(WORKSHEET_NAME)
        data = sheet.get_all_records()
        headers = sheet.row_values(1)
        
        # Ensure all columns exist (Audit Mode)
        for col in [COL_VERIFICATION_STATUS, COL_VALIDATION_LOG, COL_DATA_SOURCE]:
            if col not in headers:
                print(f"   ➕ Adding '{col}' column...")
                sheet.update_cell(1, len(headers) + 1, col)
                headers.append(col)
            
        for i, row in enumerate(data, start=2):
            full_name = f"{row.get('First Name', '')} {row.get('Last Name', '')}"
            firm = row.get('Firm', '')
            linkedin = row.get('LinkedIn URL', '')
            email = row.get('Found Email', '')
            
            print(f"   🔍 Auditing Row {i}: {full_name}")

            # 0. Name Quality Gate (Phase 2 Safeguard)
            last_name_raw = str(row.get('Last Name', '')).strip()
            if len(last_name_raw) < 2:
                 print(f"      ⏩ Skipping - Incomplete Name: '{last_name_raw}'")
                 try:
                     # Mark as Incomplete
                     valid_col = headers.index(COL_VERIFICATION_STATUS) + 1
                     sheet.update_cell(i, valid_col, VerificationStatus.NOT_VERIFIED.value)
                     
                     if email and "INCOMPLETE" not in email:
                         email_col = headers.index("Found Email") + 1
                         sheet.update_cell(i, email_col, f"{email}\n⚠️ [INCOMPLETE DATA]")
                 except: pass
                 continue
            
            # 1. Identity Verification (Email Check)
            email_status = VerificationStatus.RAW.value
            validation_log = {}
            data_source = DataSource.GUESSED.value
            
            is_imported = email and "GUESS" not in email and "[FOUND]" not in email
            if is_imported:
                data_source = DataSource.IMPORTED.value
                is_valid, reason = verify_email_identity(email, full_name)
                validation_log['email_identity_check'] = reason
                if is_valid: 
                     email_status = VerificationStatus.VERIFIED.value
                     print("      ✅ Email Identity Match")
                else: 
                     email_status = VerificationStatus.NOT_VERIFIED.value
                     print(f"      ⚠️ Email Identity Mismatch ({reason})")

            # 2. Firm Verification (Double-Key)
            _, _, firm_status, log = gather_intel(full_name, firm, linkedin)
            if log: validation_log.update(log)
            
            # 3. Final Reconciliation
            final_status = VerificationStatus.RAW.value
            
            if is_imported:
                if email_status == VerificationStatus.VERIFIED.value:
                    # Email match, but what about firm?
                    if firm_status == VerificationStatus.VERIFIED.value:
                        final_status = VerificationStatus.VERIFIED.value
                    else:
                        final_status = VerificationStatus.SUSPECT.value
                        print("      ⚠️ Verified Email, but Firm Check Failed.")
                else:
                    final_status = VerificationStatus.NOT_VERIFIED.value
            else:
                # No imported email, rely on firm check
                final_status = firm_status

            if final_status != VerificationStatus.VERIFIED.value:
                issues_found += 1

            # 4. Save Updates
            try:
                sheet.update_cell(i, headers.index(COL_VERIFICATION_STATUS) + 1, final_status)
                sheet.update_cell(i, headers.index(COL_VALIDATION_LOG) + 1, json.dumps(validation_log))
                # Only update Data Source if it's new/imported
                if is_imported:
                    sheet.update_cell(i, headers.index(COL_DATA_SOURCE) + 1, data_source)
                
                # --- CLEANUP (Anti-Hallucination) ---
                # --- CLEANUP (Anti-Hallucination) ---
                if final_status == VerificationStatus.NOT_VERIFIED.value:
                    print(f"      🧹 Wiping generated assets for {full_name}")
                    try:
                        sheet.update_cell(i, headers.index("Profile Image")+1, "")
                        sheet.update_cell(i, headers.index("Dossier Summary")+1, f"⚠️ SCOUT AGENT: {explain_failure(full_name, firm)}")
                        
                        # Label the Email Field (Without Deleting)
                        if email and "Mismatch" not in email:
                             email_col = headers.index("Found Email") + 1
                             # Append visible warning
                             new_val = f"{email}\n⚠️ [NOT VERIFIED: Identity Mismatch]"
                             sheet.update_cell(i, email_col, new_val)
                    except: pass
                    
            except Exception as e:
                print(f"      ❌ Save failed: {e}")
                
            time.sleep(1)
            
    except Exception as e:
        print(f"❌ Audit failed: {e}")
        
    return issues_found

if __name__ == "__main__":
    main()
