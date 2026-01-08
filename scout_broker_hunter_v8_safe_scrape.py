import csv
import os
import time
import requests
import json
import re
import sys
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
SERPER_API_KEY = os.getenv("SERPER_API_KEY")
INPUT_FILE = os.getenv("INPUT_FILE") 
INPUT_FILE_PATTERN = "scout_v7_final_"
OUTPUT_FILENAME = "Morning_Briefing_With_Brokers.csv"
MAX_ROWS = int(os.getenv("MAX_ROWS", 10)) 

# Canonical Map (Token -> Display Name)
BIG_BROKERS = {
    "MARSH": "Marsh", "MERCER": "Mercer", "AON": "Aon", 
    "WILLIS TOWERS WATSON": "Willis Towers Watson (WTW)", "WTW": "Willis Towers Watson (WTW)",
    "GALLAGHER": "Arthur J. Gallagher", "LOCKTON": "Lockton Companies",
    "HUB INTERNATIONAL": "HUB International", "ALLIANT": "Alliant Insurance Services",
    "USI": "USI Insurance Services", "ONE DIGITAL": "OneDigital",
    "NFP": "NFP", "SEQUOIA": "Sequoia Consulting", "WOODRUFF SAWYER": "Woodruff Sawyer",
    "ABD": "Newfront (ABD)", "NEWFRONT": "Newfront"
}

# Domain Whitelist for Safe Scraping (Token -> Domain Substring)
BROKER_DOMAINS = {
    "MARSH": "marsh.com", "MERCER": "mercer.com", "AON": "aon.com",
    "WILLIS TOWERS WATSON": "wtwco.com", "WTW": "wtwco.com",
    "GALLAGHER": "ajg.com", "LOCKTON": "lockton.com",
    "HUB INTERNATIONAL": "hubinternational.com", "ALLIANT": "alliant.com",
    "USI": "usi.com", "ONE DIGITAL": "onedigital.com",
    "NFP": "nfp.com", "SEQUOIA": "sequoia.com", "WOODRUFF SAWYER": "woodruffsawyer.com",
    "ABD": "newfront.com", "NEWFRONT": "newfront.com"
}

FIRM_PATTERNS = r"([A-Z][\w&']+(?:\s[A-Z][\w&']+){0,2})\s(BENEFITS|CONSULTING|INSURANCE|RISK|BROKERAGE|ADVISORS)"

GENERIC_TERMS = {
    "EMPLOYEE", "EMPLOYEES", "BENEFITS", "TOTAL", "REWARDS", "CONSULTING", "GROUP", "SERVICES", 
    "SOLUTIONS", "INSURANCE", "RISK", "DEPARTMENT", "TEAM", "HUMAN", "RESOURCES", 
    "ADVISORS", "BROKERAGE", "PROGRAM", "PARTNERS", "MANAGEMENT", "HEALTH", "CARE",
    "COMPENSATION", "DATA", "ANALYTICS", "STRATEGY", "CORPORATE", "GLOBAL"
}

class BrokerHunter:
    def __init__(self):
        if not SERPER_API_KEY:
            print("❌ CRITICAL: SERPER_API_KEY not found. Exiting.")
            sys.exit(1)
        self.search_cache = {}

    def clean_text(self, text):
        return re.sub(r'\s+', ' ', text).strip().upper()

    def clean_company_name(self, name):
        name = re.sub(r'(?i)\s+(inc|corp|corporation|llc|ltd|co)\.?$', '', name)
        return name.strip()

    def extract_email(self, text):
        match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
        return match.group(0) if match else None

    def search_google(self, query):
        if query in self.search_cache: return self.search_cache[query]
        
        url = "https://google.serper.dev/search"
        payload = json.dumps({"q": query, "num": 10}) 
        headers = {'X-API-KEY': SERPER_API_KEY, 'Content-Type': 'application/json'}
        
        retries = 3
        backoff = 2
        
        for i in range(retries):
            try:
                response = requests.request("POST", url, headers=headers, data=payload)
                if response.status_code == 200:
                    data = response.json()
                    self.search_cache[query] = data
                    return data
                elif response.status_code == 429:
                    print(f"   [429 Rate Limit] Cooling down {backoff}s...")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                else:
                    print(f"   [API Error {response.status_code}]")
                    return {}
            except Exception as e:
                print(f"   [Transport Error] {e}")
                time.sleep(1)
        return {}

    def is_safe_domain(self, url, firm_token):
        """Strict Gate: Only allow scraping if URL matches known broker domain."""
        if firm_token not in BROKER_DOMAINS:
            return False # Unknown firm = No scraping
            
        allowed_domain = BROKER_DOMAINS[firm_token]
        try:
            netloc = urlparse(url).netloc.lower()
            return allowed_domain in netloc
        except:
            return False

    def fetch_page_content_safe(self, url):
        """Safe scraping: Size capped, Text only, No binaries."""
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Compatible; Scout/1.0)'}
            with requests.get(url, headers=headers, timeout=5, stream=True) as r:
                ctype = r.headers.get('Content-Type', '').lower()
                if 'application/pdf' in ctype or 'image' in ctype or 'binary' in ctype:
                    return ""
                
                clength = r.headers.get('Content-Length')
                if clength and int(clength) > 2 * 1024 * 1024:
                    return ""

                content = []
                max_bytes = 1024 * 500 # 500KB cap
                current_bytes = 0
                
                for chunk in r.iter_content(chunk_size=4096):
                    content.append(chunk)
                    current_bytes += len(chunk)
                    if current_bytes > max_bytes: break
                
                return b"".join(content).decode('utf-8', errors='ignore')
        except:
            return ""

    def validate_firm_name(self, potential_firm, employer_clean):
        firm_tokens = set(potential_firm.upper().split())
        employer_tokens = set(employer_clean.upper().split())
        brand_tokens = firm_tokens - GENERIC_TERMS
        
        if not brand_tokens: return False
        if brand_tokens.issubset(employer_tokens): return False
        return True

    def check_co_occurrence(self, text, employer_clean, firm_token):
        text_tokens = set(re.findall(r'\w+', text))
        if firm_token not in text: return False
        
        emp_tokens = [t for t in employer_clean.upper().split() if len(t) > 2 and t not in GENERIC_TERMS]
        if not emp_tokens: return employer_clean.upper() in text 
        
        match_count = sum(1 for t in emp_tokens if t in text_tokens)
        return match_count >= 1

    def find_broker_firm(self, employer):
        clean_name = self.clean_company_name(employer)
        queries = [
            f"{clean_name} employee benefits broker",
            f"{clean_name} benefits consultant filetype:pdf",
            f"{clean_name} total rewards case study"
        ]
        
        candidates = {} 
        
        for q in queries:
            results = self.search_google(q)
            if not results: continue
            
            for item in results.get("organic", []):
                title = self.clean_text(item.get("title", ""))
                snippet = self.clean_text(item.get("snippet", ""))
                link = item.get("link", "")
                full_text = f"{title} {snippet}"
                
                found_canonical = None
                found_token = None
                
                # 1. Big Brokers Check
                for token, canonical in BIG_BROKERS.items():
                    if token in full_text:
                        found_canonical = canonical
                        found_token = token
                        break
                
                # 2. Generic Pattern Check
                if not found_canonical:
                    match = re.search(FIRM_PATTERNS, full_text)
                    if match:
                        potential_firm = match.group(0).strip()
                        if self.validate_firm_name(potential_firm, clean_name):
                            found_canonical = potential_firm
                            found_token = potential_firm # Use same for token

                if found_canonical:
                    if found_canonical not in candidates:
                        candidates[found_canonical] = {"score": 0, "evidence": [], "co_occurrence": False, "token": found_token}
                    
                    is_co_occurrent = self.check_co_occurrence(full_text, clean_name, found_token)
                    
                    candidates[found_canonical]["score"] += 1
                    if is_co_occurrent:
                        candidates[found_canonical]["co_occurrence"] = True
                        if link not in candidates[found_canonical]["evidence"]:
                            candidates[found_canonical]["evidence"].append(link)
                    elif not candidates[found_canonical]["evidence"]:
                        candidates[found_canonical]["evidence"].append(link)

            time.sleep(0.1)

        if not candidates:
            return "UNKNOWN", "UNKNOWN", "Low", [], "UNKNOWN", ""

        sorted_firms = sorted(
            candidates.items(), 
            key=lambda x: (x[1]['co_occurrence'], x[1]['score']), 
            reverse=True
        )
        
        best_canonical, data = sorted_firms[0]
        best_token = data['token']
        
        if data['co_occurrence']:
            confidence = "High"
            final_evidence = data['evidence']
        elif data['score'] >= 2:
            confidence = "Medium"
            final_evidence = data['evidence']
        else:
            confidence = "Low"
            final_evidence = data['evidence']

        status = "FOUND"
        cand_str = " | ".join([f"{k} [Token={v['token']}] (Score:{v['score']}, CoOcc:{v['co_occurrence']})" for k,v in sorted_firms[:3]])
        
        return best_canonical, best_token, confidence, final_evidence[:3], status, cand_str

    def find_broker_person(self, employer, firm_token, firm_canonical):
        if firm_token == "UNKNOWN": return [], "UNKNOWN"
        clean_employer = self.clean_company_name(employer)
        
        query_firm = f'("{firm_token}" OR "{firm_canonical}")'
        query = f'site:linkedin.com {query_firm} "{clean_employer}" (consultant OR executive OR "account manager")'
        
        results = self.search_google(query)
        candidates = []
        
        for item in results.get("organic", []):
            title = self.clean_text(item.get("title", ""))
            snippet = self.clean_text(item.get("snippet", ""))
            link = item.get("link", "")
            
            if "LINKEDIN" in title:
                parts = re.split(r' [-|] ', title)
                if len(parts) < 2: continue
                
                name = parts[0].title()
                job_title = parts[1]
                
                if any(x in job_title.upper() for x in ["ASSISTANT", "INTERN", "RECRUITER", "ANALYST"]):
                    continue
                
                is_high_conf = self.check_co_occurrence(title + snippet, clean_employer, firm_token)
                p_conf = "High" if is_high_conf else "Medium"
                
                candidates.append({
                    "name": name,
                    "title": job_title,
                    "linkedin": link,
                    "confidence": p_conf
                })
                if len(candidates) >= 3: break
        
        if candidates:
            return candidates, "FOUND"
        return [], "UNKNOWN"

    def find_broker_email(self, employer, firm_token, evidence_urls):
        if firm_token == "UNKNOWN": return "", "MISSING", ""
        
        # 1. Search Queries (Snippets - Always Safe)
        queries = [
            f'"{firm_token}" "{employer}" email benefits',
            f'"{firm_token}" contact email'
        ]
        
        for q in queries:
            results = self.search_google(q)
            for item in results.get("organic", []):
                snippet = item.get("snippet", "")
                email = self.extract_email(snippet)
                if email and "EXAMPLE.COM" not in email.upper():
                    return email, "FOUND", item.get("link")
        
        # 2. Active Scraping (Strict Domain Gate)
        if evidence_urls:
            target_url = evidence_urls[0]
            
            # THE SAFETY LOCK: Only scrape if domain matches known broker
            if self.is_safe_domain(target_url, firm_token):
                content = self.fetch_page_content_safe(target_url)
                email = self.extract_email(content)
                if email and "EXAMPLE.COM" not in email.upper():
                     return email, "FOUND", target_url

        return "", "MISSING", ""

    def run(self):
        if INPUT_FILE:
            input_file = INPUT_FILE
        else:
            files = [f for f in os.listdir('.') if f.startswith(INPUT_FILE_PATTERN) and f.endswith('.csv')]
            if not files: print("❌ CRITICAL: No Input File. Set INPUT_FILE env var."); sys.exit(1)
            input_file = max(files, key=os.path.getmtime)
        
        print(f"🚀 HUNTING BROKERS for: {input_file}")
        print(f"   Max Rows: {MAX_ROWS}")
        
        enriched_rows = []
        
        with open(input_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            count = 0
            
            for row in reader:
                if count >= MAX_ROWS: break
                
                employer = row.get('Client', 'Unknown')
                original_action = row.get('Action')
                
                # Init
                b_firm_name = ""
                b_firm_token = ""
                b_contact_name = ""
                b_contact_title = ""
                b_email = "" 
                b_linkedin = ""
                b_status = "UNKNOWN"
                b_conf = "Low"
                b_evidence = ""
                b_email_evidence = ""
                b_firm_cands = ""
                b_email_status = "MISSING"
                b_compliance = "Do not contact employer directly. Route via broker."
                new_action = "MANUAL_RESEARCH"
                
                if original_action in ["SEND", "REVIEW"]:
                    print(f"   🔍 Analyzing: {employer}...")
                    
                    # 1. Firm
                    firm_canon, firm_tok, conf, evidence, f_status, cand_str = self.find_broker_firm(employer)
                    
                    b_firm_name = firm_canon if firm_canon != "UNKNOWN" else ""
                    b_firm_token = firm_tok if firm_tok != "UNKNOWN" else ""
                    b_conf = conf
                    b_evidence = " | ".join(evidence)
                    b_firm_cands = cand_str
                    b_status = "CANDIDATES" if firm_canon != "UNKNOWN" else "UNKNOWN"
                    
                    # 2. Person
                    if b_firm_token:
                        candidates, p_status = self.find_broker_person(employer, b_firm_token, firm_canon)
                        if candidates:
                            top = candidates[0]
                            b_contact_name = top['name']
                            b_contact_title = top['title']
                            b_linkedin = top['linkedin']
                            
                            if top['confidence'] == "High":
                                b_status = "FOUND"
                            else:
                                b_status = "CANDIDATES"
                        
                        # 3. Email
                        email, e_status, e_evid_url = self.find_broker_email(employer, b_firm_token, evidence)
                        b_email = email
                        b_email_status = e_status
                        b_email_evidence = e_evid_url

                # Action Logic
                if original_action in ["SEND", "REVIEW"]:
                    if b_status in ["FOUND", "CANDIDATES"]:
                        new_action = "CONTACT_BROKER"
                    else:
                        new_action = "MANUAL_RESEARCH"
                    final_action = new_action
                else:
                    final_action = original_action

                # Build Row
                new_row = row.copy()
                new_row['Broker_Firm_Name'] = b_firm_name
                new_row['Broker_Firm_Token'] = b_firm_token
                new_row['Broker_Contact_Name'] = b_contact_name
                new_row['Broker_Contact_Title'] = b_contact_title
                new_row['Broker_Email'] = b_email 
                new_row['Broker_LinkedIn'] = b_linkedin
                new_row['Broker_Discovery_Status'] = b_status
                new_row['Broker_Confidence'] = b_conf
                new_row['Broker_Evidence_URLs'] = b_evidence
                new_row['Broker_Email_Evidence_URLs'] = b_email_evidence
                new_row['Broker_Firm_Candidates'] = b_firm_cands
                new_row['Broker_Email_Status'] = b_email_status
                new_row['Compliance_Notice'] = b_compliance
                new_row['Action'] = final_action 
                
                enriched_rows.append(new_row)
                count += 1
                
        # Write Output
        if enriched_rows:
            field_order = [
                "Client", "Action", "Broker_Discovery_Status", "Broker_Firm_Name", 
                "Broker_Contact_Name", "Broker_Contact_Title", "Broker_Email", "Broker_LinkedIn",
                "Broker_Confidence", "Broker_Evidence_URLs", "Broker_Email_Evidence_URLs",
                "Compliance_Notice", "Broker_Firm_Candidates", "Broker_Firm_Token"
            ]
            existing_keys = list(enriched_rows[0].keys())
            for k in existing_keys:
                if k not in field_order: field_order.append(k)

            with open(OUTPUT_FILENAME, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=field_order)
                writer.writeheader()
                writer.writerows(enriched_rows)
            print(f"\n✅ BROKER HUNT COMPLETE. Output: {OUTPUT_FILENAME}")
        else:
            print("❌ No rows processed.")

if __name__ == "__main__":
    with open("broker_debug.log", "w") as log:
        log.write("🚀 LOG START: Broker Hunter launching...\n")
        try:
            print("🔧 DEBUG: Starting Scraper...")
            print(f"🔧 DEBUG: INPUT_FILE env var: {os.getenv('INPUT_FILE')}")
            log.write(f"🔧 DEBUG: INPUT_FILE env var: {os.getenv('INPUT_FILE')}\n")
            hunter = BrokerHunter()
            hunter.run()
            log.write("✅ LOG END: Broker Hunter finished.\n")
        except Exception as e:
            log.write(f"❌ CRITICAL ERROR: {e}\n")
            import traceback
            log.write(traceback.format_exc())
            print(f"❌ CRITICAL ERROR: {e}")
