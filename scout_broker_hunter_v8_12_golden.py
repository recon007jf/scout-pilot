import csv, os, time, requests, json, re, sys
from urllib.parse import urlparse
from collections import Counter
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
SERPER_API_KEY = os.getenv("SERPER_API_KEY")
INPUT_FILE = os.getenv("INPUT_FILE") 
INPUT_FILE_PATTERN = "scout_v7_final_"
OUTPUT_FILENAME = "Morning_Briefing_With_Brokers.csv"
MAX_ROWS = int(os.getenv("MAX_ROWS", 10)) 

# Canonical Map
BIG_BROKERS = {
    "MARSH": "Marsh", "MERCER": "Mercer", "AON": "Aon", 
    "WILLIS TOWERS WATSON": "Willis Towers Watson (WTW)", "WTW": "Willis Towers Watson (WTW)",
    "GALLAGHER": "Arthur J. Gallagher", "LOCKTON": "Lockton Companies",
    "HUB INTERNATIONAL": "HUB International", "ALLIANT": "Alliant Insurance Services",
    "USI": "USI Insurance Services", "ONE DIGITAL": "OneDigital",
    "NFP": "NFP", "SEQUOIA": "Sequoia Consulting", "WOODRUFF SAWYER": "Woodruff Sawyer",
    "ABD": "Newfront (ABD)", "NEWFRONT": "Newfront"
}

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
    "COMPENSATION", "DATA", "ANALYTICS", "STRATEGY", "CORPORATE", "GLOBAL",
    "BUSINESS", "FINANCIAL", "MONETARY", "WEALTH", "RETIREMENT"
}
# Split for Regex Safety
REL_SINGLE_WORD = {"CLIENT", "PARTNER", "BROKER", "CONSULTANT", "ADVISOR", "BENEFITS", "PLAN", "WORKED", "SERVED", "SELECTED"}
REL_MULTI_WORD = {"CASE STUDY", "BENEFITS PROGRAM", "RECORD KEEPER", "TOTAL REWARDS"}

class BrokerHunter:
    def __init__(self):
        try:
            with open("broker_hunter.log", "w") as f: f.write(f"INIT v8.12 Golden: {os.times()}\n")
        except: pass
        
        if not SERPER_API_KEY: print("❌ CRITICAL: SERPER_API_KEY not found."); sys.exit(1)
        self.search_cache = {}

    def clean_text(self, text): return re.sub(r'\s+', ' ', text).strip().upper()
    def clean_company_name(self, name): return re.sub(r'(?i)\s+(inc|corp|corporation|llc|ltd|co)\.?$', '', name).strip()
    def extract_email(self, text):
        match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
        return match.group(0) if match else None

    def search_google(self, query):
        if query in self.search_cache: return self.search_cache[query]
        url = "https://google.serper.dev/search"
        payload = json.dumps({"q": query, "num": 10}) 
        headers = {'X-API-KEY': SERPER_API_KEY, 'Content-Type': 'application/json'}
        retries = 3; backoff = 2
        for i in range(retries):
            try:
                response = requests.request("POST", url, headers=headers, data=payload)
                if response.status_code == 200:
                    data = response.json(); self.search_cache[query] = data; return data
                elif response.status_code == 429: time.sleep(backoff); backoff *= 2; continue
            except: time.sleep(1)
        return {}

    def is_safe_domain(self, url, firm_token):
        if firm_token not in BROKER_DOMAINS: return False
        try: return BROKER_DOMAINS[firm_token] in urlparse(url).netloc.lower()
        except: return False

    def fetch_page_content_safe(self, url):
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Compatible; Scout/1.0)'}
            with requests.get(url, headers=headers, timeout=5, stream=True) as r:
                ctype = r.headers.get('Content-Type', '').lower()
                if any(x in ctype for x in ['application/pdf', 'image', 'binary']): return ""
                if int(r.headers.get('Content-Length', 0)) > 2*1024*1024: return ""
                content = []; max_bytes = 1024*500; current = 0
                for chunk in r.iter_content(chunk_size=4096):
                    content.append(chunk); current += len(chunk)
                    if current > max_bytes: break
                return b"".join(content).decode('utf-8', errors='ignore')
        except: return ""

    def validate_firm_name(self, potential_firm, employer_clean):
        brand_tokens = set(potential_firm.upper().split()) - GENERIC_TERMS
        employer_tokens = set(employer_clean.upper().split())
        
        if not brand_tokens: return False
        if len(brand_tokens) < 2: return False 
        if brand_tokens.issubset(employer_tokens): return False
        return True

    def calculate_confidence(self, text, employer_clean, firm_token):
        # 1. Strict Firm Match (Word Boundary)
        if not re.search(r'\b' + re.escape(firm_token) + r'\b', text): return "None"
        
        # 2. Employer Match (Token Set Intersection)
        text_tokens = set(re.findall(r'\b\w+\b', text))
        emp_raw_tokens = [t for t in re.findall(r'\b\w+\b', employer_clean.upper()) if len(t) > 2 and t not in GENERIC_TERMS]
        
        if not emp_raw_tokens:
             match_count = 1 if re.search(r'\b' + re.escape(employer_clean.upper()) + r'\b', text) else 0
        else:
             match_count = sum(1 for t in emp_raw_tokens if t in text_tokens)

        # 3. Relationship Match (Strict Boundaries)
        has_rel = False
        for term in REL_SINGLE_WORD:
            if re.search(r'\b' + re.escape(term) + r'\b', text): has_rel = True; break
        if not has_rel:
            for phrase in REL_MULTI_WORD:
                if phrase in text: has_rel = True; break
        
        if match_count >= 2: return "High"
        if match_count == 1 and has_rel: return "High"
        if match_count >= 1: return "Medium"
        return "Low"

    def find_broker_firm(self, employer):
        clean_name = self.clean_company_name(employer)
        queries = [f"{clean_name} employee benefits broker", f"{clean_name} benefits consultant filetype:pdf", f"{clean_name} total rewards case study"]
        candidates = {} 
        
        for q in queries:
            results = self.search_google(q)
            if not results: continue
            for item in results.get("organic", []):
                full_text = self.clean_text(item.get("title", "") + " " + item.get("snippet", ""))
                found_canonical = None; found_token = None
                
                # Check Big Brokers (Word Boundary)
                for token, canonical in BIG_BROKERS.items():
                    if re.search(r'\b' + re.escape(token) + r'\b', full_text):
                        found_canonical = canonical; found_token = token; break
                
                # Check Pattern
                if not found_canonical:
                    match = re.search(FIRM_PATTERNS, full_text)
                    if match:
                        pot = match.group(0).strip()
                        if self.validate_firm_name(pot, clean_name): found_canonical = pot; found_token = pot

                if found_canonical:
                    if found_canonical not in candidates: candidates[found_canonical] = {"score": 0, "evidence": [], "max_conf": "None", "token": found_token}
                    
                    conf = self.calculate_confidence(full_text, clean_name, found_token)
                    candidates[found_canonical]["score"] += 1
                    
                    current_max = candidates[found_canonical]["max_conf"]
                    if conf == "High": candidates[found_canonical]["max_conf"] = "High"
                    elif conf == "Medium" and current_max != "High": candidates[found_canonical]["max_conf"] = "Medium"
                    elif conf == "Low" and current_max == "None": candidates[found_canonical]["max_conf"] = "Low"
                    
                    if conf != "None":
                         candidates[found_canonical]["evidence"].append(item.get("link", ""))

        if not candidates: return "UNKNOWN", "UNKNOWN", "Low", [], "UNKNOWN", ""
        
        def conf_rank(c): return 3 if c == "High" else (2 if c == "Medium" else 1)
        sorted_firms = sorted(candidates.items(), key=lambda x: (conf_rank(x[1]['max_conf']), x[1]['score']), reverse=True)
        
        best, data = sorted_firms[0]
        final_conf = data['max_conf']
        if final_conf == "None": return "UNKNOWN", "UNKNOWN", "Low", [], "UNKNOWN", ""
        
        return best, data['token'], final_conf, data['evidence'][:1], "FOUND", "" 

    def find_broker_person(self, employer, firm_token, firm_canon):
        if firm_token == "UNKNOWN": return [], "UNKNOWN"
        clean = self.clean_company_name(employer)
        query = f'site:linkedin.com ("{firm_token}" OR "{firm_canon}") "{clean}" (consultant OR executive OR "account manager")'
        results = self.search_google(query); candidates = []
        for item in results.get("organic", []):
            title = self.clean_text(item.get("title", ""))
            if "LINKEDIN" in title:
                parts = re.split(r' [-|] ', title)
                if len(parts) < 2: continue
                name = parts[0].title(); job = parts[1]
                if any(x in job.upper() for x in ["ASSISTANT", "INTERN", "RECRUITER", "ANALYST"]): continue
                conf = self.calculate_confidence(title + item.get("snippet","").upper(), clean, firm_token)
                candidates.append({"name": name, "title": job, "linkedin": item.get("link"), "confidence": conf})
                if len(candidates) >= 3: break
        return candidates, "FOUND" if candidates else "UNKNOWN"

    def find_broker_email(self, employer, firm_token, evidence_urls):
        if firm_token == "UNKNOWN": return "", "MISSING", ""
        queries = [f'"{firm_token}" "{employer}" email benefits', f'"{firm_token}" contact email']
        for q in queries:
            results = self.search_google(q)
            for item in results.get("organic", []):
                email = self.extract_email(item.get("snippet", ""))
                if email and "EXAMPLE.COM" not in email.upper(): return email, "FOUND", item.get("link")
        
        if evidence_urls:
            url = evidence_urls[0]
            if self.is_safe_domain(url, firm_token):
                content = self.fetch_page_content_safe(url)
                email = self.extract_email(content)
                if email and "EXAMPLE.COM" not in email.upper(): return email, "FOUND", url
        return "", "MISSING", ""

    def run(self):
        print("🚀 HUNTING BROKERS v8.12 (Golden Master)...", flush=True)
        if INPUT_FILE: input_file = INPUT_FILE
        else:
            files = [f for f in os.listdir('.') if f.startswith(INPUT_FILE_PATTERN) and f.endswith('.csv')]
            if not files: print("❌ CRITICAL: No v7 results found. Set INPUT_FILE."); sys.exit(1)
            input_file = max(files, key=os.path.getmtime)
        
        print(f"   Target: {input_file} | Max Rows: {MAX_ROWS}", flush=True)
        enriched = []
        broker_counts = Counter()
        
        with open(input_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            
            # --- HEADER ASSERTION (PM Requirement) ---
            required_cols = {'Target_Name', 'Target_Title', 'Target_Email', 'Target_LinkedIn'}
            if not required_cols.issubset(set(reader.fieldnames)):
                missing = required_cols - set(reader.fieldnames)
                print(f"❌ CRITICAL: Input CSV missing required columns: {missing}", flush=True)
                print(f"   Found headers: {reader.fieldnames}", flush=True)
                sys.exit(1)
            # ----------------------------------------
            
            count = 0
            for row in reader:
                if count >= MAX_ROWS: break
                emp = row.get('Client', 'Unknown'); action = row.get('Action')
                
                # INTERNAL STATE 
                b_res = {"Firm": "", "Token": "", "Contact": "", "Title": "", "Email": "", "Linked": "", "Status": "UNKNOWN", "Conf": "Low", "Evid": "", "Action": "MANUAL_RESEARCH", "Anomaly": ""}
                
                if action in ["SEND", "REVIEW"]:
                    print(f"   🔍 Analyzing: {emp}...", flush=True)
                    canon, tok, conf, evid, stat, _ = self.find_broker_firm(emp)
                    
                    if canon != "UNKNOWN": broker_counts[canon] += 1
                    
                    b_res.update({"Firm": canon, "Token": tok, "Conf": conf, "Evid": " | ".join(evid), "Status": "CANDIDATES" if canon!="UNKNOWN" else "UNKNOWN"})
                    
                    if tok != "UNKNOWN":
                        people, _ = self.find_broker_person(emp, tok, canon)
                        if people:
                            top = people[0]
                            b_res.update({"Contact": top['name'], "Title": top['title'], "Linked": top['linkedin'], "Status": "FOUND" if top['confidence']=="High" else "CANDIDATES"})
                        
                        email, _, _ = self.find_broker_email(emp, tok, evid)
                        b_res.update({"Email": email})

                if action in ["SEND", "REVIEW"]: b_res["Action"] = "CONTACT_BROKER" if b_res["Status"] in ["FOUND", "CANDIDATES"] else "MANUAL_RESEARCH"
                else: b_res["Action"] = action

                # --- CLIENT OUTPUT MAPPING (Clean) ---
                client_row = {
                    "Client": emp,
                    "Action": b_res["Action"],
                    "Reason": row.get('Reason'),
                    "Name": row.get('Target_Name'),
                    "Title": row.get('Target_Title'),
                    "Email": row.get('Target_Email'),
                    "LinkedIn": row.get('Target_LinkedIn'),
                    "Broker Firm": b_res["Firm"],
                    "Broker Contact Name": b_res["Contact"],
                    "Broker Contact Title": b_res["Title"],
                    "Broker Email": b_res["Email"],
                    "Broker LinkedIn": b_res["Linked"],
                    "Broker Confidence": b_res["Conf"],
                    "Broker Discovery Status": b_res["Status"],
                    "Evidence URL": b_res["Evid"],
                    "Broker Anomaly Flag": "" 
                }
                enriched.append(client_row); count += 1
        
        # ANOMALY CHECK (Flag Only)
        total_found = sum(broker_counts.values())
        if total_found > 3: 
            for row in enriched:
                firm = row.get('Broker Firm')
                if firm and firm != "UNKNOWN" and firm != "":
                    if broker_counts[firm] / total_found > 0.4:
                         row['Broker Anomaly Flag'] = "REVIEW_BULK_ASSIGNMENT"

        if enriched:
            # STRICT HEADER ORDER
            headers = [
                "Client", "Action", "Reason", "Name", "Title", "Email", "LinkedIn",
                "Broker Firm", "Broker Contact Name", "Broker Contact Title", "Broker Email", 
                "Broker LinkedIn", "Broker Confidence", "Broker Discovery Status", 
                "Evidence URL", "Broker Anomaly Flag"
            ]
            
            with open(OUTPUT_FILENAME, 'w', newline='') as f: writer = csv.DictWriter(f, fieldnames=headers); writer.writeheader(); writer.writerows(enriched)
            print(f"\n✅ CLIENT ARTIFACT COMPLETE. Output: {OUTPUT_FILENAME}", flush=True)
            print("Summary of Findings:", flush=True)
            print(broker_counts, flush=True)
        else: print("❌ No rows processed.", flush=True)

if __name__ == "__main__": BrokerHunter().run()
