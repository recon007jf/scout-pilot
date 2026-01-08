import re
import requests
import json
import time
import os
import sys
from urllib.parse import urlparse
from collections import Counter

# --- CONSTANTS ---
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

# IMPORTS
import json
import os

# STOPWORDS for "The Bouncer" - These are NOT brands.
STOPWORDS = {
    'THE','A','AN','AND','OR','OF','FOR','TO','IN','ON','AT','BY','WITH',
    'WHAT','THESE','THOSE','YOUR','OUR','MY','THEIR','WHO','WHICH',
    'LLC','INC','CORP','CORPORATION','CO','LTD'
}

GENERIC_TERMS = {
    'COMPENSATION', 'BENEFIT', 'BENEFITS', 'INSURANCE', 'SERVICES', 'SOLUTIONS', 
    'PARTNERS', 'MANAGEMENT', 'CONSULTING', 'ADVISORS', 'BROKERAGE', 'PROGRAM', 
    'TEAM', 'GROUP', 'FINANCIAL', 'WEALTH', 'RETIREMENT', 'TOTAL', 'REWARDS',
    'COMPANY', 'DEPARTMENT', 'HUMAN', 'RESOURCES', 'STRATEGY', 'BUSINESS', 
    'GLOBAL', 'CORPORATE', 'DATA', 'ANALYTICS', 'CARE', 'HEALTH', 'PLAN'
}

REL_SINGLE_WORD = {"CLIENT", "PARTNER", "BROKER", "CONSULTANT", "ADVISOR", "BENEFITS", "PLAN", "WORKED", "SERVED", "SELECTED"}
REL_MULTI_WORD = {"CASE STUDY", "BENEFITS PROGRAM", "RECORD KEEPER", "TOTAL REWARDS"}

class BrokerHunter:
    def __init__(self, serper_api_key):
        self.api_key = serper_api_key
        if not self.api_key:
            raise ValueError("SERPER_API_KEY is required")
        self.search_cache = {}

    def clean_text(self, text): 
        return re.sub(r'\s+', ' ', text).strip().upper()

    def clean_company_name(self, name): 
        return re.sub(r'(?i)\s+(inc|corp|corporation|llc|ltd|co)\.?$', '', name).strip()

    def extract_email(self, text):
        match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
        return match.group(0) if match else None

    def search_google(self, query):
        if query in self.search_cache: return self.search_cache[query]
        url = "https://google.serper.dev/search"
        payload = json.dumps({"q": query, "num": 10}) 
        headers = {'X-API-KEY': self.api_key, 'Content-Type': 'application/json'}
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
        """
        The Bouncer (v9.x): Strictly validates potential firm names.
        Rule: Must have >= 2 BRAND tokens (non-stopword, non-generic)
        UNLESS it is a known Big Broker.
        """
        upper_firm = potential_firm.upper()
        
        # 1. Big Broker Bypass (Trust List)
        for bb in BIG_BROKERS:
            if bb in upper_firm:
                return True

        # 2. Token Analysis
        # Strip punctuation for cleaner tokenization
        clean_firm = re.sub(r'[^\w\s]', '', upper_firm)
        raw_tokens = clean_firm.split()
        
        # Filter out STOPWORDS and GENERIC_TERMS
        brand_tokens = [t for t in raw_tokens if t not in STOPWORDS and t not in GENERIC_TERMS]
        
        # 3. Minimum Brand Token Rule
        if len(brand_tokens) < 2:
            # "The Benefits Services" -> 0 tokens -> REJECT
            # "Summit" -> 1 token -> REJECT (Too risky for inference, unless big broker)
            return False
            
        return True

    def calculate_confidence(self, text, employer_clean, firm_token):
        if not re.search(r'\b' + re.escape(firm_token) + r'\b', text): return "None"
        
        text_tokens = set(re.findall(r'\b\w+\b', text))
        emp_raw_tokens = [t for t in re.findall(r'\b\w+\b', employer_clean.upper()) if len(t) > 2 and t not in GENERIC_TERMS]
        
        if not emp_raw_tokens:
             match_count = 1 if re.search(r'\b' + re.escape(employer_clean.upper()) + r'\b', text) else 0
        else:
             match_count = sum(1 for t in emp_raw_tokens if t in text_tokens)

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
