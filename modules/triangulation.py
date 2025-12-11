import pandas as pd
import time
from utils.serper import search_google
from collections import Counter

class TriangulationEngine:
    def __init__(self):
        self.west_states = ['CA', 'WA', 'OR', 'ID', 'UT', 'CO', 'AZ', 'NV', 'NM']
        
    def gather_regional_signals(self, employer_name, state, ein=None):
        """
        Gathers public intelligence signals for a specific employer.
        """
        print(f"📡 Triangulating Broker for: {employer_name} ({state})")
        
        signals = []
        candidates = []
        
        # 1. RFP SIGNAL (High Confidence)
        # Query: "Acme Corp" "WA" "benefits broker" RFP
        q_rfp = f'"{employer_name}" "{state}" "benefits broker" RFP assignment'
        res_rfp = search_google(q_rfp)
        
        if 'organic' in res_rfp:
            for r in res_rfp['organic'][:3]: # Top 3 only
                snippet = r.get('snippet', '').lower()
                title = r.get('title', '').lower()
                link = r.get('link', '')
                
                # Simple extraction of common firms (Naive NER)
                # In prod, use a list of known firms to match against
                possible_firm = self._extract_firm_from_text(title + " " + snippet)
                if possible_firm:
                    signals.append({
                        'source': 'Public RFP/Bid',
                        'value': possible_firm,
                        'link': link,
                        'confidence': 'High'
                    })
                    candidates.append(possible_firm)

        # 2. 990 SIGNAL (High Confidence for Non-Profits)
        # Query: "Acme Corp" Form 990 "compensation" "consulting"
        q_990 = f'"{employer_name}" Form 990 "compensation" "consulting"'
        res_990 = search_google(q_990)
        
        if 'organic' in res_990:
             for r in res_990['organic'][:3]:
                snippet = r.get('snippet', '').lower()
                if "schedule j" in snippet or "compensation" in snippet:
                     possible_firm = self._extract_firm_from_text(snippet)
                     if possible_firm:
                         signals.append({
                            'source': 'IRS Form 990',
                            'value': possible_firm,
                            'link': r.get('link'),
                            'confidence': 'High'
                        })
                         candidates.append(possible_firm)

        # 3. LINKEDIN SIGNAL (Medium Confidence)
        # Query: "Acme Corp" "benefits broker" site:linkedin.com
        q_li = f'"{employer_name}" "benefits consultant" site:linkedin.com'
        res_li = search_google(q_li)
        
        if 'organic' in res_li:
             for r in res_li['organic'][:5]:
                 title = r.get('title', '')
                 # Look for "Vice President at Mercer" managing "Acme"
                 # Heuristic: Find Broker Firm name in Title
                 possible_firm = self._extract_firm_from_text(title.lower())
                 if possible_firm:
                     signals.append({
                        'source': 'LinkedIn Profile',
                        'value': possible_firm,
                        'link': r.get('link'),
                        'confidence': 'Medium'
                     })
                     candidates.append(possible_firm)
                     
        return signals, candidates

    def score_candidates(self, candidates):
        """
        Aggregates candidate mentions and returns the winner.
        """
        if not candidates: return None, 0, "No Signal"
        
        counts = Counter(candidates)
        winner, count = counts.most_common(1)[0]
        
        # Scoring Logic
        # 3+ mentions = Very High (90)
        # 2 mentions = High (70)
        # 1 mention = Low (40)
        
        score = 40
        if count >= 2: score = 70
        if count >= 3: score = 90
        
        return winner, score, f"Found in {count} sources"

    def _extract_firm_from_text(self, text):
        """
        Helper to find known broker firms in text strings.
        """
        # KNOWN WESTERN BROKERS (The "Big" List + Regionals)
        # In prod, this should be a DB lookup or config file
        known_firms = [
            "mercer", "aon", "willis towers watson", "wtw", "gallagher", 
            "hub international", "hub", "lockton", "alliant", "nfp", 
            "usi", "brown & brown", "abd", "woodruff sawyer", "moss adams",
            "marsh", "buck", "keenan"
        ]
        
        text = text.lower()
        for firm in known_firms:
            # Tokenize or distinct check to avoid sub-word matching (e.g. "on" in "aon")
            # Simple check for now
            if f" {firm} " in f" {text} " or f"{firm}," in text or f"{firm}." in text:
                return firm.title() # Return capitalized
        return None
