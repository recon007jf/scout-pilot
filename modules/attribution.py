import json
import os
import logging
import time
from utils.serper import search_google

# Import Vertex AI
try:
    import vertexai
    from vertexai.preview.generative_models import GenerativeModel
    VERTEX_AVAILABLE = True
except ImportError:
    VERTEX_AVAILABLE = False
    print("⚠️ Vertex AI not installed. Deep Discovery will run in fallback mode.")

# Configure logger
logger = logging.getLogger(__name__)

class AttributionEngine:
    def __init__(self, roster_path="biz_dev_roster.json"):
        self.roster = self._load_roster(roster_path)
        
        # Init Vertex if available
        if VERTEX_AVAILABLE:
            try:
                # Default to project from env or hardcoded fallback
                project_id = os.getenv("PROJECT_ID", "scout-ai-app-480200")
                vertexai.init(project=project_id, location="us-central1")
                self.model = GenerativeModel("gemini-1.5-flash-001")
            except Exception as e:
                logger.error(f"Vertex Init Failed: {e}")
                self.model = None
        else:
            self.model = None
        
    def _load_roster(self, path):
        if not os.path.exists(path): return []
        try:
            with open(path, 'r') as f: return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load roster: {e}")
            return []

    def find_attribution(self, employer, state, broker_firm=None, broker_city=None, broker_state=None):
        """
        Executes Hybrid V2 Waterfall (Lookup -> Multi-Signal Validate -> Deep Discovery).
        """
        result = {
            "name": "Unknown",
            "role": "Unknown",
            "firm": broker_firm,
            "confidence_score": 0,
            "confidence_label": "Discard",
            "source": "None",
            "method": "None",
            "specialty_tags": []
        }

        # --- STEP 1: INTERNAL LOOKUP ---
        internal_match = self._lookup_internal(broker_firm, broker_city, broker_state)
        
        if internal_match:
            # --- STEP 2: ENHANCED VALIDATION ---
            is_valid, valid_data = self._validate_enhanced(internal_match['person_name'], broker_firm, state)
            
            if is_valid:
                result.update({
                    "name": internal_match['person_name'],
                    "role": internal_match.get('role', 'Broker'),
                    "confidence_score": 100,
                    "confidence_label": "Platinum",
                    "source": "Internal Roster",
                    "method": valid_data['method'],
                    "specialty_tags": ["Internal_Match"]
                })
                return result
            else:
                logger.info(f"Local match {internal_match['person_name']} failed validation.")

        # --- STEP 3: DEEP DISCOVERY (TRIANGULATION) ---
        discovery = self._triangulate_deep(employer, state, broker_firm, broker_city)
        
        if discovery['name']:
            # --- STEP 4: SCORING ---
            score, label = self._calculate_score(discovery)
            
            result.update({
                "name": discovery['name'],
                "role": discovery.get('role', 'Consultant'),
                "confidence_score": score,
                "confidence_label": label,
                "source": "External Discovery",
                "method": discovery['method'],
                "specialty_tags": discovery['tags'],
                "notes": discovery.get('reasoning', '')
            })
            
        return result

    def _lookup_internal(self, firm, city, state):
        if not firm: return None
        firm_lower = firm.lower()
        city_lower = city.lower() if city else ""
        
        # 1. Exact Match Try
        for entry in self.roster:
            if entry['firm'].lower() in firm_lower:
                e_city = entry['city'].lower()
                # A. Exact City
                if city_lower and e_city == city_lower:
                    return entry
                # B. State Match (if city not specific in roster)
                if state and entry['state'] == state and e_city != "west region":
                    return entry
        
        # 2. Regional Fallback (e.g. "West Region" leaders cover all Western States)
        # Western States List
        west_states = ['CA', 'WA', 'OR', 'ID', 'UT', 'CO', 'AZ', 'NV', 'NM']
        if state in west_states:
            for entry in self.roster:
                 if entry['firm'].lower() in firm_lower:
                     if entry['city'].lower() == "west region" or entry['state'].lower() == "west":
                         return entry
                         
        return None

    def _validate_enhanced(self, name, firm, state):
        """
        Checks 3 signals: Firm Website, License, Press.
        """
        # 1. Firm Website Check (Highest Authority)
        q_site = f'site:{firm.replace(" ", "").lower()}.com "{name}" "Team"'
        res_site = search_google(q_site)
        if self._check_serp_hits(res_site, [name, "team", "people"]):
            return True, {'method': 'Firm_Website_Match'}

        # 2. State License Check (Keyword proxy)
        q_license = f'"{name}" "{firm}" "{state}" insurance license producer'
        res_license = search_google(q_license)
        if self._check_serp_hits(res_license, ["license", "producer", "active"]):
             return True, {'method': 'License_Check_Match'}
             
        # 3. Press/News
        q_press = f'"{name}" "{firm}" after:2023-01-01'
        res_press = search_google(q_press)
        if self._check_serp_hits(res_press, [firm, "appointed", "joined", "promoted"]):
            return True, {'method': 'Press_Match'}
            
        return False, {}

    def _triangulate_deep(self, employer, state, firm, city):
        """
        Scrapes Stop-Loss, Conferences, and Deep LinkedIn - then asks Gemini to parse.
        """
        best_candidate = {'name': None, 'role': None, 'method': None, 'tags': [], 'reasoning': ''}
        
        # 1. Collect Signals (Raw Text)
        search_results = []
        
        # A. Stop-Loss / Elite Partner Signals
        if firm:
            q_sl = f'"{firm}" "Stop Loss" "Elite Partner" "{state}"'
            res_sl = search_google(q_sl)
            search_results.extend(self._extract_snippets(res_sl))

        # B. Conference Speaker
        q_conf = f'"{firm}" "Self-Funded" "Speaker" "{state}"'
        res_conf = search_google(q_conf)
        search_results.extend(self._extract_snippets(res_conf))
        
        # C. LinkedIn Deep (Targeted)
        q_li = f'"{firm}" "{city}" ("VP" OR "Principal" OR "Consultant") "Self-Funded" -job site:linkedin.com'
        res_li = search_google(q_li)
        search_results.extend(self._extract_snippets(res_li))

        # 2. Analyze with Gemini (if available)
        if self.model and search_results:
            try:
                 analysis = self._analyze_with_gemini(firm, city, search_results)
                 if analysis and analysis.get('confidence_score', 0) > 40:
                     best_candidate['name'] = analysis.get('best_match_name')
                     best_candidate['role'] = analysis.get('job_title')
                     best_candidate['method'] = "Gemini_AI_Analysis"
                     best_candidate['reasoning'] = analysis.get('reasoning')
                     best_candidate['tags'] = ["AI_Extracted"]
                     if "Self-Funded" in analysis.get('specialty_evidence', ''):
                         best_candidate['tags'].append("Self-Funded")
                     return best_candidate
            except Exception as e:
                logger.error(f"Gemini Analysis Failed: {e}")

        # 3. Fallback Heuristic (If Gemini fails or not avail)
        # Using the LinkedIn results from C
        if 'organic' in res_li:
             for item in res_li['organic'][:3]:
                title = item.get('title', '')
                snippet = item.get('snippet', '').lower()
                if " - " in title:
                    parts = title.split(" - ")
                    if len(parts) > 0:
                        potential_name = parts[0]
                        if len(potential_name.split()) in [2, 3]:
                            best_candidate['name'] = potential_name
                            best_candidate['role'] = "Principal/VP" 
                            best_candidate['method'] = "Heuristic_Fallback"
                            if "self-funded" in snippet: best_candidate['tags'].append("Self-Funded")
                            return best_candidate
                            
        return best_candidate

    def _analyze_with_gemini(self, firm, city, snippets):
        """
        Sends raw snippets to Gemini Pro/Flash for structured extraction.
        """
        snippet_text = "\n".join([f"- {s}" for s in snippets[:10]]) # Limit context
        
        prompt = f"""
        **Role:** You are an expert Insurance Data Analyst.
        **Task:** Analyze the provided raw search snippets to identify the specific "Self-Funded Health Plan" expert at the target firm.

        **Input Data:**
        1. Target Firm: {firm}
        2. Target Office: {city}
        3. Raw Search Snippets:
        {snippet_text}

        **Analysis Logic:**
        1. **Identify Candidates:** Look for names associated with titles like "Principal," "Area President," "Senior Consultant," or "EVP."
        2. **Filter for Relevance:** Prioritize individuals with keywords: "Self-Funded," "Stop-Loss," "Captives," "ASO," "Large Group."
        3. **Check Location:** Ensure the person is currently based in or serving {city}.
        4. **Confidence Scoring:**
        - HIGH (80+): Snippet explicitly links person to "Self-Funded" expertise AND current role at firm.
        - MED (50-79): Correct Role/Location, but no explicit "Self-Funded" keyword.
        - LOW (<50): Ambiguous or outdated info.

        **Output Format (JSON Only):**
        {{
        "best_match_name": "String",
        "job_title": "String",
        "specialty_evidence": "String (quote from snippet)",
        "confidence_score": Integer,
        "reasoning": "String"
        }}
        """
        
        try:
            response = self.model.generate_content(prompt)
            # Clean response (sometimes returns markdown block)
            text = response.text.replace("```json", "").replace("```", "").strip()
            return json.loads(text)
        except Exception as e:
            logger.error(f"LLM Parse Error: {e}")
            return None

    def _extract_snippets(self, res):
        snippets = []
        if 'organic' in res:
            for item in res['organic']:
                text = f"{item.get('title', '')}: {item.get('snippet', '')}"
                snippets.append(text)
        return snippets

    def _calculate_score(self, discovery):
        if not discovery['name']: return 0, "Discard"
        
        score = 50
        label = "Silver"
        
        if "Self-Funded" in discovery['tags'] or discovery['method'] == "Gemini_AI_Analysis":
            score = 80
            label = "Gold"
            
        return score, label

    def _check_serp_hits(self, res, keywords):
        if 'organic' not in res: return False
        for item in res['organic'][:3]:
            text = (item.get('title', '') + " " + item.get('snippet', '')).lower()
            if any(k.lower() in text for k in keywords):
                return True
        return False
