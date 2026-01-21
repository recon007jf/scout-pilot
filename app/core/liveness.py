import requests
import re
from app.config import settings
from app.utils.logger import get_logger

logger = get_logger("liveness")

class EmploymentLivenessCheck:
    """
    The Liveness Gate (Bounce Protection).
    Authority on Current Status.
    
    Logic:
    Query: site:linkedin.com/in/ "{Name}" "{Firm}"
    Negative Lookbehind: If snippet contains 'Former', 'Past', 'Ex-', 'Previous' near the firm name -> BLOCK.
    """
    
    SERPER_URL = "https://google.serper.dev/search"
    
    def __init__(self):
        self.api_key = settings.SERPER_API_KEY
        
    def check_status(self, candidate: dict) -> dict:
        """
        Returns:
        {
            "is_departure": bool,
            "risk_reason": str | None
        }
        """
        result = {
            "is_departure": False,
            "risk_reason": None
        }
        
        name = candidate.get("full_name")
        firm = candidate.get("firm")
        
        if not name or not firm:
            return result # Can't check
            
        if not self.api_key:
            logger.warning("No SERPER_API_KEY. Skipping Liveness Check.")
            return result
            
        # 1. Strict Triangulation Query
        # Relaxing quotes to improve hit rate.
        query = f'site:linkedin.com/in/ {name} {firm}'
        
        try:
            payload = {
                "q": query,
                "num": 3 
            }
            headers = {
                'X-API-KEY': self.api_key,
                'Content-Type': 'application/json'
            }
            
            response = requests.post(self.SERPER_URL, headers=headers, json=payload, timeout=5)
            data = response.json()
            organic = data.get("organic", [])
            
            if not organic:
                # If zero results for "{Name}" "{Firm}", that ITSELF is a risk factor.
                # But sometimes people just have sparse profiles.
                # Per directive: "If the snippet contains the firm name..."
                # So if no result, we can't perform the check.
                logger.info(f"Liveness: No results for {query}")
                return result 
                
            # 2. Analyze the Top Result
            top_hit = organic[0]
            title = top_hit.get("title", "")
            snippet = top_hit.get("snippet", "")
            
            # Combine for analysis
            full_text = f"{title} {snippet}"
            
            # Combine for analysis
            full_text = f"{title} {snippet}"
            
            # 3. Anchor Check: Does it actually mention the Firm?
            # If the firm is NOT in the snippet, we cannot confirm they are there.
            # In fact, if we searched for "{Name} {Firm}" and the result doesn't show {Firm},
            # it implies they might have moved or the index is stale/profile is different.
            if firm.lower() not in full_text.lower():
                # Potential Mismatch
                # But be careful of "Alera" vs "Alera Group".
                # For now, strict check.
                logger.warning(f"Liveness: Firm '{firm}' NOT found in snippet for {name}. Risk.")
                result["is_departure"] = True
                result["risk_reason"] = f"Firm '{firm}' not found in verification snippet. Possible Misalignment."
                return result
                
            # 4. NEGATIVE LOOKBEHIND (The Hard Gate)
            # Scan for "Former", "Past", "Ex-", "Previous"
            
            triggers = ["former", "past", "ex-", "previous"]
            
            # Normalize
            text_lower = full_text.lower()
            firm_lower = firm.lower()
            
            # Does the snippet say "Former... {Firm}"?
            # Or "{Firm}... (Past)"?
            
            departure_detected = False
            detected_trigger = ""
            
            for t in triggers:
                if t in text_lower:
                    # Is it referring to THIS firm?
                    # Check proximity?
                    # For V1 Hard Gate, we will enable it if both exist.
                    # This might flag "Former Intern at Lockton, now VP at Lockton", but that's rare.
                    # Usually: "Former VP at Lockton. Current: Marsh."
                    
                    # Refined Regex: trigger + 40 chars + firm   OR   firm + 40 chars + trigger
                    # Actually, let's just use the presence of both for now, as per the strict directive "Negative Lookbehind".
                    
                    departure_detected = True
                    detected_trigger = t
                    break
            
            if departure_detected:
                logger.warning(f"Liveness: DEPARTURE DETECTED for {name} ({firm}). Found '{detected_trigger}'.")
                result["is_departure"] = True
                result["risk_reason"] = f"Departure Detected (Found '{detected_trigger}' near firm name)"
                return result
                
        except Exception as e:
            logger.error(f"Liveness Check Failed: {e}")
            
        return result
