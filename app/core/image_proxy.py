import requests
from typing import Optional, Dict, Any
from app.config import settings
from app.utils.logger import get_logger

logger = get_logger("image_proxy")

class ImageProxyEngine:
    SERPER_URL = "https://google.serper.dev/search" # SWITCH TO STANDARD SEARCH

    def __init__(self):
        self.api_key = settings.SERPER_API_KEY

    def fetch_image(self, name: str, company: str, linkedin_url: Optional[str] = None) -> Dict[str, Any]:
        """
        Retrieves profile image using Serper Google Search API (Jan 17 Protocol).
        Source: https://google.serper.dev/search
        
        Logic:
        1. Query: site:linkedin.com/in/ "Name" "Company" (or similar intent)
        2. Order: 
           a. KnowledgeGraph.imageUrl
           b. Organic[0].imageUrl
        """
        if not self.api_key:
            logger.warning("SERPER_API_KEY is not set.")
            return {"imageUrl": None, "error": "API Key Missing"}

        # Construct Query
        # User Directive: site:linkedin.com/in/ "First Last" "Company"
        # We try to adhere to this, but might fallback if strict site: filter kills KG.
        # However, purely strict compliance first:
        query = f'site:linkedin.com/in/ "{name}" "{company}"'
        
        # NOTE: If we find that site: operator kills Knowledge Graph (likely), 
        # we might need to adjust creating the query to just 'Name Company LinkedIn' 
        # to get the KG, while still satisfying the "LinkedIn signal" requirement.
        # Let's trust the "Intent" > "Syntax" if syntax fails in tests.
        # But for now, putting the Strict Query.
        
        payload = {
            "q": query,
            "num": 1,
            "gl": "us",
            "hl": "en"
        }
        
        headers = {
            "X-API-KEY": self.api_key,
            "Content-Type": "application/json"
        }

        try:
            response = requests.post(self.SERPER_URL, headers=headers, json=payload, timeout=5)
            response.raise_for_status()
            data = response.json()
            
            # Extraction Logic (Strict Order)
            
            # 1. Knowledge Graph
            kg = data.get("knowledgeGraph", {})
            if kg.get("imageUrl"):
                return {"imageUrl": kg.get("imageUrl"), "source": "knowledge_graph"}
                
            # 2. Organic Result #1
            organic = data.get("organic", [])
            if organic:
                first_result = organic[0]
                if first_result.get("imageUrl"):
                    return {"imageUrl": first_result.get("imageUrl"), "source": "organic_0"}
                    
            return {"imageUrl": None, "reason": "No image in KG or Organic[0]"}

        except Exception as e:
            logger.error(f"Serper Search API Failed: {e}")
            return {"imageUrl": None, "error": str(e)}
