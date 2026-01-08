import requests
from typing import Optional, Dict, Any
from app.config import settings
from app.utils.logger import get_logger

logger = get_logger("image_proxy")

class ImageProxyEngine:
    SERPER_URL = "https://google.serper.dev/images"

    def __init__(self):
        self.api_key = settings.SERPER_API_KEY

    def fetch_image(self, name: str, company: str, linkedin_url: Optional[str] = None) -> Dict[str, Any]:
        """
        Proxies request to Serper to find a profile image.
        Prioritizes freshness and relevance.
        """
        if not self.api_key:
            logger.warning("SERPER_API_KEY is not set.")
            return {"imageUrl": None, "error": "API Key Missing"}

        # Construct Query
        # If LinkedIn URL is present, maybe use it? But Serper Images works best with text keywords.
        # "Name Company LinkedIn" is usually the strongest signal.
        query = f"{name} {company} LinkedIn professional headshot"
        
        payload = {
            "q": query,
            "num": 1,        # We only need the top result
            "autocorrect": True
        }
        
        headers = {
            "X-API-KEY": self.api_key,
            "Content-Type": "application/json"
        }

        try:
            response = requests.post(self.SERPER_URL, headers=headers, json=payload, timeout=5)
            response.raise_for_status()
            data = response.json()
            
            images = data.get("images", [])
            if images:
                # Return top result
                return {"imageUrl": images[0].get("imageUrl")}
            else:
                return {"imageUrl": None, "reason": "No results found"}

        except Exception as e:
            logger.error(f"Serper API Failed: {e}")
            # Do not crash, just return null image embedded in success response or error code
            # V0 expects a working endpoint. Null image is safer.
            return {"imageUrl": None, "error": str(e)}
