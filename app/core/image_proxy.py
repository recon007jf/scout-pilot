import requests
from typing import Optional, Dict, Any
from app.config import settings
from app.utils.logger import get_logger

logger = get_logger("image_proxy")

class ImageProxyEngine:
    SERPER_URL = "https://google.serper.dev/images" # Phase 3: Image Search (Production Safe)

    def __init__(self):
        self.api_key = settings.SERPER_API_KEY

    
    def verify_accessibility(self, url: str) -> bool:
        """
        Verifies if the URL is accessible (200 OK) via a standard HEAD/GET request.
        """
        try:
            # Use Browser User-Agent to avoid anti-bot blocks
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
            # Use stream=True to avoid downloading large files, just check headers
            r = requests.get(url, headers=headers, stream=True, timeout=3)
            if r.status_code == 200:
                # Check Content-Type is image
                if "image" in r.headers.get("Content-Type", ""):
                    return True
            return False
        except:
            return False

    def fetch_image(self, name: str, company: str, linkedin_url: Optional[str] = None) -> Dict[str, Any]:
        """
        Retrieves profile image using Serper Google Image Search (Jan 17 Production Protocol).
        Source: https://google.serper.dev/images
        
        Logic:
        1. Query: "Name" "Company" LinkedIn profile photo
        2. Filters:
           - Width & Height >= 100px
           - Aspect Ratio between 0.8 and 1.2 (Square-ish)
           - Domain: Allow media.licdn.com (CDN), Block www.linkedin.com (Auth Wall)
           - [NEW] Accessibility Check: Must return 200 OK.
        """
        if not self.api_key:
            logger.warning("SERPER_API_KEY is not set.")
            return {"imageUrl": None, "error": "API Key Missing"}

        # Construct Query
        # "Name Company LinkedIn profile photo" is the standard pattern.
        query = f'{name} {company} LinkedIn profile photo'
        
        payload = {
            "q": query,
            "num": 10, # Fetch multiple to allow for filtering
            "gl": "us",
            "hl": "en",
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
            
            # Filter Logic
            for img in images:
                w = img.get("imageWidth", 0)
                h = img.get("imageHeight", 0)
                link = img.get("imageUrl", "")
                
                # 1. Size Check (> 100x100)
                if w < 100 or h < 100:
                    continue
                    
                # 2. Aspect Ratio (Square-ish 0.8 - 1.2)
                if h == 0: continue
                ratio = w / h
                if not (0.8 <= ratio <= 1.2):
                    continue
                    
                # 3. Domain STRICT SAFETY (LinkedIn CDN Only)
                # We only want official profile photos. 
                # Blocking random websites (arizent, businessinsurance, etc) that Google finds.
                # Valid Domains: media.licdn.com
                if "licdn.com" not in link:
                    # Reject non-LinkedIn sources
                    continue
                    
                # 4. Success Check (NEW)
                # Verify URL is actually reachable
                if not self.verify_accessibility(link):
                    logger.warning(f"Image blocked/broken: {link}")
                    continue
                    
                # Success!
                return {"imageUrl": link, "source": img.get("source", "serper_images")}
            
            return {"imageUrl": None, "reason": "No valid licdn.com images found"}

        except Exception as e:
            logger.error(f"Serper Image API Failed: {e}")
            return {"imageUrl": None, "error": str(e)}
