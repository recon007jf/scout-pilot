import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("SERPER_API_KEY")
url = "https://google.serper.dev/images"

def verify_yield():
    if not api_key:
        print("❌ SERPER_API_KEY not found.")
        return

    # Mix of famous and likely-to-have-profiles
    profiles = [
        ("Reed Hastings", "Netflix"),
        ("Brian Chesky", "Airbnb"),
        ("Sheryl Sandberg", "Facebook"),
        ("Andrew Forchelli", "Golesaire"), # The user's difficult case
        ("Kevin Overbey", "Alliant"), # The user's other case
    ]

    print(f"--- Verifying LinkedIn Image Yield (Phase 3 Strategy) ---")
    
    for name, firm in profiles:
        query = f'{name} {firm} LinkedIn profile photo'
        print(f"\n🔎 Query: {query}")
        
        payload = {
            "q": query,
            "num": 10,
            "gl": "us",
            "hl": "en",
            "autocorrect": True
        }
        
        headers = {
            "X-API-KEY": api_key,
            "Content-Type": "application/json"
        }

        try:
            response = requests.post(url, headers=headers, json=payload)
            data = response.json()
            images = data.get("images", [])
            
            # Replicate Logic from image_proxy.py
            found = None
            rejected_linkedin = []
            
            for img in images:
                w = img.get("imageWidth", 0)
                h = img.get("imageHeight", 0)
                link = img.get("imageUrl", "")
                source = img.get("source", "")
                
                # Check metrics
                if h == 0: ratio = 0
                else: ratio = w / h
                
                is_linkedin = "linkedin.com" in link or "licdn.com" in link
                
                if w < 100 or h < 100:
                    if is_linkedin: rejected_linkedin.append(f"Size ({w}x{h})")
                    continue
                    
                if not (0.8 <= ratio <= 1.2):
                    if is_linkedin: rejected_linkedin.append(f"Ratio ({w}x{h}={ratio:.2f})")
                    continue
                    
                # Domain Safety
                if "linkedin.com" in link and "media.licdn.com" not in link:
                     if is_linkedin: rejected_linkedin.append("Auth Wall Domain")
                     continue

                # SUCCESS
                found = img
                print(f"✅ MATCH: {link}")
                print(f"   Source: {source} | Dim: {w}x{h}")
                break
                
            if not found:
                print("❌ NO MATCH FOUND.")
            
            if rejected_linkedin:
                print(f"   (Skipped {len(rejected_linkedin)} raw LinkedIn matches: {', '.join(rejected_linkedin)})")

        except Exception as e:
            print(f"Exception: {e}")

if __name__ == "__main__":
    verify_yield()
