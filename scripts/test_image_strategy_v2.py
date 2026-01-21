import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("SERPER_API_KEY")
url = "https://google.serper.dev/images"

def test_image_search_v2():
    if not api_key:
        print("❌ SERPER_API_KEY not found.")
        return

    queries = [
        # Satya (High Profile)
        ('Satya Nadella Microsoft LinkedIn profile photo', "Satya Nadella"),
        # Kevin (Target) - "Alliant" is his firm, but DB has "Unknown Firm". 
        # Testing with "Alliant" to see if it works as intended per user "Strategy".
        ('Kevin Overbey Alliant LinkedIn profile photo', "Kevin Overbey (Correct Firm)"),
        ('Kevin Overbey "Unknown Firm" LinkedIn profile photo', "Kevin Overbey (Bad Firm)"),
    ]

    for q, label in queries:
        print(f"\n--- Testing Query: {label} ---")
        print(f"Query: {q}")
        
        payload = {
            "q": q,
            "num": 10, # Fetch more to filter
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
            if response.status_code != 200:
                print(f"Error: {response.status_code} - {response.text}")
                continue

            data = response.json()
            images = data.get("images", [])
            print(f"Found {len(images)} raw results.")

            # Filter Logic (User Directive)
            # - Square-ish
            # - > 100x100
            # - Not linkedin.com (media.licdn.com is usually OK or cdns)
            
            found = None
            for idx, img in enumerate(images):
                if idx == 0:
                    print(f"  [DEBUG] Keys: {list(img.keys())}")
                
                w = img.get("imageWidth", 0)
                h = img.get("imageHeight", 0)
                link = img.get("imageUrl", "")
                domain = img.get("source", "") # Serper 'source' or domain extraction
                
                print(f"  [{idx}] {w}x{h} | {link[:40]}... | Source: {domain}")

                # 1. Size Check
                if w < 100 or h < 100:
                    continue
                
                # 2. Aspect Ratio (Square-ish)
                # Let's say 0.8 to 1.2
                if h == 0: continue
                ratio = w / h
                if not (0.8 <= ratio <= 1.2):
                    continue

                # 3. Domain Check
                # User: "Does NOT come from linkedin.com"
                # But "Comes from a cached CDN (gstatic, bing, media)"
                if "linkedin.com" in link and "media.licdn.com" not in link:
                     # Block www.linkedin.com/ ...
                     # Allow media.licdn.com (CDN)
                     pass
                
                # Actually, user said: "Does NOT come from linkedin.com (blocked)"
                # If the URL is `https://media.licdn.com/dms/image...` that IS acceptable if proxied?
                # User said: "Comes from a cached CDN (gstatic, bing, media)"
                # "media" likely refers to media.licdn.com.
                
                found = img
                print(f"✅ MATCH SELECTED: {link}")
                break
            
            if not found:
                print("⚠️ No matching image found after filtering.")

        except Exception as e:
            print(f"Exception: {e}")

if __name__ == "__main__":
    test_image_search_v2()
