import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

PDL_API_KEY = os.getenv("PDL_API_KEY")
COMPANY_URL = "https://api.peopledatalabs.com/v5/company/enrich"

def debug_headers():
    if not PDL_API_KEY:
        print("❌ CRITICAL: PDL_API_KEY Missing in environment!")
        return

    print("🕵️ HEADER DISCOVERY PROBE v2 (Hardened)...")

    # --- TEST 1: The "Happy Path" (Website) ---
    print("\n--- TEST 1: Website Match (spacex.com) ---")
    params = {"website": "spacex.com", "pretty": True}
    headers = {"X-Api-Key": PDL_API_KEY}
    
    try:
        # TIMEOUT ADDED (10s)
        resp = requests.get(COMPANY_URL, params=params, headers=headers, timeout=10)
        
        # LOG REQUEST URL (Forensics)
        print(f"Request URL: {resp.url}")
        print(f"Status: {resp.status_code}")
        
        # PRINT ALL HEADERS (Case Insensitive View)
        print("\n--- RESPONSE HEADERS ---")
        for k, v in resp.headers.items():
            # Highlight relevant keys
            key_lower = k.lower()
            if any(x in key_lower for x in ["credit", "limit", "usage", "bill", "rate"]):
                print(f"💰 {k}: {v}")
            else:
                print(f"{k}: {v}")
        
        # PRINT METADATA SNIPPET
        if resp.status_code == 200:
            try:
                data = resp.json()
                print("\n--- RESPONSE BODY METADATA ---")
                print(f"Name: {data.get('name')}")
                print(f"Website: {data.get('website')}")
                print(f"Likelihood: {data.get('likelihood')}")
                print(f"ID: {data.get('id', 'N/A')}")
            except Exception as e:
                print(f"JSON Parse Error: {e}")
                print(f"Raw Body: {resp.text[:500]}")
        else:
            print(f"\nError Body: {resp.text[:1000]}")

    except Exception as e:
        print(f"Exception: {e}")

    # --- TEST 2: The "Failure Path" (Raw Name) ---
    print("\n--- TEST 2: Raw Name (Space Exploration...) ---")
    params = {
        "name": "SPACE EXPLORATION TECHNOLOGIES CORP",
        "location": "CA",
        "pretty": True
    }
    
    try:
        resp = requests.get(COMPANY_URL, params=params, headers=headers, timeout=10)
        print(f"Request URL: {resp.url}")
        print(f"Status: {resp.status_code}")
        
        if resp.status_code != 200:
            print(f"Error Body: {resp.text[:1000]}")
            
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    debug_headers()
