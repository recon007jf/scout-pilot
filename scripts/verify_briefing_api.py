
import requests
import os
from dotenv import load_dotenv

# Load env to get URL
load_dotenv("/Users/josephlf/.gemini/antigravity/scratch/backend/.env")
load_dotenv("/Users/josephlf/.gemini/antigravity/scratch/scout-production/.env.local")

# We want to test the Backend API directly (port 8000) or via Next Proxy (port 3000)?
# Let's test Backend directly first to prove the "Contract".
API_URL = "http://localhost:8000/api/briefing"

def verify():
    print(f"Testing Backend Contract: {API_URL}")
    
    # We are in default_user mode, so no token needed for "Identity Bridge" fallback
    # But if we want to simulate properly, we might need headers.
    # The Identity Bridge code checks SCOUT_IDENTITY_MODE.
    
    try:
        resp = requests.get(API_URL)
        print(f"Status: {resp.status_code}")
        
        if resp.status_code != 200:
            print("FAILED:", resp.text)
            return

        data = resp.json()
        targets = data.get("targets", [])
        print(f"Targets Found: {len(targets)}")
        
        found = False
        for t in targets:
            name = t.get("broker", {}).get("name")
            print(f" - Candidate: {name}")
            if "Tauseef" in str(name):
                found = True
        
        if found:
            print("✅ SUCCESS: Tauseef Rahman found in API payload.")
        else:
            print("❌ FAILURE: Tauseef Rahman NOT found in payload.")
            
    except Exception as e:
        print(f"CRITICAL: {e}")

if __name__ == "__main__":
    verify()
