
import requests
import os
import json
import uuid
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "http://localhost:8000"
ENDPOINT = f"{BASE_URL}/api/scout/generate-draft"
# Use a valid test dossier ID (Assuming one exists or using a dummy that might fail DB check but hit endpoint logic)
# To get a 200, we need a valid dossier ID. The previous test used all-zeros which might have worked if DB constraints are loose or row exists.
# We'll use the one from verify_draft_security that worked.
DOSSIER_ID = "00000000-0000-0000-0000-000000000000" 
SECRET = os.getenv("SCOUT_INTERNAL_SECRET", "scout_secret_123_secure")

def generate_packet():
    url = ENDPOINT
    payload = {
        "dossier_id": DOSSIER_ID,
        "force_regenerate": True
    }
    headers_in = {
        "x-scout-internal-secret": SECRET,
        "x-debug-llm": "1"
    }

    try:
        res = requests.post(url, json=payload, headers=headers_in)
        
        status = res.status_code
        h = res.headers
        
        # PROOF PACKET FORMAT
        print("\n--- THE PROOF PACKET ---")
        print(f"Request URL: {url}")
        print(f"Status Code: {status}")
        print(f"Request Payload JSON: {json.dumps(payload)}")
        print("\nProof Headers:")
        print(f"x-llm-status: {h.get('x-llm-status', 'MISSING')}")
        print(f"x-llm-model: {h.get('x-llm-model', 'MISSING')}")
        print(f"x-request-trace: {h.get('x-request-trace', 'MISSING')}")
        print(f"x-llm-latency-ms: {h.get('x-llm-latency-ms', 'MISSING')}")
        print(f"x-llm-tokens-out: {h.get('x-llm-tokens-out', 'MISSING')}")
        
        print("\nResponse JSON body:")
        try:
            print(json.dumps(res.json(), indent=2))
        except:
            print(res.text)
            
    except Exception as e:
        print(f"TEST FAILED: {e}")

if __name__ == "__main__":
    generate_packet()
