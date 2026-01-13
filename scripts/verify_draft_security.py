
import requests
import os
import threading
import time
import json
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "http://localhost:8000"
ENDPOINT = f"{BASE_URL}/api/scout/generate-draft"
SECRET = os.getenv("SCOUT_INTERNAL_SECRET", "scout_secret_123")
DOSSIER_ID = "00000000-0000-0000-0000-000000000000" # Valid UUID format


def print_res(name, res):
    print(f"[{name}] Status: {res.status_code}")
    print(f"[{name}] Body: {res.text[:100]}...")
    print(f"[{name}] Headers: {json.dumps(dict(res.headers), indent=2)}")

def test_auth_rejection():
    print("\n--- TEST: AUTH REJECTION ---")
    # 1. No Header
    res = requests.post(ENDPOINT, json={"dossier_id": DOSSIER_ID})
    if res.status_code == 401:
        print("PASS: Missing Header rejected (401)")
    else:
        print(f"FAIL: Missing Header got {res.status_code}")

    # 2. Bad Header
    res = requests.post(ENDPOINT, json={"dossier_id": DOSSIER_ID}, headers={"x-scout-internal-secret": "wrong"})
    if res.status_code in [401, 403]:
        print(f"PASS: Wrong Header rejected ({res.status_code})")
    else:
        print(f"FAIL: Wrong Header got {res.status_code}")

def test_valid_generation():
    print("\n--- TEST: VALID GENERATION ---")
    headers = {"x-scout-internal-secret": SECRET, "x-debug-llm": "1"} # Force regeneration
    payload = {"dossier_id": DOSSIER_ID, "force_regenerate": True}
    
    print(f"Sending Request... (Secret len: {len(SECRET)})")
    res = requests.post(ENDPOINT, json=payload, headers=headers)
    
    if res.status_code == 200:
        print("PASS: Generation Success (200)")
        data = res.json()
        
        # Verify Keys
        required = ["body_clean", "signature_block", "body_with_signature", "trace_id", "status"]
        missing = [k for k in required if k not in data]
        if not missing:
            print("PASS: All Response Keys Present")
        else:
            print(f"FAIL: Missing Keys: {missing}")
            
        # Verify Headers
        h = res.headers
        if "x-llm-model" in h and "x-llm-tokens-out" in h:
            print(f"PASS: Proof Headers Present (Model: {h.get('x-llm-model')})")
        else:
            print("FAIL: Missing Proof Headers")

    else:
        print(f"FAIL: Generation Failed {res.status_code} {res.text}")

def call_endpoint(name, force=False):
    headers = {"x-scout-internal-secret": SECRET}
    if force:
        headers["x-debug-llm"] = "1"
        
    res = requests.post(ENDPOINT, json={"dossier_id": DOSSIER_ID, "force_regenerate": force}, headers=headers)
    print_res(name, res)
    return res.status_code

def test_concurrency():
    print("\n--- TEST: CONCURRENCY LOCK ---")
    # T1: Force = True (Start Generation, set status='generating')
    # T2: Force = False (Should see 'generating' and hit 409)
    t1 = threading.Thread(target=call_endpoint, args=("Thread-1", True))
    t2 = threading.Thread(target=call_endpoint, args=("Thread-2", False))
    
    t1.start()
    time.sleep(0.5) # Ensure T1 has time to acquire lock and start LLM
    t2.start()
    
    t1.join()
    t2.join()

if __name__ == "__main__":
    test_auth_rejection()
    test_valid_generation()
    # Note: Concurrency test is tricky against localhost if LLM is fast or mocked.
    # But with x-debug-llm: 1, it should take ~1-2s.
    test_concurrency()
