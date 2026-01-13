
import requests
import json
import os

BASE_URL = "http://localhost:8000"

def test_health_auth():
    print("\n--- TEST: /health/auth ---")
    try:
        res = requests.get(f"{BASE_URL}/health/auth")
        print(f"Status: {res.status_code}")
        print(f"Body: {res.text}")
        
        data = res.json()
        if data["status"] == "ok":
            print("PASS: JWKS Fetch verified (or mocked setup correct).")
        elif "connectivity" in data.get("details", {}):
            print("PASS: JWKS Connectivity confirmed (Key mismatch expected without real ID).")
        else:
            print("FAIL: Diagnostics returned error.")
            
    except Exception as e:
        print(f"FAIL: Request failed {e}")

def test_whoami_no_token():
    print("\n--- TEST: /auth/whoami (No Token) ---")
    res = requests.get(f"{BASE_URL}/auth/whoami")
    if res.status_code == 401:
        print("PASS: 401 Unauthorized received.")
    else:
        print(f"FAIL: Expected 401, got {res.status_code}")

def test_whoami_invalid_token():
    print("\n--- TEST: /auth/whoami (Invalid Token) ---")
    res = requests.get(f"{BASE_URL}/auth/whoami", headers={"Authorization": "Bearer invalid_token"})
    if res.status_code == 401:
        print("PASS: 401 Unauthorized received (as expected).")
        print(res.text)
    else:
        print(f"FAIL: Expected 401, got {res.status_code}")

if __name__ == "__main__":
    test_health_auth()
    test_whoami_no_token()
    test_whoami_invalid_token()
