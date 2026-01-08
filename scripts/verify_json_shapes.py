
import requests
import json
import os
import sys
import google.auth
import google.auth.transport.requests

# Configuration
BASE_URL = "https://scout-backend-prod-283427197752.us-central1.run.app"
TIMEOUT = 10 

def log(msg):
    print(msg)
    sys.stdout.flush()

def get_id_token():
    log("Acquiring ID Token...")
    try:
        creds, project = google.auth.default()
        auth_req = google.auth.transport.requests.Request()
        creds.refresh(auth_req)
        return creds.id_token
    except Exception as e:
        log(f"Error getting token: {e}")
        # Try finding a gcloud token if default fails, or just fail.
        # User wants "DUMB" script. If auth fails, we probably can't test.
        # But let's try to be helpful if possible.
        return None

def test_endpoint(method, endpoint, token, data=None):
    log(f"--- REQUEST: {method} {endpoint} ---")
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{BASE_URL}{endpoint}"
    
    try:
        if method == "GET":
            response = requests.get(url, headers=headers, timeout=TIMEOUT)
        else:
            response = requests.post(url, headers=headers, json=data, timeout=TIMEOUT)
            
        log(f"Status Code: {response.status_code}")
        log("Response Body:")
        try:
            # Print raw text if not json, or json if possible
            # User asked for "Raw JSON Response"
            print(response.text) 
        except:
            print(response.text)
            
    except Exception as e:
        log(f"Request Failed: {e}")

def main():
    token = get_id_token()
    
    if not token:
        # Fallback to env var
        token = os.environ.get("TOKEN")

    if not token:
        log("CRITICAL: Failed to acquire ID Token. Exiting.")
        sys.exit(1)

    # 1. GET /api/briefing
    test_endpoint("GET", "/api/briefing", token)

    # 2. GET /api/contacts?page=1&page_size=5
    test_endpoint("GET", "/api/contacts?page=1&page_size=5", token)

    # 3. GET /api/signals?page=1&page_size=3
    test_endpoint("GET", "/api/signals?page=1&page_size=3", token)

    # 4. GET /api/settings
    test_endpoint("GET", "/api/settings", token)

    # 5. POST /api/notes (Dummy payload)
    test_endpoint("POST", "/api/notes", token, {"content": "Test Note", "dossier_id": "dummy"})

    # 6. POST /api/drafts/action (Dummy payload, expect 403)
    test_endpoint("POST", "/api/drafts/action", token, {"action": "test"})

    # 7. GET /api/profile-image?name=Test&company=Test
    test_endpoint("GET", "/api/profile-image?name=Test&company=Test", token)

if __name__ == "__main__":
    main()
