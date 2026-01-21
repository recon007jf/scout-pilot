
import requests
from supabase import create_client

# Hardcoded Service Role Key
URL = "https://ojzqwwaebxvpnenkthaw.supabase.co"
KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9qenF3d2FlYnh2cG5lbmt0aGF3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NTM4MjE0MywiZXhwIjoyMDgwOTU4MTQzfQ.IvHe8xsdiIix3XugOHBEEV_ZG1Y0x6KzJF3JL0ByUcQ"

def recover():
    print("Recovering FAILED candidates...")
    sb = create_client(URL, KEY)
    # Reset FAILED to POOL
    # Only reset if they have 'missing' log? No, just all FAILED for now.
    sb.table("candidates").update({"status": "POOL"}).eq("status", "FAILED").execute()
    print("Recovered.")

def verify_jared():
    url = "https://media.licdn.com/dms/image/v2/D5603AQGlede80NbEdw/profile-displayphoto-shrink_200_200/profile-displayphoto-shrink_200_200/0/1699298391253"
    print(f"Verifying {url}...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    try:
        r = requests.get(url, headers=headers, stream=True, timeout=5)
        print(f"Status: {r.status_code}")
        print(f"Headers: {r.headers}")
        print(f"Content-Type: {r.headers.get('Content-Type')}")
        
        if r.status_code == 200 and "image" in r.headers.get("Content-Type", ""):
            print("PASS")
        else:
            print("FAIL")
            # If text, print a snippet
            if "text" in r.headers.get("Content-Type", ""):
                print(f"Body snippet: {r.content[:200]}")
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    recover()
    verify_jared()
