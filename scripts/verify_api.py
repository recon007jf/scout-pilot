import requests
import json
import time

def test_api():
    url = "http://localhost:8000/api/briefing"
    print(f"🔌 Connecting to {url}...")
    
    try:
        # Retry loop to wait for server boot
        for i in range(5):
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    break
            except requests.exceptions.ConnectionError:
                print(f"⏳ Waiting for server... ({i+1}/5)")
                time.sleep(2)
        else:
            print("❌ Failed to connect to server after retries.")
            return

        data = response.json()
        print(f"✅ API Response: {len(data)} items found.")
        
        if data:
            print("📊 First Item Payload:")
            print(json.dumps(data[0], indent=2))
        else:
            print("⚠️ Response list is empty.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_api()
