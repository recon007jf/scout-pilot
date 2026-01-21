import os
import requests
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("SERPER_API_KEY")
url = "https://google.serper.dev/images"

def test_serper():
    if not api_key:
        print("❌ SERPER_API_KEY not found in environment.")
        return

    print(f"Testing Serper API with key: {api_key[:5]}...")
    
    payload = {
        "q": "Kevin Overbey Unknown Firm LinkedIn professional headshot",
        "num": 1
    }
    
    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            images = data.get("images", [])
            if images:
                print(f"✅ Success! Found image: {images[0].get('imageUrl')}")
            else:
                print("⚠️ Request succeeded but no images returned.")
        elif response.status_code == 403:
            print("❌ 403 Forbidden - Invalid API Key or Unauthorized.")
        elif response.status_code == 401:
            print("❌ 401 Unauthorized - Invalid API Key.")
        else:
            print(f"❌ Error: {response.text}")

    except Exception as e:
        print(f"❌ Exception: {e}")

if __name__ == "__main__":
    test_serper()
