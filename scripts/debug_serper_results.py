import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("SERPER_API_KEY")
url = "https://google.serper.dev/images"

def debug_query(name, firm):
    query = f"{name} {firm} LinkedIn profile photo"
    print(f"--- Query: {query} ---")
    
    payload = {
        "q": query,
        "num": 10,
        "gl": "us",
        "hl": "en"
    }
    
    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json"
    }
    
    resp = requests.post(url, headers=headers, json=payload)
    data = resp.json()
    
    print(json.dumps(data, indent=2))

if __name__ == "__main__":
    # Test the problematic case
    debug_query("Andrew Forchelli", "HUB International")
