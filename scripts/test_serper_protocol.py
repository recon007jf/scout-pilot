import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("SERPER_API_KEY")
url = "https://google.serper.dev/search"

def test_serper_search():
    if not api_key:
        print("❌ SERPER_API_KEY not found in environment.")
        return

    # Directive: site:linkedin.com/in/ “First Last” “Company”
    query = 'site:linkedin.com/in/ "Kevin Overbey" "Unknown Firm"' 
    # Note: "Unknown Firm" might be bad for search. Let's try to match the user's manual fix context if possible, 
    # but for this test I will use the one from the directive example roughly.
    # Actually, Kevin's firm in the DB was "Unknown Firm". 
    # Let's try a real query that is likely to work to see the JSON structure.
    # User mentioned Kevin Overbey. Let's try to find him without the bad firm name if possible or just use the name.
    # The directive says: site:linkedin.com/in/ “First Last” “Company”
    
    # Let's try a known public figure to verify Knowledge Graph structure first.
    # Then Kevin.
    
    queries = [
        # Strict User Example
        ('site:linkedin.com/in/ "Satya Nadella" "Microsoft"', "Strict User Example"),
        # Loose Intent (Likely to trigger KG)
        ('"Satya Nadella" "Microsoft" LinkedIn', "Loose Intent"),
        # Kevin (Strict)
        ('site:linkedin.com/in/ "Kevin Overbey" "Alliant"', "Kevin Strict"),
        # Kevin (Loose)
        ('"Kevin Overbey" "Alliant" LinkedIn', "Kevin Loose")
    ]

    for q, label in queries:
        print(f"\n--- Testing Query: {label} ---")
        print(f"Query: {q}")
        payload = {
            "q": q,
            "gl": "us",
            "hl": "en"
        }
        
        headers = {
            "X-API-KEY": api_key,
            "Content-Type": "application/json"
        }

        try:
            response = requests.post(url, headers=headers, json=payload)
            print(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                
                # Check Knowledge Graph
                kg = data.get("knowledgeGraph", {})
                kg_img = kg.get("imageUrl")
                print(f"  [Knowledge Graph] Found: {bool(kg)}")
                print(f"  [Knowledge Graph] Image: {kg_img}")
                
                # Check Organic
                organic = data.get("organic", [])
                org_img = None
                if organic:
                    # Directive: "If that result includes an imageUrl field"
                    org_img = organic[0].get("imageUrl") # Serper often puts snippet images here? OR is it 'thumbnail'? Serper docs say 'imageUrl' for organic sometimes?
                    # Let's print keys of first organic result to be sure
                    print(f"  [Organic #1] Keys: {list(organic[0].keys())}")
                    print(f"  [Organic #1] Image: {org_img}")
                
            else:
                print(f"Error: {response.text}")

        except Exception as e:
            print(f"Exception: {e}")

if __name__ == "__main__":
    test_serper_search()
