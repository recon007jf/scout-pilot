import os
import json
from peopledatalabs import PDLPY
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_connection():
    api_key = os.getenv("PDL_API_KEY")
    if not api_key:
        print("❌ Error: PDL_API_KEY not found in .env")
        return

    print(f"🔑 Found API Key: {api_key[:4]}...{api_key[-4:]}")

    # Initialize Client
    client = PDLPY(
        api_key=api_key,
    )

    # Test with a known "Golden Record" (e.g., Sean Thorne, founder of PDL, or just a generic one)
    # Using a LinkedIn URL is the most common use case for us.
    test_linkedin = "linkedin.com/in/seanthorne"
    
    print(f"📡 Querying PDL for: {test_linkedin}...")

    try:
        response = client.person.enrichment(
            linkedin=test_linkedin
        )
        
        if response.get('status') == 200:
            print("✅ Success! Connection established.")
            data = response.get('data', {})
            
            # Print Key Stats
            print(f"👤 Name: {data.get('full_name')}")
            print(f"📧 Work Email: {data.get('work_email')}")
            print(f"📱 Mobile Phone: {data.get('mobile_phone')}")
            print(f"Pf Match Confidence: {response.get('likelihood')}")
            
            # Dump full JSON for inspection
            with open("pdl_test_response.json", "w") as f:
                json.dump(response, f, indent=2)
            print("📄 Full response saved to 'pdl_test_response.json'")
            
        else:
            print(f"⚠️ API returned status: {response.get('status')}")
            print(response)

    except Exception as e:
        print(f"❌ Exception occurred: {str(e)}")

if __name__ == "__main__":
    test_connection()
