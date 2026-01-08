from app.services.enrichment_service import EnrichmentService
import json

def verify():
    print("🚀 Verifying PDL Integration...")
    
    try:
        service = EnrichmentService()
    except Exception as e:
        print(f"❌ Failed to init service: {e}")
        return

    # Test Case: Apple CFO
    target = {
        "company": "Apple",
        "website": "apple.com",
        "titles": ["Chief Financial Officer", "CFO"]
    }
    
    print(f"🔎 Searching for: {target['titles']} at {target['company']}...")
    
    result = service.find_person(
        company_name=target['company'], 
        website=target['website'], 
        title_keywords=target['titles']
    )
    
    print("\n--- RESULT ---")
    print(json.dumps(result, indent=2))
    
    if result.get('success'):
        print(f"\n✅ SUCCESS: Found {result.get('name')} ({result.get('email')})")
        
        # Verify correctness (Luca Maestri is the known CFO, or maybe Kevan Parekh incoming?)
        # Let's just check for valid data format.
        if "@apple.com" in str(result.get('email')):
             print("✅ Email domain matches.")
        else:
             print("⚠️ Email domain mismatch or missing.")
    else:
        print("\n❌ FAILURE: No match or API error.")

if __name__ == "__main__":
    verify()
