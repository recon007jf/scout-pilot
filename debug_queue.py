
import requests
from supabase import create_client
from app.core.image_proxy import ImageProxyEngine

# Hardcoded Service Role Key
URL = "https://ojzqwwaebxvpnenkthaw.supabase.co"
KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9qenF3d2FlYnh2cG5lbmt0aGF3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NTM4MjE0MywiZXhwIjoyMDgwOTU4MTQzfQ.IvHe8xsdiIix3XugOHBEEV_ZG1Y0x6KzJF3JL0ByUcQ"

def analyze():
    admin_db = create_client(URL, KEY)
    
    # 1. Check Diversity in Queue
    print("--- Queue Analysis ---")
    res = admin_db.table("morning_briefing_queue").select("*, candidate:candidates(full_name, firm, linkedin_image_url)").execute()
    
    firm_counts = {}
    no_image_count = 0
    
    for item in res.data:
        cand = item.get('candidate')
        if not cand: continue
        
        firm = cand.get('firm')
        firm_counts[firm] = firm_counts.get(firm, 0) + 1
        
        if not cand.get('linkedin_image_url'):
            no_image_count += 1
            if count := firm_counts.get(firm):
                 if count <= 2: # Just sample a few
                     print(f"Missing Image: {cand['full_name']} ({firm})")

    print("\nFirm Counts:")
    for firm, count in sorted(firm_counts.items(), key=lambda x: x[1], reverse=True):
        if count > 1:
            print(f"{firm}: {count}")
            
    print(f"\nTotal Missing Images: {no_image_count}/{len(res.data)}")

def test_fetch(name, firm):
    print(f"\n--- Testing Fetch for {name} ---")
    engine = ImageProxyEngine()
    
    # 1. Search
    print("Searching...")
    try:
        res = engine.fetch_image(name, firm)
        print(f"Result: {res}")
        
        if url := res.get('imageUrl'):
            print(f"Verifying {url}...")
            # Create a dummy class to access the verify method if it's an instance method
            # Actually I can just call the engine method if accessible
            # Checking code... verify_accessibility is instance method
            
            is_valid = engine.verify_accessibility(url)
            print(f"Accessible (Strict): {is_valid}")
            
    except Exception as e:
        print(f"Fetch Failed: {e}")

if __name__ == "__main__":
    analyze()
    test_fetch("Ashley Bradford", "Lockton Companies")
