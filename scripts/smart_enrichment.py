import os
import requests
import time
import json
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# Config
SERPER_API_KEY = os.getenv("SERPER_API_KEY")
PDL_API_KEY = os.getenv("PDL_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not all([SERPER_API_KEY, PDL_API_KEY, SUPABASE_URL, SUPABASE_KEY]):
    print("❌ Error: Missing API Keys")
    exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Endpoints
SERPER_URL = "https://google.serper.dev/search"
PDL_ENRICH_URL = "https://api.peopledatalabs.com/v5/person/enrich"

def find_linkedin_via_serper(name, firm, region=None):
    """Step 1: Use Google Search to find LinkedIn Profile."""
    query = f"{name} {firm} LinkedIn"
    if region:
        query += f" {region}"
        
    print(f"   🔍 Serper Search: '{query}'")
    
    payload = {
        "q": query,
        "num": 3,
        "gl": "us",
        "hl": "en"
    }
    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}
    
    try:
        resp = requests.post(SERPER_URL, headers=headers, json=payload)
        if resp.status_code != 200:
            print(f"      ❌ Serper Error: {resp.status_code}")
            return None
            
        organic = resp.json().get("organic", [])
        for item in organic:
            link = item.get("link", "")
            if "linkedin.com/in/" in link:
                print(f"      ✅ Found URL: {link}")
                return link
                
        print("      ⚠️ No LinkedIn URL found in top results.")
        return None
    except Exception as e:
        print(f"      ❌ Exception: {e}")
        return None

def enrich_via_pdl_url(linkedin_url):
    """Step 2: Use LinkedIn URL to fetch Email via PDL."""
    print(f"   🧬 PDL Lookup via URL: {linkedin_url}")
    
    params = {
        "api_key": PDL_API_KEY,
        "profile": linkedin_url,
        "min_likelihood": 6
    }
    
    try:
        resp = requests.get(PDL_ENRICH_URL, params=params)
        if resp.status_code == 200:
            data = resp.json()
            if data['status'] == 200:
                return data['data']
            else:
                print(f"      ⚠️ PDL Status: {data['status']}")
                return None
        else:
            print(f"      ❌ PDL Error: {resp.status_code}")
            return None
    except Exception as e:
        print(f"      ❌ Exception: {e}")
        return None

def main():
    print("🚀 Starting Smart Enrichment Pipeline (Triangulation)...")
    
    # 1. Fetch Targets
    res = supabase.table("target_brokers").select("*").execute()
    targets = res.data
    
    for t in targets:
        updates = {}
        needs_update = False
        
        name = t.get("full_name")
        firm = t.get("firm")
        region = t.get("region")
        
        # --- PATH A: Missing LinkedIn? ---
        current_linkedin = t.get("linkedin_url")
        if not current_linkedin:
            print(f"\n[Target: {name}] Missing LinkedIn. Triangulating...")
            found_url = find_linkedin_via_serper(name, firm, region)
            
            if found_url:
                updates["linkedin_url"] = found_url
                current_linkedin = found_url # Use for next step
                needs_update = True
                
        # --- PATH B: Missing Email? ---
        current_email = t.get("work_email")
        # Only try if we HAVE a linkedin URL (either existing or just found)
        if not current_email and current_linkedin:
            print(f"\n[Target: {name}] Missing Email. enriching via PDL...")
            pdl_data = enrich_via_pdl_url(current_linkedin)
            
            if pdl_data:
                # Extract Email
                emails = pdl_data.get('emails', [])
                work_email = next((e['address'] for e in emails if e.get('type') == 'professional'), None)
                
                if work_email:
                     print(f"      📧 Found Email: {work_email}")
                     updates["work_email"] = work_email
                     needs_update = True
                     
                # Bonus: Backfill Image if still generic?
                # (Skipping to keep scope tight, but PDL has images too)

        # --- Commit Updates ---
        if needs_update and updates:
             res = supabase.table("target_brokers").update(updates).eq("id", t["id"]).execute()
             print("      💾 Database updated.")
        
        time.sleep(1) # Rate limit niceness

    print("\n✅ Smart Enrichment Complete.")

if __name__ == "__main__":
    main()
