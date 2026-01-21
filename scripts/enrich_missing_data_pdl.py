import os
import json
import time
import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

PDL_API_KEY = os.getenv("PDL_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not all([PDL_API_KEY, SUPABASE_URL, SUPABASE_KEY]):
    print("❌ Error: Missing API Keys")
    exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
pdl_url = "https://api.peopledatalabs.com/v5/person/enrich"

def enrich_target(target):
    print(f"\n🔍 Enriching: {target['full_name']} @ {target['firm']}")
    
    # Construct params, filtering out None values
    params = {
        "api_key": PDL_API_KEY,
        "name": target['full_name'],
        "company": target['firm'],
        "min_likelihood": 6
    }
    
    if target.get('region'):
        params['location'] = target['region']
    if target.get('role'):
        params['title'] = target['role']
    
    try:
        response = requests.get(pdl_url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            if data['status'] == 200:
                pdl_data = data['data']
                
                updates = {}
                
                # 1. LinkedIn URL
                current_linkedin = target.get('linkedin_url')
                pdl_linkedin = pdl_data.get('linkedin_url')
                
                if not current_linkedin and pdl_linkedin:
                    # Clean URL if needed? PDL usually returns valid full URLs
                    updates['linkedin_url'] = pdl_linkedin
                    print(f"   ✅ Found LinkedIn: {pdl_linkedin}")
                
                # 2. Work Email
                current_email = target.get('work_email')
                if not current_email:
                    # PDL returns 'emails' list with 'type'
                    emails = pdl_data.get('emails', [])
                    # Try to find professional
                    work_email = next((e['address'] for e in emails if e.get('type') == 'professional'), None)
                    # If no professional, maybe take first if we are desperate? 
                    # Let's stick to professional/current or NULL to avoid personal spam
                    
                    # Actually, for "work_email" column, strictly prefer professional
                    if work_email:
                        updates['work_email'] = work_email
                        print(f"   ✅ Found Work Email: {work_email}")
                    else:
                        print("   ⚠️ No professional email found.")

                if updates:
                    res = supabase.table("target_brokers").update(updates).eq("id", target['id']).execute()
                    if res.data:
                        print("   💾 Database updated.")
                    else:
                        print("   ❌ Update failed.")
                else:
                    print("   ⚠️ No new relevant data found (or already matches).")
            else:
                 print(f"   ⚠️ PDL Match Status: {data['status']}")
        else:
            print(f"   ❌ PDL API Error: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"   ❌ Exception: {e}")

def main():
    print("🚀 Starting Bulk PDL Enrichment...")
    
    # 1. Fetch targets
    res = supabase.table("target_brokers").select("*").execute()
    targets = res.data
    
    # 2. Filter for missing data
    missing_queue = []
    for t in targets:
        # Check for missing crucial fields
        if not t.get('linkedin_url') or not t.get('work_email'):
            missing_queue.append(t)
            
    print(f"📋 Found {len(missing_queue)} targets missing key data (LinkedIn/Email).")
    
    # 3. Process
    for t in missing_queue:
        enrich_target(t)
        time.sleep(1.5) # Be nice to rate limits

    print("\n✅ Enrichment Run Complete.")

if __name__ == "__main__":
    main()
