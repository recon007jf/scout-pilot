import asyncio
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# Setup Supabase
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(supabase_url, supabase_key)

async def check_kevin():
    print("Checking Kevin Overbey in DB...")
    
    # 1. Check Candidates Table
    response = supabase.table("candidates").select("*").ilike("full_name", "%Kevin Overbey%").execute()
    
    if not response.data:
        print("❌ No candidate found for Kevin Overbey")
    else:
        candidate = response.data[0]
        print(f"✅ Found Candidate: {candidate.get('id')}")
        print(f"   Name: {candidate.get('full_name')}")
        print(f"   Status: {candidate.get('status')}")
        print(f"   Image URL: {candidate.get('linkedin_image_url')}")
        print(f"   Dossier ID: {candidate.get('dossier_id')}")

        # 2. Check Dossier (Fallback)
        dossier_id = candidate.get('dossier_id')
        if dossier_id:
            d_response = supabase.table("dossiers").select("*").eq("id", dossier_id).execute()
            if d_response.data:
                dossier = d_response.data[0]
                print(f"   [Fallback] Dossier Image: {dossier.get('image_url')}")

if __name__ == "__main__":
    asyncio.run(check_kevin())
