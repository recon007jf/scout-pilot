import asyncio
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# Setup Supabase
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(supabase_url, supabase_key)

NEW_IMAGE_URL = "https://media.licdn.com/dms/image/v2/C5603AQF1Tqfb0iJFLA/profile-displayphoto-shrink_400_400/profile-displayphoto-shrink_400_400/0/1659381296092?e=1770249600&v=beta&t=qn8AdEDD_rCRlYmMUImuwUCZXm1b_AI7jrsGthSD_I0"

async def fix_andrew():
    print(f"Searching for Andrew Forchelli...")
    
    # Find ID
    res = supabase.table("candidates").select("id, full_name").ilike("full_name", "%Andrew Forchelli%").execute()
    
    if not res.data:
        print("❌ Andrew Forchelli not found in candidates table.")
        return

    target_id = res.data[0]['id']
    print(f"Found {res.data[0]['full_name']} ({target_id})")

    print(f"Updating image URL...")
    
    response = supabase.table("candidates").update({
        "linkedin_image_url": NEW_IMAGE_URL
    }).eq("id", target_id).execute()
    
    print(f"✅ Update complete.")
    print(response.data)

if __name__ == "__main__":
    asyncio.run(fix_andrew())
