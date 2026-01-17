import asyncio
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# Setup Supabase
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(supabase_url, supabase_key)

TARGET_ID = "b30ecd4b-46e9-440b-80df-a7314c1137fa" # Queued Kev
NEW_IMAGE_URL = "https://media.licdn.com/dms/image/v2/D5603AQGt6R8az7FWEQ/profile-displayphoto-shrink_400_400/profile-displayphoto-shrink_400_400/0/1694020995279?e=1770249600&v=beta&t=GWcAt02OItD0CiqfdDavzbJibDA9b77Vrz8u5TuUCuc"

async def fix_kevin():
    print(f"Update image for candidate {TARGET_ID}...")
    
    response = supabase.table("candidates").update({
        "linkedin_image_url": NEW_IMAGE_URL
    }).eq("id", TARGET_ID).execute()
    
    print(f"✅ Update complete.")
    print(response.data)

if __name__ == "__main__":
    asyncio.run(fix_kevin())
