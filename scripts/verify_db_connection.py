import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

try:
    print(f"🔌 Connecting to {SUPABASE_URL}...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # Lightweight verification
    response = supabase.table("targets").select("*").limit(1).execute()
    print("✅ Connection Successful!")
    print(f"📊 Targets Count: {len(response.data)}")
    
except Exception as e:
    print(f"❌ Connection Failed: {e}")
