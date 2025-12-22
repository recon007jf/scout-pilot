from dotenv import load_dotenv
import os
from supabase import create_client, Client

load_dotenv(os.path.join(os.getcwd(), '.env'))

def check_count():
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[Error] Missing Supabase Credentials in .env")
        return

    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        # Check simple connectivity and count
        res = supabase.table("silver_broker_map_2021").select("*", count="exact", head=True).execute()
        print(f"Count: {res.count}")
    except Exception as e:
        print(f"[Supabase Error] {e}")

if __name__ == "__main__":
    check_count()
