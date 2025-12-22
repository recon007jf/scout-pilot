import os
from supabase import create_client
from dotenv import load_dotenv

def debug_connectivity():
    BASE_PATH = "/Users/josephlf/.gemini/antigravity/scratch"
    load_dotenv(os.path.join(BASE_PATH, ".env"))
    
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    print(f"Connecting to {url}...")
    print(f"Project Reference: {url.split('//')[1].split('.')[0]}")
    supabase = create_client(url, key)
    
    try:
        # Check known table
        res = supabase.table("scout_drafts").select("count", count="exact").limit(1).execute()
        print(f"[SUCCESS] Connected to scout_drafts. Count: {res.count}")
    except Exception as e:
        print(f"[ERROR] Failed to connect to scout_drafts: {e}")

    try:
        # Check new table again
        res2 = supabase.table("target_brokers").select("count", count="exact").limit(1).execute()
        print(f"[SUCCESS] Connected to target_brokers. Count: {res2.count}")
    except Exception as e:
        print(f"[ERROR] Failed to connect to target_brokers: {e}")

if __name__ == "__main__":
    debug_connectivity()
