import sys
import os
from supabase import create_client

# Path setup
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from app.config import settings

def inspect_schema():
    print("Inspecting Schema (Service Role)...")
    admin_db = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_ROLE_KEY)
    
    # 1. Check if 'candidates' table exists
    try:
        res = admin_db.table("candidates").select("*").limit(1).execute()
        print("Found 'candidates' table.")
    except Exception:
        print("'candidates' table NOT found. Assuming target_brokers.")

    # 2. Inspect 'target_brokers' columns if possible (via checking a row)
    try:
        res = admin_db.table("target_brokers").select("*").limit(1).execute()
        if res.data:
            print(f"target_brokers sample keys: {res.data[0].keys()}")
            print(f"target_brokers sample status: {res.data[0].get('status')}")
        else:
            print("target_brokers is empty, cannot inspect keys via row.")
            
    except Exception as e:
        print(f"Error inspecting target_brokers: {e}")

if __name__ == "__main__":
    inspect_schema()
