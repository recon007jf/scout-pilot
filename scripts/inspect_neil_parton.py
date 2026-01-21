import os
import json
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

supabase = create_client(url, key)

def inspect_target(name):
    print(f"--- Inspecting: {name} ---")
    res = supabase.table("target_brokers").select("*").ilike("full_name", f"%{name}%").execute()
    
    if not res.data:
        print("❌ No record found.")
        return

    record = res.data[0]
    print(json.dumps(record, indent=2))

if __name__ == "__main__":
    inspect_target("Neil Parton")
