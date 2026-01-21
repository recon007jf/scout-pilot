
import os
import datetime
from supabase import create_client
from collections import Counter

# Setup Supabase (Using Service Role to mimic backend)
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not url or not key:
    print("Error: Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
    exit(1)

sb = create_client(url, key)

print(f"--- DIAGNOSTIC: Status Distribution ---")

try:
    # Fetch all statuses
    res = sb.table("target_brokers").select("status").execute()
    statuses = [row['status'] for row in res.data]
    
    counts = Counter(statuses)
    
    print("\ntarget_brokers Status Breakdown:")
    for status, count in counts.items():
        print(f"  {status}: {count}")

except Exception as e:
    print(f"Query Failed: {e}")
