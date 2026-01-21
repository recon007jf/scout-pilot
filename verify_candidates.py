
import os
from supabase import create_client
from collections import Counter

# Setup Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not url or not key:
    print("Error imports")
    exit(1)

sb = create_client(url, key)

print("--- DIAGNOSTIC: Candidates Table ---")

try:
    # Check if 'candidates' table exists and has rows
    try:
        res = sb.table("candidates").select("*", count="exact").limit(1).execute()
        count = res.count
        print(f"Candidates Table Count: {count}")
        
        if count > 0:
            # Check Status
            res = sb.table("candidates").select("status").execute()
            statuses = [row['status'] for row in res.data]
            print(f"Candidates Status Breakdown: {Counter(statuses)}")
        else:
            print("Candidates Table is EMPTY.")

    except Exception as ie:
        print(f"Candidates Table Query Failed (might not exist): {ie}")

    # Check 'target_brokers' again to be sure
    res = sb.table("target_brokers").select("*", count="exact").limit(1).execute()
    print(f"Target Brokers Count: {res.count}")

except Exception as e:
    print(f"General Error: {e}")
