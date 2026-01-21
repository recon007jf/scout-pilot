
import os
import datetime
from supabase import create_client

# Setup Supabase (Using Service Role to mimic backend)
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not url or not key:
    print("Error: Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
    exit(1)

sb = create_client(url, key)

today = datetime.date.today().isoformat()
print(f"--- DIAGNOSTIC: {today} ---")

try:
    # 1. Check Morning Briefing Queue for TODAY
    res = sb.table("morning_briefing_queue").select("count", count="exact").eq("selected_for_date", today).execute()
    queue_count = res.count
    print(f"Morning Briefing Queue (Today): {queue_count} rows")

    # 2. Check Total Queue (Any Date)
    res = sb.table("morning_briefing_queue").select("count", count="exact").execute()
    total_queue = res.count
    print(f"Morning Briefing Queue (Total): {total_queue} rows")

    # 3. Check Target Brokers (Pool)
    res = sb.table("target_brokers").select("count", count="exact").execute()
    pool_count = res.count
    print(f"Target Brokers (Total Pool): {pool_count} rows")
    
    # 4. Check "Ready" Candidates in Pool
    res = sb.table("target_brokers").select("count", count="exact").eq("status", "new").execute()
    ready_count = res.count
    print(f"Target Brokers (Status='new'): {ready_count} rows")

except Exception as e:
    print(f"Query Failed: {e}")
