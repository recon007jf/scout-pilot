
import os
from supabase import create_client

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not url or not key:
    print("Error imports")
    exit(1)

sb = create_client(url, key)

print("--- DIAGNOSTIC: Queue Dates ---")
try:
    res = sb.table("morning_briefing_queue").select("id, selected_for_date, status, candidate_id").execute()
    for row in res.data:
        print(row)
except Exception as e:
    print(e)
