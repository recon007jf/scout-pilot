
import os
from supabase import create_client
import datetime

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
sb = create_client(url, key)
today = datetime.date.today().isoformat()

print(f"--- Fixing Batches for {today} ---")
# Update 90 -> 5
sb.table("morning_briefing_queue").update({"priority_score": 5, "ranking_reason": "Batch 5 (Normalized)"})\
    .eq("selected_for_date", today).eq("priority_score", 90).execute()
print("Fixed.")
