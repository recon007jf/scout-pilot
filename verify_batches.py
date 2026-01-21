
import os
from supabase import create_client
import datetime

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not url or not key:
    print("Error imports")
    exit(1)

sb = create_client(url, key)
today = datetime.date.today().isoformat()

print(f"--- Verification for {today} ---")

try:
    res = sb.table("morning_briefing_queue").select("priority_score", count="exact").eq("selected_for_date", today).execute()
    print(f"Total Items: {len(res.data)}")
    
    from collections import Counter
    counts = Counter([r['priority_score'] for r in res.data])
    print("Batch Distribution (By Priority Score/BatchNum):")
    for score, count in sorted(counts.items()):
        print(f"  Batch {score}: {count} items")
        
except Exception as e:
    print(f"Error: {e}")
