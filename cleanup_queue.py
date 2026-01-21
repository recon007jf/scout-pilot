
import os
from supabase import create_client
import datetime

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
sb = create_client(url, key)
today = datetime.date.today().isoformat()

print(f"--- Cleanup for {today} ---")

# Scan logic again to identify IDs
q_res = sb.table("morning_briefing_queue").select("id, candidate_id").eq("selected_for_date", today).execute()
ids = [i['candidate_id'] for i in q_res.data]
# Check Candidates
c_res = sb.table("candidates").select("id, email, linkedin_image_url").in_("id", ids).execute()

bad_ids = []
for c in c_res.data:
    if not c.get('email') or not c.get('linkedin_image_url'):
        bad_ids.append(c.get('id'))

print(f"Found {len(bad_ids)} bad candidates.")

if bad_ids:
    print(f"Removing {bad_ids} from Queue...")
    # Delete from queue where candidate_id in bad_ids
    sb.table("morning_briefing_queue").delete().in_("candidate_id", bad_ids).eq("selected_for_date", today).execute()
    print("Purged.")
else:
    print("Clean.")
