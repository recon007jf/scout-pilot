
import os
from supabase import create_client
import datetime

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
sb = create_client(url, key)
today = datetime.date.today().isoformat()

print(f"--- Data Quality Check for {today} ---")

# Get Queue Items
q_res = sb.table("morning_briefing_queue").select("candidate_id, priority_score").eq("selected_for_date", today).execute()
q_items = q_res.data
print(f"Queue Size: {len(q_items)}")

if not q_items:
    exit()

# Sample 5 IDs -> Check ALL
ids = [i['candidate_id'] for i in q_items]

# Check Candidates
print("\n[CANDIDATES Table - Read Model]")
c_res = sb.table("candidates").select("id, full_name, email, linkedin_image_url").in_("id", ids).execute()

missing_email = 0
missing_img = 0
for c in c_res.data:
    has_email = bool(c.get('email'))
    has_img = bool(c.get('linkedin_image_url'))
    if not has_email: missing_email += 1
    if not has_img: missing_img += 1

print(f"Audit Result ({len(c_res.data)} items):")
print(f"  Missing Email: {missing_email}")
print(f"  Missing Image: {missing_img}")

if missing_email > 0 or missing_img > 0:
    print("WARNING: Data gaps detected.")
