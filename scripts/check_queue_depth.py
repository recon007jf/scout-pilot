
from app.main import get_service_db
import json

db = get_service_db()
# Fetch ALL, not just status filtered, to see total potential
all_targets = db.table('target_brokers').select('*').execute().data
total = len(all_targets)
valid_count = 0
invalid_candidates = []

for t in all_targets:
    # Use correct DB Column Names
    is_valid = (
        t.get('full_name') and 
        t.get('work_email') and 
        t.get('linkedin_url') and 
        t.get('firm') and 
        t.get('role') and 
        t.get('llm_email_subject') and 
        t.get('llm_email_body')
    )
    if is_valid:
        valid_count += 1
    else:
        invalid_candidates.append({
            "name": t.get('full_name', 'Unknown'),
            "missing": [k for k in ['full_name','work_email','linkedin_url','firm','role','llm_email_subject','llm_email_body'] if not t.get(k)]
        })

print(f"Total Database Targets: {total}")
print(f"Valid P0 Candidates: {valid_count}")
print(f"Invalid Candidates: {len(invalid_candidates)}")
if invalid_candidates:
    print("Invalid Details:")
    for c in invalid_candidates:
        print(f" - {c['name']}: Missing {c['missing']}")
