
import csv
import uuid
import os
from app.main import get_service_db

# CSV Path
CSV_PATH = "Master_Hunting_List_Production_v3_SYSTEM_ENRICHED_FUNDING_PATCHED.csv"

def ingest_daily_batch(limit=50):
    db = get_service_db()
    print(f"🚀 Starting Daily Batch Ingestion (Limit: {limit})...")
    
    # 1. Read CSV
    if not os.path.exists(CSV_PATH):
        print(f"❌ Error: CSV not found at {CSV_PATH}")
        return

    candidates = []
    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            candidates.append(row)
    
    print(f"📊 Loaded {len(candidates)} rows from CSV.")

    # 2. Filter & Map
    to_insert = []
    
    # Fetch existing emails to avoid duplicates
    existing_res = db.table("target_brokers").select("work_email").execute()
    existing_emails = set([r['work_email'] for r in existing_res.data if r.get('work_email')])

    count = 0
    for row in candidates:
        if count >= limit:
            break

        # Map Fields
        full_name = row.get("Contact Full Name", "").strip()
        firm = row.get("Company Name", "").strip()
        role = row.get("Contact Job Title", "").strip()
        email = row.get("Contact Email", "").strip()
        linkedin = row.get("LinkedIn_URL", "").strip()
        state = row.get("firm_state", "").strip()

        # P0 Hard Gate Check
        if not (full_name and firm and role and email and linkedin):
            continue # Skip incomplete
        
        # Dedupe
        if email in existing_emails:
            continue

        # Create Draft (Placeholder for Zero-Latency)
        # In a real cycle, this would be an LLM call. For ingestion speed, we use a template.
        first_name = full_name.split(" ")[0]
        subject = f"Connect regarding {firm}'s benefits strategy"
        body = f"Hi {first_name},\n\nI've been following {firm}'s growth in {state} and noticed some trends in the self-funded market that align with your role as {role}.\n\nWe've helped similar firms optimize their captured risk. Open to a brief chat?\n\nBest,\nAndrew"

        target = {
            "id": str(uuid.uuid4()),
            "full_name": full_name,
            "firm": firm,
            "role": role,
            "work_email": email,
            "linkedin_url": linkedin,
            "region": state,
            "status": "ENRICHED", # Valid status for Queue
            "llm_email_subject": subject,
            "llm_email_body": body,
            "created_at": "2026-01-18T12:00:00Z" # Simulating fresh batch
        }
        
        to_insert.append(target)
        existing_emails.add(email) # Prevent dups within batch
        count += 1

    print(f"✅ Prepared {len(to_insert)} valid P0 candidates for ingestion.")

    # 3. Bulk Insert
    if to_insert:
        try:
            db.table("target_brokers").insert(to_insert).execute()
            print(f"🎉 Successfully inserted {len(to_insert)} candidates into target_brokers.")
        except Exception as e:
            print(f"❌ Batch Insert Failed: {e}")
            # Fallback to one-by-one
            for t in to_insert:
                try:
                     db.table("target_brokers").insert(t).execute()
                     print(f"   + Inserted {t['full_name']}")
                except Exception as inner_e:
                     print(f"   ! Failed {t['full_name']}: {inner_e}")

if __name__ == "__main__":
    ingest_daily_batch()
