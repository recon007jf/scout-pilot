import os
import json
import random
from app.services.enrichment_service import EnrichmentService
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# --- Config ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

enrichment = EnrichmentService()

SEEDS = [
    {"company": "SpaceX", "location": "Hawthorne, CA", "titles": ["CFO", "VP Finance", "Head of Finance"]},
    {"company": "Rivian", "location": "Irvine, CA", "titles": ["Chief People Officer", "VP HR", "Head of People"]},
    {"company": "Chipotle Mexican Grill", "location": "Newport Beach, CA", "titles": ["Director of Real Estate", "VP Development", "Chief Development Officer"]}
]

def seed_db():
    print("🌱 Starting Seed Process (Real Humans, Mock Drafts)...")
    
    for seed in SEEDS:
        print(f"🔎 Enriching target for: {seed['company']}...")
        
        # 1. Real Identity Resolution
        person = enrichment.find_person(
            company_name=seed['company'],
            location=seed['location'],
            title_keywords=seed['titles']
        )
        
        if not person.get('success'):
            print(f"⚠️ Failed to find person for {seed['company']}: {person.get('error')}")
            print(f"⚠️ Applying MOCK FALLBACK for {seed['company']}...")
            # Fallback Mock Data
            person = {
                "company": seed['company'],
                "name": f"Mock Exec ({seed['company']})",
                "first_name": "Mock",
                "title": seed['titles'][0],
                "email": f"mock@{seed['company'].lower().replace(' ', '')}.com",
                "linkedin": f"https://linkedin.com/in/mock-{seed['company'].lower()}",
                "confidence": 0.5
            }
            
        print(f"✅ Found/Mocked: {person.get('name')} ({person.get('title')})")
        
        # 2. Insert into Targets (Table A)
        target_data = {
            "company_name": person.get('company'),
            "person_name": person.get('name'),
            "email": person.get('email'),
            "title": person.get('title'),
            "linkedin_url": person.get('linkedin'),
            "pdl_confidence": person.get('confidence'),
            "do_not_contact": False,
            "raw_data": json.loads(json.dumps(person)) # Ensure JSON serializable
        }
        
        try:
            # Check if exists first to avoid dupes? For MVP, we'll just insert/upsert match on email maybe?
            # Or simplified: Insert and ignore ID. 
            # Supabase upsert:
            # But we don't have a unique constraint on email defined in schema_mvp (should have, but adhering to user schema).
            # We'll just insert. If run multiple times, we get dupes. That's fine for Seed.
            t_res = supabase.table("targets").insert(target_data).execute()
            target_id = t_res.data[0]['id']
            
        except Exception as e:
            print(f"❌ DB Error inserting target: {e}")
            continue

        # 3. Create Draft (Table B) - VARIANCE LOGIC
        
        # Variance Logic
        rand_val = random.random()
        
        if rand_val > 0.6:
            # High Confidence
            sys_conf = 0.98
            reason = f"Perfect Title Match ({person.get('title')}) - Strong Fit"
            priority = 90
            subject = f"Question for {person.get('first_name')} re: {seed['company']} Benefits"
        elif rand_val > 0.3:
             # Med Confidence
            sys_conf = 0.75
            reason = f"Title Match ({person.get('title')}) - No C-Level found, using VP"
            priority = 60
            subject = f"Introduction: {seed['company']} + Scout"
        else:
            # Lower Priority
            sys_conf = 0.45
            reason = "Role inferred from department match. Review needed."
            priority = 40
            subject = f"Connect regarding {seed['company']}"

        draft_data = {
            "target_id": target_id,
            "status": "draft",
            "priority_score": priority,
            "system_confidence": sys_conf,
            "selection_reason": reason, # Ensure key matches schema
            "email_subject": subject,
            "email_body": f"Hi {person.get('first_name')},\n\nI noticed {seed['company']} is expanding..."
        }
        
        try:
            supabase.table("outreach_queue").insert(draft_data).execute()
            print(f"📝 Draft Created: Priority {priority}, Conf {sys_conf}")
        except Exception as e:
             print(f"❌ DB Error inserting draft: {e}")

    print("🏁 Seed Complete.")

if __name__ == "__main__":
    seed_db()
