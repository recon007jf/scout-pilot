import os
import sys
from supabase import create_client
import uuid

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from app.config import settings

def seed_candidates():
    print("Seeding Candidates (Service Role)...")
    admin_db = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_ROLE_KEY)
    
    # Generate 50 mock candidates
    candidates = []
    for i in range(50):
        candidates.append({
            "full_name": f"Mock Candidate {i+1}",
            "firm": "Mock Firm Inc.",
            "role": "CEO",
            "email": f"mock{i+1}@example.com",
            "status": "POOL",
            "draft_ready": False
        })
    
    try:
        res = admin_db.table("candidates").insert(candidates).execute()
        print(f"Seeded {len(res.data)} candidates into POOL.")
    except Exception as e:
        print(f"FAILED to seed: {e}")

if __name__ == "__main__":
    seed_candidates()
