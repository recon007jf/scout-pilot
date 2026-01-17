import os
import sys
import json
from supabase import create_client

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from app.config import settings

db = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_ROLE_KEY)

def promote():
    print("🚀 Promoting Dossiers to Candidates (Real Data Injection)...")
    
    # 1. Fetch Dossiers
    # Assuming 'raw_data' contains 'Full Name', 'Company', etc.
    res = db.table("dossiers").select("*").execute()
    dossiers = res.data
    print(f"Found {len(dossiers)} dossiers.")
    
    promoted = 0
    for d in dossiers:
        raw = d.get("raw_data") or {}
        name = raw.get("Full Name") or raw.get("name")
        firm = raw.get("Company") or raw.get("company")
        
        if not name:
            continue
            
        # Check if exists
        check = db.table("candidates").select("id").eq("full_name", name).execute()
        if check.data:
            print(f"  Skipping {name} (Exists)")
            continue
            
        # Insert
        payload = {
            "full_name": name,
            "firm": firm or "Unknown Firm",
            "role": raw.get("Title") or raw.get("title") or "Executive",
            "email": raw.get("Email") or raw.get("email"),
            "linkedin_url": raw.get("LinkedIn") or raw.get("linkedin"),
            "status": "POOL",
            "draft_body": f"Subject: Connection\n\nHi {name.split(' ')[0]},\n\nI saw your work at {firm}.\n\nBest,\nAndrew" 
        }
        
        try:
            db.table("candidates").insert(payload).execute()
            print(f"  ✅ Promoted: {name}")
            promoted += 1
        except Exception as e:
            print(f"  ❌ Failed {name}: {e}")

    print(f"Done. Promoted {promoted} candidates.")

if __name__ == "__main__":
    promote()
