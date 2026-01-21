import os
import sys
from datetime import date
from supabase import create_client
from dotenv import load_dotenv

# Load env variables from backend/.env
# Assuming we run this from backend directory or similar
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

SUPABASE_URL = os.environ.get("SUPABASE_URL") 
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("Error: detailed environment variables missing (SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)")
    # Fallback for local execution context if .env is tricky
    # Trying to read from known location if load_dotenv fails relative path
    try:
        load_dotenv("/Users/josephlf/.gemini/antigravity/scratch/backend/.env")
        SUPABASE_URL = os.environ.get("SUPABASE_URL") 
        SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    except:
        pass

if not SUPABASE_URL or not SUPABASE_KEY:
    print("CRITICAL: Still missing keys.")
    sys.exit(1)

def inject():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    print(f"Connecting to Supabase...")
    
    # Test Data
    dossier_id = "be51faf4-1267-4f2e-a8cb-7c9fe1fe0c96"
    today_str = date.today().isoformat()
    
    data = {
        "dossier_id": dossier_id,
        "candidate_id": dossier_id, # FIX: Ensure FK is populated for join
        "selected_for_date": today_str,
        "status": "pending",
        "priority_score": 99,
        "ranking_reason": "Antigravity Verification Injection",
        "draft_preview": "This is a test draft generated for verification."
        # Note: If 'user_id' is required by constraint, we might fail here.
        # But based on code review, it wasn't explicit in the select.
        # Let's try.
    }
    
    print(f"Injecting queue item for {today_str}...")
    
    try:
        # Upsert based on dossier_id + date? 
        # Or just insert. Let's try upsert if we know the PK, but we don't.
        # morning_briefing_queue likely has (dossier_id, selected_for_date) unique constraint?
        # Let's try simple insert first.
        res = supabase.table("morning_briefing_queue").insert(data).execute()
        print("Success:", res.data)
    except Exception as e:
        print(f"Insert failed: {e}")
        print("Attempting to read existing queue to debug...")
        try:
             res = supabase.table("morning_briefing_queue").select("*").eq("selected_for_date", today_str).execute()
             print("Current Queue for Today:", res.data)
        except Exception as e2:
             print(f"Read failed: {e2}")

if __name__ == "__main__":
    inject()
