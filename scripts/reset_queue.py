import os
import sys
from supabase import create_client

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from app.config import settings

db = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_ROLE_KEY)

def reset_queue():
    print("🧹 Resetting Today's Queue...")
    today = "2026-01-17" 
    
    # Check count first
    res = db.table("morning_briefing_queue").select("id").eq("selected_for_date", today).execute()
    print(f"Deleting {len(res.data)} items...")
    
    db.table("morning_briefing_queue").delete().eq("selected_for_date", today).execute()
    print("Done. Queue is empty.")

if __name__ == "__main__":
    reset_queue()
