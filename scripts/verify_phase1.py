import sys
import os
from supabase import create_client

# Path setup
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from app.config import settings
from app.core.safety_phase1 import check_rolling_velocity

def verify_phase1():
    print("Verifying Phase 1 (Service Role)...")
    admin_db = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_ROLE_KEY)
    
    # 1. Verify QUEUED count
    res = admin_db.table("candidates").select("*").eq("status", "QUEUED").eq("draft_ready", True).execute()
    count = len(res.data)
    print(f"QUEUED & READY Count: {count}")
    
    if count == 20:
        print("PASS: Buffer target met.")
    else:
        print(f"FAIL: Expected 20, got {count}")
        
    # 2. Verify Draft Body
    completed = 0
    for c in res.data:
        if c.get("draft_body") and len(c.get("draft_body")) > 10:
            completed += 1
            
    print(f"Draft Bodies Populated: {completed}/{count}")
    if completed == count and count > 0:
         print("PASS: Draft content present.")
    
    # 3. Verify Velocity Check
    vel = check_rolling_velocity(admin_db)
    print(f"Rolling Velocity (Last 24h): {vel}")
    # Should be 0 since we haven't 'SENT' anything
    if vel == 0:
        print("PASS: Velocity check clean.")

if __name__ == "__main__":
    verify_phase1()
