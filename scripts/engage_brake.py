import sys
import os
from supabase import create_client

# Path setup
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from app.config import settings

def engage_safety_brake():
    print("Engaging Global Safety Brake...")
    db = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
    
    # Force Update (Simple)
    res = db.table("global_outreach_status").update({"status": "paused"}).eq("id", 1).execute()
    
    # Verify
    verify = db.table("global_outreach_status").select("*").eq("id", 1).execute()
    print("Final Status:")
    print(verify.data[0])

if __name__ == "__main__":
    try:
        engage_safety_brake()
    except Exception as e:
        print(f"Error: {e}")
