import os
import sys
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not url or not key:
    print("Missing Env Vars")
    sys.exit(1)

db = create_client(url, key)

def reset_targets():
    print("Resetting Kevin & Andrew to POOL...")
    names = ["Kevin Overbey", "Andrew Forchelli", "Steve Wolfenberger"]
    
    for name in names:
        # 1. Reset Status and Clear Image (Force Refetch)
        res = db.table("candidates").update({
            "status": "POOL",
            "linkedin_image_url": None, # Force re-enrichment
            "draft_ready": False
        }).eq("full_name", name).execute()
        
        if res.data:
            print(f"✅ Reset {name}")
        else:
            print(f"❌ Could not find {name}")

    # Also clean queue again just in case
    # db.table("morning_briefing_queue").delete().in_("candidate_id", [ids])... 
    # But reset_queue already cleared today's queue.
    
if __name__ == "__main__":
    reset_targets()
