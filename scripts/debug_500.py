import os
import sys
from supabase import create_client

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from app.config import settings

def debug():
    print("--- DEBUGGING 500 ERROR ---")
    
    # 1. Test Lazy Import of Auth
    print("[1] Testing app.core.auth_clerk import...")
    try:
        from app.core.auth_clerk import clerk_verifier
        print("    ✅ Import successful")
    except Exception as e:
        print(f"    ❌ Import FAILED: {e}")
        import traceback
        traceback.print_exc()

    # 2. Test Safety Engine
    print("\n[2] Testing SafetyEngine.get_outreach_status...")
    try:
        db = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_ROLE_KEY)
        from app.core.safety import SafetyEngine
        safety = SafetyEngine(db)
        res = safety.get_outreach_status(user_email=None)
        print(f"    ✅ Success (None): {res}")
    except Exception as e:
        print(f"    ❌ SafetyEngine FAILED: {e}")
        import traceback
        traceback.print_exc()

    # 3. Test Missing Key (Simulate Cloud Run env missing)
    print("\n[3] Testing Missing Service Role Key...")
    try:
        bad_db = create_client(settings.SUPABASE_URL, None)
        print("    ⚠️ Created client with None key (Unexpected success?)")
        # Try to use it
        bad_db.table("global_outreach_status").select("*").execute()
    except Exception as e:
        print(f"    ✅ CRASH CONFIRMED with missing key: {e}")

if __name__ == "__main__":
    debug()
