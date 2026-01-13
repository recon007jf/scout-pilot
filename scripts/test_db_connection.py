import os
import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not url or not key:
    print("Missing DB credentials")
    exit(1)

supabase: Client = create_client(url, key)

DOSSIER_ID = "acc5f009-6d33-4118-8e91-6ac346a1f662"

print(f"Testing Access to 'drafts' table for dossier: {DOSSIER_ID}")

try:
    # 1. Try Select
    print("1. Selecting...")
    res = supabase.table("drafts").select("*").eq("dossier_id", DOSSIER_ID).execute()
    print(f"Select Result: {res.data}")

    # 2. Try Upsert
    print("2. Upserting...")
    payload = {
        "dossier_id": DOSSIER_ID,
        "status": "test_script_check",
        "updated_at": datetime.datetime.utcnow().isoformat()
    }
    # Note: On Conflict is key
    res = supabase.table("drafts").upsert(payload, on_conflict="dossier_id").execute()
    print(f"Upsert Result: {res.data}")

    # 3. Clean up? No, leave it for debug.
    print("SUCCESS: scripts/test_db_connection.py passed.")

except Exception as e:
    print(f"FAILURE: {e}")
