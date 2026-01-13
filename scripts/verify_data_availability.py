import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY")

if not url or not key:
    print("Error: Missing SUPABASE_URL or SUPABASE_KEY")
    exit(1)

supabase: Client = create_client(url, key)

ORG_ID = "df966238-4b56-4ed3-886c-157854d8ce90"

print(f"Verifying Data Availability for Org: {ORG_ID}...")

# Count dossiers
try:
    response = supabase.table("dossiers").select("*", count="exact").eq("org_id", ORG_ID).execute()
    count = response.count
    print(f"\n[DRY RUN] COUNT dossiers WHERE org_id = '{ORG_ID}'")
    print(f"RESULT: {count} records found.")
except Exception as e:
    print(f"Query Failed: {e}")
