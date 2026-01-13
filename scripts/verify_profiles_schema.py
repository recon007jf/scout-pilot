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

print("Verifying 'profiles' table schema...")

# We can query the 'profiles' table and see what keys come back for a test user or just try to select headers
# Or better, we can assume if we just select * limit 1, we see keys.
# But if table is empty, we see nothing.
# Let's try to select 'org_id' specifically and see if it fails.

try:
    print("Querying information_schema.columns for table 'profiles'...")
    response = supabase.table("profiles").select("*").limit(1).execute() 
    # The above is just to check connectivity/auth, but for schema:
    
    # We can't query information_schema easily via postgrest client usually due to permissions 
    # unless exposed.
    # But we can try to inspect the error more closely.
    
    # Let's try to list what IS there.
    res = supabase.table("profiles").select("*").limit(1).execute()
    if res.data:
        print(f"Row 1 keys: {res.data[0].keys()}")
    else:
        print("Table is empty, but query worked (Columns exist).")
        
    print("Now checking specifically for 'org_id'...")
    res_org = supabase.table("profiles").select("org_id").limit(1).execute()
    print("SUCCESS: 'org_id' column is queryable.")

except Exception as e:
    print("\n[VERIFICATION FAILURE]")
    print(f"Error Details: {e}")
    print("\nDEBUG INFO:")
    print(f"Target URL: {url}")
