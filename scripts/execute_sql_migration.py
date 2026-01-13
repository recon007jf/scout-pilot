import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY")

if not url or not key:
    print("Error: Missing SUPABASE_URL or SUPABASE_KEY/SERVICE_ROLE_KEY")
    exit(1)

supabase: Client = create_client(url, key)

print("Connecting to Supabase...")

sql_file = "app/db/migrations/002_ironclad_auth.sql"
with open(sql_file, "r") as f:
    sql = f.read()

# Since supabase-py doesn't have a direct 'query' method for raw DDL in all versions, 
# we often use the rpc call or the pg_meta endpoint if available. 
# However, for DDL, the dashboard SQL editor is best.
# BUT, if we have a function `exec_sql`, we can use it.
# Let's try to see if we can use the `rpc` method if a helper function exists, 
# otherwise we will have to stop and ask the user to run it manually.

print(f"Reading SQL from {sql_file}...")
print("-" * 20)
print(sql[:100] + "...")
print("-" * 20)

print("\nIMPORTANT: The Supabase Python SDK does NOT support raw DDL execution (CREATE TABLE) directly via the client unless an RPC function exists.")
print("Checking for 'exec_sql' RPC function...")

try:
    response = supabase.rpc("exec_sql", {"sql_query": sql}).execute()
    print("Execution successful via exec_sql RPC!")
    print(response)
except Exception as e:
    print(f"RPC execution failed: {e}")
    print("\n[FALLBACK REQUIRED]: Please copy the content of 'app/db/migrations/002_ironclad_auth.sql' and run it in the Supabase Dashboard SQL Editor.")
