
import os
import psycopg2
from urllib.parse import urlparse

# Force Supabase URL/Key to be loaded
url = os.environ.get("SUPABASE_URL")
if not url:
    print("No URL")
    exit(1)

# Hack: Construct postgres connection string for Supabase (Pooler: 6543)
# We need the DB Password. Assuming it's in APP_PASSWORD or known.
db_pass = os.environ.get("APP_PASSWORD") or os.environ.get("SUPABASE_DB_PASSWORD")
if not db_pass:
    print("No DB Password found.")
    exit(1)

# Parse standard URL to get host/user/db
# e.g. https://ojzq...supabase.co
project_ref = url.split("https://")[1].split(".")[0]
db_host = f"db.{project_ref}.supabase.co"
db_user = "postgres"
db_name = "postgres"
db_port = 6543

conn_str = f"postgresql://{db_user}.{project_ref}:{db_pass}@{db_host}:{db_port}/{db_name}?sslmode=require"
print(f"Connecting to {db_host}...")

try:
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    
    with open("app/db/migrations/013_enable_rls.sql", "r") as f:
        sql = f.read()
        cur.execute(sql)
        conn.commit()
    print("RLS Enabled.")
except Exception as e:
    print(f"Error: {e}")
