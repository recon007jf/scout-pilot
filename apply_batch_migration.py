
import os
import psycopg2
from dotenv import load_dotenv

# Load .env explicitly to ensure we get params
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

print("Applying Migration...")

supa_url = os.getenv("SUPABASE_URL")
password = os.getenv("APP_PASSWORD") or os.getenv("SUPABASE_DB_PASSWORD")

if not supa_url or not password:
    print("Error: Missing SUPABASE_URL or APP_PASSWORD in .env")
    exit(1)

project_ref = supa_url.replace("https://", "").replace(".supabase.co", "")
# Use Direct Port 5432 for DDL
db_url = f"postgres://postgres:{password}@db.{project_ref}.supabase.co:5432/postgres"

sql = """
ALTER TABLE target_brokers ADD COLUMN IF NOT EXISTS selected_for_date DATE;
ALTER TABLE target_brokers ADD COLUMN IF NOT EXISTS batch_number INTEGER;
CREATE INDEX IF NOT EXISTS idx_tb_date_batch ON target_brokers(selected_for_date, batch_number);
"""

try:
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql)
    print("Migration Success!")
    conn.close()
except Exception as e:
    print(f"Migration Failed: {e}")
