import os
import psycopg2
from dotenv import load_dotenv

# Load .env explicitly
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

def get_db_url():
    # Try direct URL first
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    
    # Construct from Supabase env vars (Service Role)
    supa_url = os.getenv("SUPABASE_URL")
    # We need the DB password for DDL, checking various keys
    password = os.getenv("APP_PASSWORD") or os.getenv("SUPABASE_DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD")
    
    if supa_url and password:
        # Extract project ref from URL (https://<ref>.supabase.co)
        project_ref = supa_url.replace("https://", "").replace(".supabase.co", "")
        return f"postgres://postgres:{password}@db.{project_ref}.supabase.co:5432/postgres"
    
    return None

def apply_migration(sql_file_path):
    url = get_db_url()
    if not url:
        print("ERROR: Could not construct DATABASE_URL. Missing APP_PASSWORD or SUPABASE_URL?")
        return

    print(f"Applying migration: {sql_file_path}...")
    try:
        conn = psycopg2.connect(url)
        conn.autocommit = True
        cursor = conn.cursor()
        
        with open(sql_file_path, 'r') as f:
            sql = f.read()
            cursor.execute(sql)
            
        print("SUCCESS: Migration applied.")
        conn.close()
    except Exception as e:
        print(f"FAILED: {e}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        apply_migration(sys.argv[1])
    else:
        # Default to 008
        mig_path = os.path.join(os.path.dirname(__file__), '..', 'app', 'db', 'migrations', '008_create_candidates.sql')
        apply_migration(mig_path)
