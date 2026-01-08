import os
import sys
# Path setup
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.config import settings
from app.utils.logger import get_logger
import psycopg2

logger = get_logger("schema_migration")

def migrate():
    logger.info("Starting Schema Migration...")
    
    # Read Schema
    schema_path = os.path.join(os.path.dirname(__file__), '..', 'app', 'db', 'schema.sql')
    with open(schema_path, 'r') as f:
        sql = f.read()
        
    try:
        # Supabase provides a Postgres connection string, usually.
        # However, supabase-py is HTTP based. schema.sql is SQL.
        # We need a Direct Postgres Connection to run DDL.
        # settings.SUPABASE_URL is HTTP URL.
        # Usually Supabase provides a connection string env var too, e.g. DATABASE_URL.
        # If we don't have it, we can't run DDL from here easily without psql or the connection string.
        # BUT, the `supabase` python client might expose a way to run raw SQL via RPC if enabled, 
        # or we might need the user to run it via Dashboard.
        
        # Checking constraints: User said "Action: Execute against Production Supabase".
        # If we lack the connection string, we must abort or ask.
        # BUT, the instruction says "Run backend/scripts/seed_targets.py locally...".
        # It doesn't explicitly say "Run SQL script". But Phase 2 says "Action: Execute...".
        
        # Logic: If I can't connect via psycopg2 (missing DSN), I will fail.
        # I'll check if DB_URL or DATABASE_URL exists in env.
        
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            # Try to construct it? postgres://postgres:[PASSWORD]@[HOST]:[PORT]/postgres
            # We don't have the password (DB_PASSWORD). We have the API Key.
            logger.warning("No DATABASE_URL found. Cannot execute DDL directly.")
            print("\n[MANUAL ACTION REQUIRED] Please run `backend/app/db/schema.sql` in your Supabase Dashboard SQL Editor.\n")
            return
            
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Schema Migration Complete.")
        
    except Exception as e:
        logger.error(f"Migration Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    migrate()
