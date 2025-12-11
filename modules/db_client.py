import os
import json
import sqlite3
from datetime import datetime
from dotenv import load_dotenv

# Try importing supabase, handled gracefully if missing
try:
    from supabase import create_client, Client
    SUPABASE_INSTALLED = True
except ImportError:
    SUPABASE_INSTALLED = False

load_dotenv()

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LOCAL_DB_FILE = "leads_pilot.db"

class DBClient:
    def __init__(self):
        self.mode = "SQLITE"
        self.supabase: Client = None
        
        # Determine Mode
        if SUPABASE_INSTALLED and SUPABASE_URL and SUPABASE_KEY:
            try:
                self.supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
                self.mode = "SUPABASE"
                print(f"   🔌 Connected to Supabase: {SUPABASE_URL}")
            except Exception as e:
                print(f"   ⚠️ Supabase Connection Failed: {e}. Falling back to SQLite.")
                self.mode = "SQLITE"
        else:
            if not SUPABASE_INSTALLED:
                print("   ℹ️ 'supabase' lib not installed. Using SQLite.")
            elif not SUPABASE_URL:
                 print("   ℹ️ No Supabase Creds found. Using SQLite.")
            self.mode = "SQLITE"
            self._init_sqlite()

    def _init_sqlite(self):
        """Initializes local SQLite DB with the Pilot Schema."""
        conn = sqlite3.connect(LOCAL_DB_FILE)
        c = conn.cursor()
        
        # Logical Schema for Pilot
        c.execute('''
            CREATE TABLE IF NOT EXISTS leads_pilot (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                employer_name TEXT,
                broker_firm TEXT,
                broker_human_name TEXT,
                broker_email TEXT,
                state TEXT,
                lives_count INTEGER,
                assets_amount REAL,
                verification_status TEXT,
                psych_profile_json TEXT,
                draft_email_text TEXT,
                andrew_feedback_score TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()
        print(f"   📂 Local SQLite Ready: {LOCAL_DB_FILE}")

    def insert_lead(self, data):
        """
        Inserts a lead into the DB (Universal Adapter).
        Data dict keys must match schema columns.
        """
        if self.mode == "SUPABASE":
            try:
                # Prepare data for Supabase (remove ID for auto-increment usually, or handle UUID)
                # For Pilot, we just push the dict
                response = self.supabase.table("leads_pilot").insert(data).execute()
                return True, "Supabase Insert OK"
            except Exception as e:
                print(f"❌ Supabase Insert Error: {e}")
                return False, str(e)
                
        else:
            # SQLITE INSERT
            try:
                conn = sqlite3.connect(LOCAL_DB_FILE)
                c = conn.cursor()
                
                # Extract known fields to prevent injection/schema mismatch errors
                # This is a bit manual but safe for MVP
                columns = [
                    'employer_name', 'broker_firm', 'broker_human_name',
                    'broker_email', 'state', 'lives_count', 'assets_amount',
                    'verification_status', 'psych_profile_json', 'draft_email_text',
                    'andrew_feedback_score'
                ]
                
                values = [data.get(col) for col in columns]
                
                # JSON dump needed for dicts in SQLite
                if isinstance(values[7], dict): values[7] = json.dumps(values[7])
                
                placeholders = ",".join(["?" for _ in columns])
                query = f"INSERT INTO leads_pilot ({','.join(columns)}) VALUES ({placeholders})"
                
                c.execute(query, values)
                conn.commit()
                conn.close()
                return True, "SQLite Insert OK"
            except Exception as e:
                print(f"❌ SQLite Insert Error: {e}")
                return False, str(e)

    def store_leads(self, leads_list):
        """
        Bulk helper to store multiple leads.
        Iterates through list and calls insert_lead.
        """
        success_count = 0
        for lead in leads_list:
            ok, _ = self.insert_lead(lead)
            if ok: success_count += 1
        return success_count

    def fetch_pilot_leads(self):
        """Fetches all leads for the Pilot Review UI."""
        if self.mode == "SUPABASE":
            try:
                response = self.supabase.table("leads_pilot").select("*").execute()
                return response.data
            except Exception as e:
                return []
        else:
            try:
                conn = sqlite3.connect(LOCAL_DB_FILE)
                conn.row_factory = sqlite3.Row # Return dict-like rows
                c = conn.cursor()
                c.execute("SELECT * FROM leads_pilot ORDER BY created_at DESC")
                rows = [dict(row) for row in c.fetchall()]
                conn.close()
                return rows
            except:
                return []

    def update_feedback(self, lead_id, feedback):
        """Updates the feedback score (Good/Bad)."""
        if self.mode == "SUPABASE":
            self.supabase.table("leads_pilot").update({"andrew_feedback_score": feedback}).eq("id", lead_id).execute()
        else:
            c.execute("UPDATE leads_pilot SET andrew_feedback_score = ? WHERE id = ?", (feedback, lead_id))
            conn.commit()
            conn.close()

    def clear_leads(self):
        """Wipes the leads_pilot table to start fresh."""
        if self.mode == "SUPABASE":
            # Supabase delete all is tricky, usually requires policy. For MVP assume SQLite or admin role.
            self.supabase.table("leads_pilot").delete().neq("id", 0).execute() 
        else:
            conn = sqlite3.connect(LOCAL_DB_FILE)
            c = conn.cursor()
            c.execute("DELETE FROM leads_pilot")
            conn.commit()
            conn.close()
