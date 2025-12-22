import pandas as pd
import os
import random
import json
from supabase import create_client, Client
from dotenv import load_dotenv

# TASK: AGENT HEMINGWAY (DEEP AUDIT EDITION)
# GOAL: 
# 1. Generate Drafts in "Andrew's Voice".
# 2. Output a 'Deep Audit CSV' with ALL columns so Andrew can validate 2021 vs 2025 reality.
# 3. Populate the Web UI with clean drafts.

class AgentHemingway:
    def __init__(self, voice_file_path):
        # LOAD THE VOICE BOX
        if not os.path.exists(voice_file_path):
            raise FileNotFoundError(f"Voice Profile not found at: {voice_file_path}")
            
        with open(voice_file_path, 'r') as f:
            data = json.load(f)
            
        # Select Active Mode
        mode_name = data.get("current_mode", "strategic_peer")
        self.voice = data["modes"][mode_name]
        
        print(f"   [VOICE LOADED] Mode: '{mode_name}'")

    def get_role_context(self, title):
        t = str(title).upper()
        if any(x in t for x in ["CFO", "FINANCE", "TREASURER"]):
            return "From a P&L perspective, this is about recapturing capital that doesn't need to leave the building."
        elif any(x in t for x in ["HR", "PEOPLE", "CHRO"]):
            return "This is about upgrading the member experience without asking Finance for more budget."
        return "It's about getting more leverage out of your current spend."

    def write_draft(self, row):
        # Extract Data
        first_name = str(row.get('Contact Full Name', '')).split()[0]
        if pd.isna(first_name) or first_name.lower() == 'nan': first_name = "there"
        
        company = str(row.get('sponsor_name', 'This Company')).title()
        lives = int(row.get('lives', 0))
        broker = str(row.get('broker_2021', 'Current Broker'))
        title = str(row.get('Contact Job Title', ''))
        state = str(row.get('broker_state', 'your region'))
        
        # 1. SELECT COMPONENTS
        opener = random.choice(self.voice["openers"]).format(company=company, lives=lives, state=state, broker=broker)
        value_prop = random.choice(self.voice["value_props"])
        sign_off = self.voice.get("sign_off", "Best,\nAndrew")
        
        # 2. SELECT JAB
        jab = self.voice["jabs"].get("The Incumbent", "")
        # Fuzzy match fix
        u_broker = broker.upper()
        for k, v in self.voice["jabs"].items():
            if k in u_broker:
                jab = v
                break
        
        role_pitch = self.get_role_context(title)

        # 3. ASSEMBLE
        subject = f"Thoughts on {company}'s plan strategy"
        
        body = f"Hi {first_name},\n\n"
        body += f"{opener}\n\n"
        body += f"It looks like you're currently with {broker}. {jab}\n\n"
        body += f"{role_pitch} {value_prop}\n\n"
        body += "I'm not asking for an RFP right now. Just a 5-minute conversation to compare notes on what we're seeing in the market.\n\n"
        body += "Open to a brief chat next Tuesday?\n\n"
        body += f"{sign_off}"
        
        return subject, body

def run_hemingway_execution():
    print(">>> AGENT HEMINGWAY (DEEP AUDIT RUN) INITIATED")
    
    # PATHS (FIXED FOR SCRATCH)
    BASE_PATH = "/Users/josephlf/.gemini/antigravity/scratch"
    INPUT_FILE = os.path.join(BASE_PATH, "backend/Scout_Data_Artifacts/Leads_Shortlist_Sniper.csv")
    VOICE_FILE = os.path.join(BASE_PATH, "backend/Scout_Data_Artifacts/pilot_inputs/voice_profile.json")
    
    # OUTPUT 1: The Master Audit File (For Andrew's Eyes)
    OUTPUT_CSV = os.path.join(BASE_PATH, "backend/Scout_Data_Artifacts/Drafts_For_Andrew_Review.csv")
    
    # OUTPUT 2: Supabase Credentials
    load_dotenv(os.path.join(BASE_PATH, ".env"))
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    # Using Service Key
    SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")

    if not os.path.exists(INPUT_FILE):
        print("   [ERROR] Shortlist not found. Run Sniper Scope first.")
        return
    
    try:
        hemingway = AgentHemingway(VOICE_FILE)
    except Exception as e:
        print(f"   [ERROR] Voice Init Failed: {e}")
        return

    print("... Loading Targets ...")
    df = pd.read_csv(INPUT_FILE, low_memory=False)
    
    print(f"... Drafting Emails for {len(df)} Targets ...")
    
    csv_payload = []
    db_payload = []
    
    for _, row in df.iterrows():
        subj, body = hemingway.write_draft(row)
        
        # 1. PAYLOAD FOR CSV (MAXIMUM CONTEXT)
        csv_item = {
            "Sponsor Name (Plan)": row.get('sponsor_name'),
            "Incumbent Broker (Data 2021)": row.get('broker_2021'), # CRITICAL CHECK
            "Broker State": row.get('broker_state'),
            "Lives": row.get('lives'),
            "Contact Name": row.get('Contact Full Name'),
            "Contact Title": row.get('Contact Job Title'),
            "Contact Email": row.get('Contact Email'),
            "Draft Subject": subj,
            "Draft Body": body,
            "Psych Hook Used": row.get('psych_hook')
        }
        csv_payload.append(csv_item)
        
        # 2. PAYLOAD FOR DB (UI SCHEMA COMPLIANT)
        db_item = {
            "lead_email": row.get('Contact Email'),
            "lead_name": str(row.get('Contact Full Name')),
            "company": str(row.get('sponsor_name')),
            "draft_subject": subj,
            "draft_body": body,
            "status": "NEEDS_REVIEW",
            "source_file": "Deep_Audit_Run_v1"
        }
        db_payload.append(db_item)

    # SAVE THE AUDIT CSV
    df_out = pd.DataFrame(csv_payload)
    
    # Specific column order for easy reading in Excel
    cols = [
        "Sponsor Name (Plan)", "Lives", "Incumbent Broker (Data 2021)", 
        "Contact Name", "Contact Title", "Contact Email", 
        "Draft Subject", "Draft Body"
    ]
    # Ensure all cols exist before selecting
    final_cols = [c for c in cols if c in df_out.columns]
    
    df_out[final_cols].to_csv(OUTPUT_CSV, index=False)
    print(f"   [CHECKPOINT] Deep Audit CSV saved: {OUTPUT_CSV}")
    print(f"   (Contains {len(df_out)} rows with Broker & Plan details for validation)")
    
    # UPLOAD TO SUPABASE
    print("... Uploading to Supabase (scout_drafts) ...")
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("   [WARN] Missing Supabase Credentials. Skipping Upload.")
    else:
        try:
            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            
            batch_size = 100
            for i in range(0, len(db_payload), batch_size):
                batch = db_payload[i:i+batch_size]
                try:
                    supabase.table("scout_drafts").insert(batch).execute()
                    if i % 200 == 0: print(f"    Uploaded {i}...")
                except Exception as ex:
                    print(f"    [WARN] Batch {i} failed: {ex}")
            
            print(f"   [SUCCESS] Uploaded workflows to Cloud.")
            
        except Exception as e:
            print(f"   [WARN] Database Upload Failed: {e}")

    print("-" * 30)
    print(f"MISSION COMPLETE.")
    print(f"1. SEND FILE TO ANDREW: '{os.path.basename(OUTPUT_CSV)}'")
    print(f"   --> Ask him: 'Check column C (Broker). Is this still accurate?'")
    print("-" * 30)

if __name__ == "__main__":
    run_hemingway_execution()
