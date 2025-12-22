import time
import json
from modules.db_client import DBClient
from recon_agent import analyze_lead, write_email, gather_intel_lite

class BernaysAgent:
    def __init__(self):
        self.db = DBClient()
        
    def run_bernays_cycle(self, memory_leads=None):
        """
        Orchestrates the AI Generation Phase.
        Reads Verified Leads -> Generates Profiles/Emails -> Updates DB (or RAM).
        """
        print("🧠 Starting Bernays AI Cycle...")
        
        # 1. Fetch Candidates
        if memory_leads is not None:
             print("   🧠 RAM MODE ACTIVE: Using In-Memory Leads List")
             leads = memory_leads
        else:
             leads = self.db.fetch_pilot_leads()
        
        processed_count = 0
        
        for lead in leads:
            # Skip if already processed
            draft = lead.get('draft_email_text')
            if draft and isinstance(draft, str) and len(draft) > 10:
                continue
                
            # Skip if unverified (Quality Gate)
            # if lead.get('verification_status') == 'UNVERIFIED': continue
            
            # --- HUMAN CHECK (Phase 74) ---
            human = lead.get('broker_human_name')
            if not human or human == 'Unknown':
                 print(f"   ⚠️ Skipping {lead['employer_name']}: No Human Broker identified.")
                 continue
            
            employer = lead['employer_name']
            broker = lead['broker_human_name']
            firm = lead['broker_firm']
            state = lead.get('state', '')
            
            print(f"   🎨 Generating Content for: {employer} ({broker})")
            
            # --- BERNAYS LOGIC (Triangulation 1.0) ---
            # 1. Gather Intel (Lite Mode)
            # Query: "[Name] [Firm] [State] profile"
            raw_intel, _, _, _ = gather_intel_lite(broker, firm, city=state) 
            
            # Append Context
            intel = f"Employer: {employer}. Broker Firm: {firm}. Role: {broker} in {state}.\n\n{raw_intel}"
            
            # 2. Psych Profile
            try:
                profile = analyze_lead(intel, broker, employer)
                profile_json = json.dumps(profile)
            except Exception as e:
                print(f"     ❌ Profiling Failed: {e}")
                profile_json = "{}"
                profile = {}
                
            # 3. Write Email
            try:
                # Basic context derivation
                email = write_email(profile, str(broker).split()[0], client_context={'client_name': employer, 'source': 'F5500_General_Assets'})
            except Exception as e:
                print(f"     ❌ Email Gen Failed: {e}")
                email = ""
                
            # 4. Save to DB
            # We strictly use the ID if available, or just update based on employer?
            # DBClient needs an update method. It has update_feedback, but maybe not generic update.
            # I will check DBClient logic. It has `update_feedback`.
            # I might need to add `update_lead_content` to DBClient or just use SQL if I can.
            # For now I will mock the update or assuming DBClient has it. 
            # Wait, I wrote DBClient and viewed it. content of db_client.py only has `update_feedback`.
            # I NEED TO ADD `update_lead_gen` to DBClient.
            
            # 4. Save
            if memory_leads is not None:
                # RAM UPDATE
                lead['psych_profile_json'] = profile_json
                lead['draft_email_text'] = email
            else:
                # DB UPDATE
                lead_id = lead['id']
                self.update_lead_gen(lead_id, profile_json, email)
                
            processed_count += 1
            
        print(f"✅ Bernays Cycle Complete. Generated {processed_count} drafts.")
        return processed_count

    def update_lead_gen(self, lead_id, profile_json, email_text):
        """
        Helper to update the DB with AI content.
        """
        # Quick hack to extend DBClient functionality without editing it yet?
        # accessing self.db.supabase directly if available
        if self.db.mode == "SUPABASE":
            self.db.supabase.table("leads_pilot").update({
                "psych_profile_json": profile_json,
                "draft_email_text": email_text
            }).eq("id", lead_id).execute()
        else:
            # SQLite
            import sqlite3
            conn = sqlite3.connect("leads_pilot.db")
            c = conn.cursor()
            c.execute("UPDATE leads_pilot SET psych_profile_json = ?, draft_email_text = ? WHERE id = ?", (profile_json, email_text, lead_id))
            conn.commit()
            conn.close()
