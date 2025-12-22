from modules.db_client import DBClient
import os

def test_db_insert():
    print("Testing DB Adapter (Supabase/SQLite)...")
    
    # 1. Init
    db = DBClient()
    print(f"Mode: {db.mode}")
    
    # 2. Insert Mock Lead
    mock_lead = {
        'employer_name': 'Test Pilot Employer',
        'broker_firm': 'Test Broker Firm',
        'broker_human_name': 'Neil Test',
        'broker_email': 'neil@test.com',
        'state': 'CA',
        'lives_count': 500,
        'verification_status': 'VERIFIED',
        'psych_profile_json': '{"psych_profile": "Visionary", "Hook": "Innovation"}',
        'draft_email_text': 'Neil, we beat UMR.',
        'andrew_feedback_score': None
    }
    
    success, msg = db.insert_lead(mock_lead)
    
    if success:
        print("✅ INSERT SUCCESS")
    else:
        print(f"❌ INSERT FAILED: {msg}")
        
    # 3. Read Back
    leads = db.fetch_pilot_leads()
    print(f"Fetched {len(leads)} leads.")
    
    found = any(l['employer_name'] == 'Test Pilot Employer' for l in leads)
    
    if found:
        print("✅ READ SUCCESS: Found inserted lead.")
    else:
        print("❌ READ FAILED: Inserted lead not found.")

if __name__ == "__main__":
    test_db_insert()
