import os
import sys
from supabase import create_client
from dotenv import load_dotenv

sys.path.append(os.getcwd())
load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
db = create_client(url, key)

BANNED_PHRASES = [
    "I hope this message finds you well",
    "Scout AI",
    "Pacific AI Systems",
    "[Your Name]",
    "[Your Title]",
    "Your Contact Information"
]

print("Scanning DOSSIERS table for corrupted legacy content...")

# Fetch all dossiers with generated emails
res = db.table("dossiers").select("id, full_name, llm_email_body").neq("llm_email_body", "null").execute()
dossiers = res.data
count = 0
purged = 0

for d in dossiers:
    body = d.get("llm_email_body", "") or ""
    dossier_id = d.get("id")
    
    violation = None
    for phrase in BANNED_PHRASES:
        if phrase.lower() in body.lower():
            violation = phrase
            break
    
    if violation:
        print(f"[PURGE] Dossier {dossier_id} ({d.get('full_name')}): Found '{violation}'")
        
        # 1. Nullify in dossiers
        db.table("dossiers").update({
            "llm_email_body": None, 
            "llm_email_subject": None,
            "llm_draft_status": "idle"
        }).eq("id", dossier_id).execute()
        
        # 2. Also delete from drafts to be safe
        db.table("drafts").delete().eq("dossier_id", dossier_id).execute()
        
        purged += 1
    count += 1

print(f"Dossier Scan Complete. Scanned: {count}. Purged: {purged}.")
