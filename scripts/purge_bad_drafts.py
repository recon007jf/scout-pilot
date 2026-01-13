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
    "[Your Title]"
]

print("Scanning for corrupted drafts...")

# Scan drafts
# We can't do a single OR query easily with text_search for all phrases in Supabase-py easily without raw SQL
# So we fetch all 'ready' drafts and scan in python (assuming dataset is manageable for now, or chunk it)
# Or we use multiple queries.

# Fetch all ready drafts
res = db.table("drafts").select("*").eq("status", "ready").execute()
drafts = res.data
count = 0
purged = 0

for d in drafts:
    body = d.get("email_body", "") or ""
    dossier_id = d.get("dossier_id")
    
    violation = None
    for phrase in BANNED_PHRASES:
        if phrase.lower() in body.lower():
            violation = phrase
            break
    
    if violation:
        print(f"[PURGE] Dossier {dossier_id}: Found '{violation}'")
        # Delete from drafts
        db.table("drafts").delete().eq("id", d["id"]).execute()
        # Reset Dossier status to idle so frontend knows to re-request
        db.table("dossiers").update({
            "llm_email_body": None, 
            "llm_email_subject": None,
            "llm_draft_status": "idle"
        }).eq("id", dossier_id).execute()
        purged += 1
    count += 1

print(f"Scan Complete. Scanned: {count}. Purged: {purged}.")
