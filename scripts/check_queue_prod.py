
import os
import sys
from supabase import create_client

# Load Env
from dotenv import load_dotenv
load_dotenv(".env")

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

if not url or not key:
    print("Missing Supabase Credentials")
    sys.exit(1)

supabase = create_client(url, key)

print("--- DIAGNOSING PRODUCTION QUEUE ---")

# 1. Total Active Candidates (Raw Count)
try:
    res = supabase.table("target_brokers")\
        .select("id, status, full_name, work_email, linkedin_url, firm, role, llm_email_subject")\
        .eq("status", "ENRICHED")\
        .execute()
    
    candidates = res.data
    print(f"Inspecting {len(candidates)} candidates for Hard Gate violations...")
    
    violation_counts = {
        "missing_identity": 0,
        "missing_context": 0,
        "missing_draft": 0
    }
    
    for c in candidates:
        identity_missing = []
        if not c.get("full_name"): identity_missing.append("name")
        if not c.get("work_email"): identity_missing.append("email")
        if not c.get("linkedin_url"): identity_missing.append("linkedin")
        
        context_missing = []
        if not c.get("firm"): context_missing.append("company")
        if not c.get("role"): context_missing.append("title")
        
        draft_missing = []
        if not c.get("llm_email_subject"): draft_missing.append("subject")
        
        if identity_missing:
            violation_counts["missing_identity"] += 1
            if violation_counts["missing_identity"] <= 3:
                 print(f" - {c['id']} Missing Identity: {identity_missing}")
        
        if context_missing:
             violation_counts["missing_context"] += 1
             if violation_counts["missing_context"] <= 3:
                 print(f" - {c['id']} Missing Context: {context_missing}")

    print("\nSummary of Violations:")
    print(violation_counts)

except Exception as e:
    print(f"Error: {e}")
