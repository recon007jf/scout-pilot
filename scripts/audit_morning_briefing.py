import os
import json
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
client = create_client(url, key)

def check_contract(target):
    # P0 Checks
    p0_missing = []
    
    # 1. Identity
    if not target.get("full_name"): p0_missing.append("Identity: Name")
    if not target.get("work_email"): p0_missing.append("Identity: Email")
    if not target.get("linkedin_url"): p0_missing.append("Identity: LinkedIn")
    
    # 2. Professional Context
    if not target.get("firm"): p0_missing.append("Context: Company")
    if not target.get("role"): p0_missing.append("Context: Title")
    
    # 3. Location
    if not target.get("region"): p0_missing.append("Location: Region")
    
    # 4. Funding (Approximation: tier=Tier 1 implies funding, but really need 5500 data)
    # The user said "Stop-Loss Indicator" or "Self-Funded Flag". 
    # Current DB schema might not have this explicit flag visible here, 
    # but we will check if 'tier' is set as a proxy for now, 
    # or look for a 'funding_model' column if it exists (it might not yet).
    # Let's check 'tier' as P0 for now.
    if not target.get("tier"): p0_missing.append("Funding: Tier/Signal")

    # 6. Draft Readiness
    if not target.get("llm_email_subject") or not target.get("llm_email_body"):
        p0_missing.append("Draft: Subject/Body")

    return p0_missing

def main():
    print("📋 Starting Morning Briefing Contract Audit...\n")
    
    # 1. Check Table Counts
    tables = ["target_brokers", "scout_drafts", "raw_leads"] 
    # (Just guessing 'raw_leads' exists, usually it's 'leads_dump' or similar)
    
    # Check target_brokers
    res = client.table("target_brokers").select("*").execute()
    targets = res.data
    
    eligible_count = 0
    ineligible_count = 0
    
    print(f"--- Table: target_brokers ({len(targets)} rows) ---")
    
    for t in targets:
        missing = check_contract(t)
        status = "✅ READY" if not missing else "❌ BLOCKED"
        if not missing: eligible_count += 1
        else: ineligible_count += 1
        
        print(f"[{status}] {t['full_name']} ({t['firm']})")
        if missing:
            print(f"   Missing: {', '.join(missing)}")
            
    print(f"\nSummary:")
    print(f"   ✅ Eligible: {eligible_count}")
    print(f"   ❌ Ineligible: {ineligible_count}")
    
    if ineligible_count > 0:
        print("\n⚠️  ACTION REQUIRED: Run Enrichment to unblock candidates.")
    else:
        print("\n✨ All Visible Candidates are Compliant.")

if __name__ == "__main__":
    main()
