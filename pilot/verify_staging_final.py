import os
import sys
import json
from supabase import create_client, Client
from dotenv import load_dotenv

# TASK: LENS TEST (FINAL STRICT)
# GOAL: Verify Integrity. Fail Fast on any error.
# CHECKS: JSONB Type, Exact Field Match, Cost=1, Run ID.

def verify_staging_final():
    print(">>> INITIATING STRICT LENS TEST (FINAL)")
    
    BASE_PATH = "/Users/josephlf/.gemini/antigravity/scratch"
    load_dotenv(os.path.join(BASE_PATH, ".env"))
    
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY") or os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_KEY")
    
    if not url or not key:
        print("   [CRITICAL] Missing Supabase Credentials.")
        sys.exit(1)

    supabase = create_client(url, key)
    
    # 1. Fetch Rows (Optimized Select)
    print("... Fetching 'PENDING_ENRICHMENT' rows ...")
    response = supabase.table("scout_drafts")\
        .select("company, lives, status, draft_body")\
        .eq("status", "PENDING_ENRICHMENT")\
        .order("lives", desc=True)\
        .execute()
        
    rows = response.data
    
    if not rows:
        print("   [FAIL] No pending rows found. Staging empty.")
        sys.exit(1)

    print(f"   [INFO] Found {len(rows)} Staged Targets.")
    print("   Top 3 Targets (Whale Check):")
    for i, r in enumerate(rows[:3]):
        print(f"    {i+1}. {r.get('company')} ({r.get('lives')} lives)")
    
    # 2. Inspect the Top Whale
    target = rows[0]
    print(f"\n   INSPECTING: {target.get('company')}")
    
    # 3. VERIFY THE CONTRACT (STRICT)
    body = target.get('draft_body')
    APPROVED_FIELDS = [
        "decision_maker_name", 
        "decision_maker_title", 
        "decision_maker_email_verified", 
        "decision_maker_linkedin_url"
    ]
    
    print("\n   --- CONTRACT VALIDATION ---")
    
    # Check 1: Is it a Dict?
    if not isinstance(body, dict):
        print(f"   [FAIL] draft_body is not a JSON Object. Type: {type(body)}")
        sys.exit(1)

    # Check 2: Run ID
    if not body.get('run_id'):
        print("   [FAIL] Missing 'run_id'.")
        sys.exit(1)
    else:
        print(f"   [PASS] Run ID: {body.get('run_id')}")

    # Check 3: Cost
    cost = body.get('expected_credit_cost')
    if cost != 1:
        print(f"   [FAIL] Invalid Cost. Expected 1, got {cost}.")
        sys.exit(1)
    else:
        print("   [PASS] Expected Cost: 1 Credit")

    # Check 4: Allowed Fields (Strict Set Match)
    current_fields = body.get('allowed_fields', [])
    
    if set(current_fields) == set(APPROVED_FIELDS) and len(current_fields) == len(APPROVED_FIELDS):
        print(f"   [PASS] Allowed Fields Match Exact Schema.")
        print(f"          {current_fields}")
    else:
        print("   [FAIL] Schema Mismatch!")
        print(f"          Expected: {APPROVED_FIELDS}")
        print(f"          Got:      {current_fields}")
        sys.exit(1)

    print("\n   >>> LENS TEST PASSED: READY FOR CLAY WORKER <<<")

if __name__ == "__main__":
    verify_staging_final()
