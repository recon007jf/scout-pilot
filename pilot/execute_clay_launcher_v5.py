import os
import sys
import json
import uuid
import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

# TASK: CLAY LAUNCHER V5 (RPC ATOMIC LOCKING - SIMULATION)
# GOAL: Call DB Function -> Validation -> Simulation Dump -> Clean Revert.
# REQUIRES: public.claim_enrichment_batch RPC function.

def execute_clay_launcher_v5():
    print(">>> INITIATING CLAY LAUNCHER V5 (RPC ATOMIC)")
    
    BASE_PATH = "/Users/josephlf/.gemini/antigravity/scratch"
    load_dotenv(os.path.join(BASE_PATH, ".env"))
    
    url = os.getenv("SUPABASE_URL")
    # Must use Service Role Key to bypass RLS/Security Definer checks
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_KEY")
    
    if not url or not key:
        print("   [CRITICAL] Missing Supabase Credentials.")
        sys.exit(1)

    supabase = create_client(url, key)
    
    # 1. GENERATE LOCK TOKEN
    run_token = f"sim_{uuid.uuid4().hex[:8]}"
    print(f"   Lock Token: {run_token}")

    # 2. CALL ATOMIC DB FUNCTION (RPC)
    print("... Requesting batch claim via RPC ...")
    
    try:
        rpc_res = supabase.rpc(
            'claim_enrichment_batch', 
            {
                'p_lock_id': run_token,
                'p_locked_by': 'clay_launcher_v5',
                'p_limit': 10
            }
        ).execute()
        
        # Handle cases where data might be None or empty
        locked_rows = rpc_res.data if rpc_res.data else []
        
    except Exception as e:
        print(f"   [ERROR] RPC Call Failed. Check permissions? Error: {e}")
        return

    if not locked_rows:
        print("   [INFO] No pending rows available to claim.")
        return

    print(f"   [SUCCESS] Atomically Claimed {len(locked_rows)} rows (Whales First).")

    # 3. VALIDATION & PAYLOAD BUILD
    valid_payloads = []
    total_cost = 0
    ids_to_fail = []
    
    for row in locked_rows:
        contract = row.get('draft_body')
        
        # Safe Contract Check
        if not isinstance(contract, dict) or not contract.get('allowed_fields'):
            print(f"   [WARN] Row {row.get('company')} invalid/missing contract.")
            ids_to_fail.append(row['id'])
            continue

        # Cost Check (Int Safety)
        try:
            cost = int(contract.get('expected_credit_cost', 1))
        except (ValueError, TypeError):
            cost = 1
            
        total_cost += cost
        
        # Build Payload
        item = {
            "record_id": row['id'],
            "company": row['company'],
            "enrichment_config": {
                "fields": contract['allowed_fields'],
                "provider": "Clay"
            },
            # Whitelisted Context
            "context": {
                "broker": row.get('broker_2021'),
                "lives": row.get('lives'),
                "dol_renewal": contract.get('dol_data', {}).get('renewal_date')
            }
        }
        valid_payloads.append(item)

    # 4. CAP CHECK
    print(f"   [AUDIT] Payload Count: {len(valid_payloads)} | Est Cost: {total_cost}")
    
    if total_cost > 10 or len(valid_payloads) > 10:
        print(f"   [FAIL] Cap Exceeded. Aborting.")
        # Revert happens automatically below
    else:
        # 5. DUMP JSON (SIMULATION)
        final_payload = {
            "batch_id": f"BATCH_{run_token}",
            "records": valid_payloads
        }
        
        outfile = os.path.join(BASE_PATH, "clay_payload_sim.json")
        with open(outfile, "w") as f:
            json.dump(final_payload, f, indent=2)
            
        print(f"   [SUCCESS] JSON generated: {os.path.basename(outfile)}")

    # 6. AUTO-ROLLBACK (CLEANUP)
    print("... Reverting Locks (Simulation Cleanup) ...")
    
    # Unlock rows owned by this token
    revert_res = supabase.table("scout_drafts")\
        .update({
            "status": "PENDING_ENRICHMENT",
            "lock_id": None, # Use explicit None (JSON null)
            "locked_at": None,
            "locked_by": None,
            "last_error": None 
        })\
        .eq("lock_id", run_token)\
        .eq("status", "ENRICHMENT_IN_PROGRESS")\
        .execute()
        
    print(f"   [CLEANUP] Unlocked {len(revert_res.data)} rows.")
    print("-" * 30)
    print("ACTION: Inspect 'clay_payload_sim.json'.")
    print("-" * 30)

if __name__ == "__main__":
    execute_clay_launcher_v5()
