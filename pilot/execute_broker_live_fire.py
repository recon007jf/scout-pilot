import os
import sys
import json
import uuid
import datetime
import requests
from supabase import create_client
from dotenv import load_dotenv

# TASK: BROKER LIVE FIRE (LAUNCHER)
# GOAL: Atomic claim -> POST -> SENT_TO_CLAY
# TABLE: target_brokers
# RPC: claim_broker_batch

MAX_ROWS = 10

def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def execute_broker_live_fire():
    print(">>> INITIATING BROKER LIVE FIRE (LAUNCHER)")

    BASE_PATH = "/Users/josephlf/.gemini/antigravity/scratch"
    load_dotenv(os.path.join(BASE_PATH, ".env"))

    url = os.getenv("SUPABASE_URL")
    # Try multiple keys as fallback
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_KEY")
    webhook_url = os.getenv("CLAY_WEBHOOK_URL")

    if not url or not key:
        print("   [CRITICAL] Missing Supabase credentials.")
        sys.exit(1)
    if not webhook_url:
        print("   [CRITICAL] Missing CLAY_WEBHOOK_URL in .env.")
        sys.exit(1)

    supabase = create_client(url, key)

    run_token = f"broker_live_{uuid.uuid4().hex[:8]}"
    print(f"   Run Token: {run_token}")

    # 1. ATOMIC CLAIM (RPC)
    print("... Claiming brokers via RPC ...")
    try:
        rpc_res = supabase.rpc(
            "claim_broker_batch",
            {"p_lock_id": run_token, "p_locked_by": "broker_live_fire", "p_limit": MAX_ROWS},
        ).execute()
        locked_rows = rpc_res.data if rpc_res.data else []
    except Exception as e:
        print(f"   [ERROR] RPC failed: {e}")
        return

    if not locked_rows:
        print("   [INFO] No READY_TO_PROCESS brokers found.")
        return

    print(f"   [LOCKED] Claimed {len(locked_rows)} rows.")

    # 2. PAYLOAD BUILD
    records = []
    # No strict validation against 'contract' needed here as these are raw seeds
    
    for r in locked_rows:
        records.append({
            "record_id": r["id"],
            "full_name": r["full_name"],
            "firm": r["firm"],
            "role": r.get("role"),
            "region": r.get("region"),
            "tier": r.get("tier"),
            "context": {
                "source": "broker_pivot_seed",
                "run_token": run_token
            }
        })

    final_payload = {"batch_id": f"BATCH_{run_token}", "records": records}

    # 3. FIRE (HTTP POST)
    print(f"   ... POSTing {len(records)} records to Clay webhook ...")
    try:
        resp = requests.post(webhook_url, json=final_payload, timeout=20)
        code = resp.status_code
        body_snip = (resp.text or "")[:500]

        if not (200 <= code < 300):
            raise RuntimeError(f"http_{code}: {body_snip}")

        print(f"   [SUCCESS] Clay accepted batch (HTTP {code}).")

        # 4. SUCCESS: Move to SENT_TO_CLAY & Clear Locks
        row_ids = [r["id"] for r in locked_rows]
        
        supabase.table("target_brokers").update({
            "status": "SENT_TO_CLAY",
            "clay_status": f"transmitted_http_{code}",
            "lock_id": None,
            "locked_at": None,
            "locked_by": None,
        }).in_("id", row_ids).execute()

        print(f"   [COMPLETE] Batch sent. Rows moved to SENT_TO_CLAY.")

    except Exception as e:
        err = str(e)
        print(f"   [ERROR] Transmission failed: {err}")

        # FAIL safe
        supabase.table("target_brokers").update({
            "status": "ENRICHMENT_FAILED",
            "last_error": f"transport_error: {err}",
            "lock_id": None,
            "locked_at": None,
            "locked_by": None,
        }).eq("lock_id", run_token).eq("status", "IN_PROGRESS").execute()

if __name__ == "__main__":
    execute_broker_live_fire()
