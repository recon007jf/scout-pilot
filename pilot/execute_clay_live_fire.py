import os
import sys
import json
import uuid
import datetime
import requests
from supabase import create_client
from dotenv import load_dotenv

# TASK: CLAY LIVE FIRE (LAUNCHER) - FINAL
# GOAL: Atomic claim -> Strict Validation -> Cap Check -> POST -> SENT_TO_CLAY
# FIXES: Correct .in_ usage, Safe Reverts, Full Lock Cleanup.

APPROVED_FIELDS = [
    "decision_maker_name",
    "decision_maker_title",
    "decision_maker_email_verified",
    "decision_maker_linkedin_url",
]

MAX_ROWS = 10
MAX_CREDITS = 10

def utc_now():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def execute_clay_live_fire():
    print(">>> INITIATING CLAY LIVE FIRE (LAUNCHER)")

    BASE_PATH = "/Users/josephlf/.gemini/antigravity/scratch"
    load_dotenv(os.path.join(BASE_PATH, ".env"))

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_KEY")
    webhook_url = os.getenv("CLAY_WEBHOOK_URL")

    if not url or not key:
        print("   [CRITICAL] Missing Supabase credentials.")
        sys.exit(1)
    if not webhook_url:
        print("   [CRITICAL] Missing CLAY_WEBHOOK_URL in .env.")
        sys.exit(1)

    supabase = create_client(url, key)

    run_token = f"live_{uuid.uuid4().hex[:8]}"
    sent_at = utc_now()
    print(f"   Run Token: {run_token}")

    # 1. ATOMIC CLAIM (RPC)
    print("... Claiming targets via RPC ...")
    try:
        rpc_res = supabase.rpc(
            "claim_enrichment_batch",
            {"p_lock_id": run_token, "p_locked_by": "clay_live_fire", "p_limit": MAX_ROWS},
        ).execute()
        locked_rows = rpc_res.data if rpc_res.data else []
    except Exception as e:
        print(f"   [ERROR] RPC failed: {e}")
        return

    if not locked_rows:
        print("   [INFO] No PENDING_ENRICHMENT targets found.")
        return

    print(f"   [LOCKED] Claimed {len(locked_rows)} rows.")

    # 2. VALIDATION, CAP, PAYLOAD BUILD
    valid_rows = []
    invalid_ids = []
    total_cost = 0

    for row in locked_rows:
        rid = row.get("id")
        company = row.get("company")
        contract = row.get("draft_body") or {}

        # STRICT VALIDATION
        if not isinstance(contract, dict):
            invalid_ids.append(rid)
            continue

        allowed = contract.get("allowed_fields") or []
        if set(allowed) != set(APPROVED_FIELDS) or len(allowed) != len(APPROVED_FIELDS):
            invalid_ids.append(rid)
            continue

        cost_raw = contract.get("expected_credit_cost", 1)
        try:
            cost = int(cost_raw)
        except (TypeError, ValueError):
            cost = 1

        total_cost += cost

        valid_rows.append({
            "id": rid,
            "company": company,
            "lives": row.get("lives"),
            "broker_2021": row.get("broker_2021"),
            "contract": contract,
            "cost": cost,
        })

    # A. HANDLE INVALID ROWS (Fail Fast)
    if invalid_ids:
        print(f"   [WARN] {len(invalid_ids)} rows failed validation. Marking ENRICHMENT_FAILED.")
        supabase.table("scout_drafts").update({
            "status": "ENRICHMENT_FAILED",
            "last_error": "validation_error: invalid contract or schema mismatch",
            "lock_id": None,
            "locked_at": None,
            "locked_by": None,
        }).in_("id", invalid_ids).execute()

    # B. HANDLE EMPTY BATCH (Safe Revert)
    if not valid_rows:
        print("   [STOP] No valid rows remain. Releasing locks on remaining items.")
        supabase.table("scout_drafts").update({
            "status": "PENDING_ENRICHMENT",
            "lock_id": None,
            "locked_at": None,
            "locked_by": None,
        }).eq("lock_id", run_token).eq("status", "ENRICHMENT_IN_PROGRESS").execute()
        return

    # C. CAP CHECK
    if total_cost > MAX_CREDITS:
        print(f"   [ABORT] Credit cap exceeded: {total_cost} > {MAX_CREDITS}. Reverting claimed rows.")
        supabase.table("scout_drafts").update({
            "status": "PENDING_ENRICHMENT",
            "last_error": f"credit_cap_exceeded: {total_cost} > {MAX_CREDITS}",
            "lock_id": None,
            "locked_at": None,
            "locked_by": None,
        }).eq("lock_id", run_token).eq("status", "ENRICHMENT_IN_PROGRESS").execute()
        return

    # 3. BUILD & AUDIT
    records = []
    for r in valid_rows:
        records.append({
            "record_id": r["id"],
            "company": r["company"],
            "enrichment_config": {"fields": APPROVED_FIELDS, "provider": "Clay"},
            "context": {
                "broker": r.get("broker_2021"),
                "lives": r.get("lives"),
                "dol_renewal": (r["contract"].get("dol_data") or {}).get("renewal_date"),
            },
        })

    final_payload = {"batch_id": f"BATCH_{run_token}", "records": records}

    print("... Writing outbound audit to DB ...")
    for r in valid_rows:
        body = r["contract"]
        audit = body.get("clay_audit") or []
        audit.append({
            "action": "outbound_request_preflight",
            "timestamp": sent_at,
            "run_token": run_token,
            "expected_credit_cost": r["cost"],
            "payload_record": next(x for x in records if x["record_id"] == r["id"]),
        })
        body["clay_audit"] = audit

        # Auditing: Just update body, keep status IN_PROGRESS for now
        supabase.table("scout_drafts").update({
            "draft_body": body,
        }).eq("id", r["id"]).execute()

    # 4. FIRE (HTTP POST)
    print(f"   ... POSTing {len(records)} records to Clay webhook ...")
    try:
        resp = requests.post(webhook_url, json=final_payload, timeout=20)
        code = resp.status_code
        body_snip = (resp.text or "")[:500]

        if not (200 <= code < 300):
            raise RuntimeError(f"http_{code}: {body_snip}")

        print(f"   [SUCCESS] Clay accepted batch (HTTP {code}).")

        # 5. SUCCESS: Move to SENT_TO_CLAY & Clear Locks
        for r in valid_rows:
            body = r["contract"]
            audit = body.get("clay_audit") or []
            audit.append({
                "action": "outbound_request_accepted",
                "timestamp": utc_now(),
                "run_token": run_token,
                "http_code": code,
                "response_snippet": body_snip,
            })
            body["clay_audit"] = audit

            supabase.table("scout_drafts").update({
                "status": "SENT_TO_CLAY",
                "draft_body": body,
                "last_error": None,
                "lock_id": None,
                "locked_at": None,
                "locked_by": None,
            }).eq("id", r["id"]).execute()

        print(f"   [COMPLETE] Batch sent. Rows moved to SENT_TO_CLAY.")

    except Exception as e:
        err = str(e)
        print(f"   [ERROR] Transmission failed: {err}")

        # FAIL ONLY rows owned by this run_token that are still in progress
        supabase.table("scout_drafts").update({
            "status": "ENRICHMENT_FAILED",
            "last_error": f"transport_error: {err}",
            "lock_id": None,
            "locked_at": None,
            "locked_by": None,
        }).eq("lock_id", run_token).eq("status", "ENRICHMENT_IN_PROGRESS").execute()

if __name__ == "__main__":
    execute_clay_live_fire()
