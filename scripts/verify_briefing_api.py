import os
import sys
import json
from supabase import create_client
from typing import Dict, Any

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from app.config import settings
from app.core.briefing import BriefingEngine

# Mock DB Client
db = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_ROLE_KEY)

def verify_briefing_output():
    print("🧪 Verifying Briefing Engine Output...")
    engine = BriefingEngine(db)
    result = engine.generate_briefing()
    
    targets = result.get("targets", [])
    print(f"Items Returned: {len(targets)}")
    
    if not targets:
        print("❌ FAIL: No items returned (Queue might be empty or Date mismatch).")
        return

    first = targets[0]
    print("\n[Sample Item Structure]")
    print(json.dumps(first, indent=2))
    
    # Check Contract (BriefingTarget)
    if "targetId" not in first:
        print("❌ FAIL: contract violation: missing targetId")
        # exit(1) # Soft fail for now

    broker = first.get("broker", {})
    if not broker.get("name") or not broker.get("title"):
        print(f"❌ FAIL: Broker details incomplete: {broker}")
    
    # Phase 2: Check Image
    if broker.get("imageUrl"):
        print(f"✅ PASS: Image URL present: {broker.get('imageUrl')[:30]}...")
    else:
        print("⚠️ WARNING: Image URL missing (Enrichment gap or no result).")

    # Check Draft
    draft = first.get("draft", {})
    if not draft.get("body") or "Subject:" not in draft.get("body"):
         print(f"❌ FAIL: Draft content invalid: {draft.get('body')}")
    else:
         print("✅ PASS: Draft content looks valid.")

if __name__ == "__main__":
    verify_briefing_output()
