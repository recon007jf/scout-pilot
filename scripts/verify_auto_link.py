
# scripts/verify_auto_link.py
from supabase import create_client, ClientOptions
from app.config import settings
from app.core.identity_bridge import IdentityBridge
import uuid
import time

# Use Service Role to simulate backend
url = settings.SUPABASE_URL
key = settings.SUPABASE_SERVICE_ROLE_KEY
db = create_client(url, key, options=ClientOptions(flow_type="implicit"))

bridge = IdentityBridge(db)

# Test Data
TEST_CLERK_ID = f"user_test_{int(time.time())}"
TEST_EMAIL = f"test_auto_{int(time.time())}@example.com"
TEST_CLAIMS = {
    "sub": TEST_CLERK_ID,
    "email": TEST_EMAIL,
    "email_verified": True
}

print("--- TEST: AUTO-LINKING ---")
print(f"Clerk ID: {TEST_CLERK_ID}")
print(f"Email: {TEST_EMAIL}")

try:
    # 1. Resolve (Should trigger auto-link)
    result = bridge.resolve_user(TEST_CLAIMS)
    print("Bridge Result:", result)
    
    assert result["email"] == TEST_EMAIL
    assert result["is_new_link"] == True
    internal_id = result["user_id"]
    print(f"PASS: Auto-linked to Internal ID: {internal_id}")
    
    # 2. Verify DB Persistence
    # Query user_identities
    resp = db.table("user_identities").select("*").eq("clerk_user_id", TEST_CLERK_ID).execute()
    data = resp.data
    assert len(data) == 1
    assert data[0]["internal_user_id"] == internal_id
    print("PASS: DB Row Confirmed")
    
    # 3. Idempotency Check
    # Call again
    result2 = bridge.resolve_user(TEST_CLAIMS)
    assert result2["user_id"] == internal_id
    assert result2["is_new_link"] == False
    print("PASS: Idempotency Confirmed (Existing link reused)")
    
    # 4. Security Check (Unverified)
    unver_claims = TEST_CLAIMS.copy()
    unver_claims["email_verified"] = False
    unver_claims["sub"] = TEST_CLERK_ID + "_hacker"
    unver_claims["email"] = "hacker@example.com"
    
    try:
        bridge.resolve_user(unver_claims)
        print("FAIL: Security Check failed (Allowed unverified email)")
    except ValueError as e:
        print(f"PASS: Security Check Success (Rejected unverified: {e})")

except Exception as e:
    print(f"FAIL: {e}")
