
# scripts/verify_legacy_link.py
from supabase import create_client, ClientOptions
from app.config import settings
from app.core.identity_bridge import IdentityBridge
import uuid
import time
import sys

# Use Service Role
url = settings.SUPABASE_URL
key = settings.SUPABASE_SERVICE_ROLE_KEY
db = create_client(url, key, options=ClientOptions(flow_type="implicit"))

bridge = IdentityBridge(db)

TEST_CLERK_ID = f"clerk_legacy_{int(time.time())}"
TEST_EMAIL = f"legacy_user_{int(time.time())}@example.com"
TEST_PASSWORD = "Password123!"

print("--- TEST: EXISTING USER MIGRATION ---")
print(f"Goal: Ensure {TEST_EMAIL} maps to EXISTING Supabase UUID, not new one.")

try:
    # 1. Create Layout User (Simulate functionality)
    # Using Admin API
    print(f"Creating Legacy User in Supabase: {TEST_EMAIL}")
    user_attrs = {"email": TEST_EMAIL, "password": TEST_PASSWORD, "email_confirm": True}
    base_user = db.auth.admin.create_user(user_attrs)
    legacy_uuid = base_user.user.id
    print(f"Legacy UUID Created: {legacy_uuid}")
    
    # 2. Run Bridge
    # Mock Claims
    claims = {
        "sub": TEST_CLERK_ID,
        "email": TEST_EMAIL,
        "email_verified": True
    }
    
    result = bridge.resolve_user(claims)
    print(f"Bridge Resolved: {result['user_id']}")
    
    # 3. Assert
    if result['user_id'] == legacy_uuid:
        print("PASS: Linked to Legacy UUID.")
    else:
        print(f"FAIL: Generated NEW UUID {result['user_id']} instead of using {legacy_uuid}")
        sys.exit(1)

    # 4. Cleanup (optional, but good practice if possible)
    # db.auth.admin.delete_user(legacy_uuid)

except Exception as e:
    print(f"FAIL: {e}")
    sys.exit(1)
