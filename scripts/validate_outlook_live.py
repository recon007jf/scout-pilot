
import sys
import os
import datetime
import json

# Add backend directory to sys.path to allow imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings
from app.core.outlook import OutlookAuth, OutlookClient
from supabase import create_client, ClientOptions

def validate_outlook_connection(user_email: str):
    print(f"🔍 Validating Outlook Connection for: {user_email}")
    print("-" * 50)
    
    # 0. Setup DB
    if not settings.SUPABASE_SERVICE_ROLE_KEY:
        print("❌ Error: SUPABASE_SERVICE_ROLE_KEY not set.")
        return
        
    db = create_client(
        settings.SUPABASE_URL, 
        settings.SUPABASE_SERVICE_ROLE_KEY,
        options=ClientOptions(flow_type="implicit")
    )

    # Step 1: Token & Scope Verification (Hard Proof)
    print("\n[Step 1] Verifying Token Storage...")
    res = db.table("integration_tokens").select("*").eq("user_email", user_email).eq("provider", "outlook").execute()
    
    if not res.data:
        print("❌ Fail: No record found in integration_tokens.")
        return

    record = res.data[0]
    access_token = record.get("access_token")
    refresh_token = record.get("refresh_token")
    
    # Check Metadata from User Preferences (Workaround for Schema Lock)
    pref_res = db.table("user_preferences").select("*").eq("user_email", user_email).execute()
    prefs = pref_res.data[0].get("preferences", {}) if pref_res.data else {}
    outlook_meta = prefs.get("outlook", {})
    
    scopes = outlook_meta.get("scopes") or record.get("scopes")
    expires_at_str = outlook_meta.get("expires_at") or record.get("expires_at")
    
    print(f"   - Access Token: {'[PRESENT]' if access_token else '[MISSING]'}")
    print(f"   - Refresh Token: {'[PRESENT]' if refresh_token else '[MISSING]'}")
    print(f"   - Scopes: {scopes}")
    print(f"   - Expires At: {expires_at_str} (Source: {'UserPrefs' if outlook_meta.get('expires_at') else 'TokensTable'})")

    if not access_token or not refresh_token:
        print("❌ Fail: Missing tokens.")
        return
        
    if not scopes:
        print("⚠️ Warning: Scopes column is empty (Migrate schemas? Re-auth needed?).")
    
    # Check expiry
    if expires_at_str:
        expires_at = datetime.datetime.fromisoformat(expires_at_str.replace('Z', '+00:00'))
        is_expired = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc) > expires_at
        if is_expired:
             print("⚠️ Warning: Token appears expired in DB. Refresh logic should handle this.")
        else:
             print("✅ Token is valid (time-wise).")

    # Step 2: Microsoft Graph Read-Only Call
    print("\n[Step 2] Testing Graph API Read (GET /me)...")
    try:
        client = OutlookClient(access_token)
        me = client.get_me()
        print(f"✅ Success: Connected as {me.get('displayName')} ({me.get('userPrincipalName')})")
    except Exception as e:
        print(f"❌ Fail: Graph Read Failed: {e}")
        print("   Attempting Refresh early...")
        # Fallthrough to refresh?
    
    # Step 3: Draft Email Test (No Send)
    print("\n[Step 3] Testing Draft Creation (No Send)...")
    try:
        draft = client.create_draft(
            subject=f"Scout Validation Test {datetime.datetime.utcnow().isoformat()}",
            body="<h1>Technical Validation</h1><p>Draft creation successful.</p>",
            to_emails=[user_email]
        )
        print(f"✅ Success: Draft Created (ID: {draft.get('id')})")
        # Cleanup? 
        # print("   (Draft left in folder for verification)")
    except Exception as e:
        print(f"❌ Fail: Draft Creation Failed: {e}")

    # Step 4: Refresh Token Validation
    print("\n[Step 4] Validating Refresh Flow...")
    try:
        auth = OutlookAuth()
        new_tokens = auth.refresh_token(refresh_token)
        
        if "access_token" in new_tokens:
            print("✅ Success: New Access Token Acquired.")
            # Verify new token works?
            client_v2 = OutlookClient(new_tokens["access_token"])
            me_v2 = client_v2.get_me()
            print(f"   - Verified new token works for {me_v2.get('userPrincipalName')}")
        else:
            print(f"❌ Fail: Refresh response invalid: {new_tokens}")
            
    except Exception as e:
        print(f"❌ Fail: Refresh Flow Error: {e}")

    print("-" * 50)
    print("Validation Complete.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/validate_outlook_live.py <email>")
        sys.exit(1)
    
    validate_outlook_connection(sys.argv[1])
