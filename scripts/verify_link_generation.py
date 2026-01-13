import os
import sys
import urllib.parse
from supabase import create_client

# Add parent dir to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings

def verify_link_generation():
    print("--- Verifying Supabase Link Generation ---")
    
    url = settings.SUPABASE_URL
    key = settings.SUPABASE_SERVICE_ROLE_KEY
    
    # 1. Confirm Project URL
    print(f"Supabase Project URL: {url}")
    
    if not key:
        print("CRITICAL: SUPABASE_SERVICE_ROLE_KEY missing.")
        return

    admin_db = create_client(url, key)
    
    AUTH_REDIRECT = "https://v0-scout-ui.vercel.app/auth/callback?next=/auth/update-password"
    TEST_EMAIL = "test_verify_link@pacificaisystems.com"
    
    print(f"Target Redirect: {AUTH_REDIRECT}")
    print(f"Test Email: {TEST_EMAIL}")
    
    # 0. Clean Slate (Delete if exists)
    print("\n[TEST 0] Cleaning up Test User...")
    try:
        users_res = admin_db.auth.admin.list_users(page=1, per_page=100)
        u_list = getattr(users_res, "users", []) if not isinstance(users_res, list) else users_res
        for u in u_list:
            if u.email == TEST_EMAIL:
                 print(f"User {TEST_EMAIL} found. Deleting...")
                 admin_db.auth.admin.delete_user(u.id)
                 print("User deleted.")
                 break
    except Exception as e:
         print(f"Error cleaning user: {e}")

    # 1. Generate Invite Link (Requires user NOT to exist usually, or just works)
    # Actually, generate_link(type="invite") works for new users.

    # 2. Generate Invite Link
    print("\n[TEST A] Generating 'Invite' Link...")
    try:
        # Note: generate_link uses 'options' just like invite_user_by_email
        res = admin_db.auth.admin.generate_link(
            params={
                "type": "invite",
                "email": TEST_EMAIL,
                "options": {
                    "data": {"role": "test", "org_id": "test-org"},
                    "redirect_to": AUTH_REDIRECT
                }
            }
        )
        link = res.properties.action_link
        print(f"GENERATED LINK: {link}")
        
        # Parse and Update
        parsed = urllib.parse.urlparse(link)
        params = urllib.parse.parse_qs(parsed.query)
        actual_redirect = params.get("redirect_to", ["MISSING"])[0]
        
        if actual_redirect == AUTH_REDIRECT:
            print("✅ PASS: redirect_to matches exactly.")
        else:
            print(f"❌ FAIL: redirect_to is '{actual_redirect}'")
            
    except Exception as e:
        print(f"Error generating invite link: {e}")

    # 2. Setup for Recovery (User must exist)
    print("\n[TEST B PREP] Creating User for Recovery Test...")
    try:
         admin_db.auth.admin.create_user({
             "email": TEST_EMAIL,
             "email_confirm": True,
             "password": "TempPassword123!"
         })
         print("User created.")
    except Exception as e:
         print(f"Error creating user: {e}")

    # 3. Generate Recovery Link
    print("\n[TEST B] Generating 'Recovery' Link...")
    try:
        res = admin_db.auth.admin.generate_link(
            params={
                "type": "recovery",
                "email": TEST_EMAIL,
                "options": {
                    "redirect_to": AUTH_REDIRECT
                }
            }
        )
        link = res.properties.action_link
        print(f"GENERATED LINK: {link}")
        
        # Parse and Update
        parsed = urllib.parse.urlparse(link)
        params = urllib.parse.parse_qs(parsed.query)
        actual_redirect = params.get("redirect_to", ["MISSING"])[0]
        
        if actual_redirect == AUTH_REDIRECT:
            print("✅ PASS: redirect_to matches exactly.")
        else:
            print(f"❌ FAIL: redirect_to is '{actual_redirect}'")

    except Exception as e:
        print(f"Error generating recovery link: {e}")

if __name__ == "__main__":
    verify_link_generation()
