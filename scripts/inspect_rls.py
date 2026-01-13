import os
import sys
from supabase import create_client

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.config import settings

def inspect_rls():
    print("--- Inspecting RLS Policies for 'profiles' ---")
    
    url = settings.SUPABASE_URL
    key = settings.SUPABASE_SERVICE_ROLE_KEY
    if not key: return

    admin_db = create_client(url, key)
    
    # Query pg_policies via RPC or direct SQL if possible?
    # Supabase-py doesn't have direct policy inspection easily.
    # But we can try to SELECT from profiles as the USER (simulate login) if we had their password.
    # Since we don't have their password, we can't get a user token easily.
    
    # Alternative: We can try a known SQL query if we have an RPC for executing SQL?
    # Or just use the 'postgres' connection if exposed? No.
    
    # Let's try to verify via the artifacts we have. 
    # But first, let's just create a dummy RLS test:
    # 1. Create a dummy user.
    # 2. Login as dummy user.
    # 3. Try to read own profile.
    # This simulates the frontend flow.
    
    TEST_EMAIL = "rls_test_admin@pacificaisystems.com"
    TEST_PASS = "TestPass123!"
    
    print(f"Provisioning RLS Test User: {TEST_EMAIL}")
    
    # 1. Ensure User Exists
    u_res = admin_db.auth.admin.list_users(page=1, per_page=100)
    u_list = getattr(u_res, "users", []) if not isinstance(u_res, list) else u_res
    
    test_uid = None
    for u in u_list:
        if u.email == TEST_EMAIL:
            test_uid = u.id
            break
            
    if not test_uid:
        r = admin_db.auth.admin.create_user({"email": TEST_EMAIL, "password": TEST_PASS, "email_confirm": True})
        test_uid = r.user.id
        print(f"Created Test User: {test_uid}")
    
    # 2. Login as User (Get User Client)
    print("Logging in as Test User...")
    try:
        user_client = create_client(url, settings.SUPABASE_KEY) # Public Anon Key needed? No, just use client logic
        # Actually create_client uses the anon key by default usually?
        # We need to sign in.
        session = user_client.auth.sign_in_with_password({"email": TEST_EMAIL, "password": TEST_PASS})
        token = session.session.access_token
        
        print("Login Successful. Token obtained.")
        
        # 3. Try to Read Own Profile
        print("Attempting to SELECT own profile...")
        # Create client authenticated with user token
        # supabase-py doesn't easily swizzle tokens on the fly without re-init or headers?
        # Actually verify_session logic used explicit headers.
        
        # Let's just use the `options` to pass header if possible, or use postgrest directly.
        # user_client.postgrest.auth(token)
        user_client.postgrest.auth(token)
        
        res = user_client.table("profiles").select("*").eq("id", test_uid).execute()
        
        if res.data:
            print("✅ RLS SUCCESS: User can read own profile.")
            print(res.data)
        else:
            print("❌ RLS FAILURE: User CANNOT read own profile (Empty Data).")
            
        return
        
    except Exception as e:
        print(f"❌ Login/RLS Test Failed: {e}")

if __name__ == "__main__":
    inspect_rls()
