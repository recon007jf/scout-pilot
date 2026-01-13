import os
import sys
import json
from supabase import create_client

# Add parent dir
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings

def debug_user_state():
    email = "admin@pacificaisystems.com"
    print(f"--- Deep Debug for: {email} ---")
    
    url = settings.SUPABASE_URL
    key = settings.SUPABASE_SERVICE_ROLE_KEY
    
    if not key:
        print("CRITICAL: SUPABASE_SERVICE_ROLE_KEY missing.")
        return

    admin_db = create_client(url, key)
    
    # 1. Auth User Check
    print("\n[1] Checking Auth User...")
    try:
        # Get user by email using list_users (safest way to search)
        # Note: using the same logic as before but dumping MORE info
        res = admin_db.auth.admin.list_users(page=1, per_page=100)
        u_list = getattr(res, "users", []) if not isinstance(res, list) else res
        
        target_user = None
        for u in u_list:
            if u.email == email:
                target_user = u
                break
        
        if target_user:
            print(f"✅ Auth User Found: {target_user.id}")
            print(f"   Email Confirmed At: {target_user.email_confirmed_at}")
            print(f"   Last Sign In At:    {target_user.last_sign_in_at}")
            try:
                print(f"   Banned Until:       {getattr(target_user, 'banned_until', 'N/A')}")
            except:
                pass
            print(f"   User Metadata:      {target_user.user_metadata}")
            print(f"   App Metadata:       {target_user.app_metadata}")
        else:
            print("❌ Auth User NOT FOUND.")
            return

    except Exception as e:
        print(f"Error checking auth user: {e}")
        # Continue to profile check anyway if we have target_user
        if not target_user: return

    # 2. Profile Check
    print("\n[2] Checking Profile...")
    try:
        # Bypass RLS with service role key
        p_res = admin_db.table("profiles").select("*").eq("id", target_user.id).execute()
        if p_res.data:
            profile = p_res.data[0]
            print(f"✅ Profile Found: {profile.get('id')}")
            print(f"   Role:   {profile.get('role')}")
            print(f"   Org ID: {profile.get('org_id')}")
            
            # Check Org Validity
            org_id = profile.get('org_id')
            if org_id:
                o_res = admin_db.table("organizations").select("*").eq("id", org_id).execute()
                if o_res.data:
                     print(f"✅ Linked Organization Exists: {o_res.data[0].get('name')} ({org_id})")
                else:
                     print(f"❌ Linked Organization {org_id} DOES NOT EXIST (Orphaned Profile).")
            else:
                 print("❌ Profile has NO Org ID.")
        else:
            print("❌ Profile NOT FOUND (Sync Issue).")

    except Exception as e:
        print(f"Error checking profile: {e}")

if __name__ == "__main__":
    debug_user_state()
