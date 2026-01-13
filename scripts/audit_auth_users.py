import os
import sys
from supabase import create_client

# Add parent dir to path to import app.config if needed, 
# but here we just need raw env vars to be safe and simple
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings

def audit_auth_users(email):
    print(f"--- Auditing Auth Users for: {email} ---")
    
    url = settings.SUPABASE_URL
    key = settings.SUPABASE_SERVICE_ROLE_KEY
    
    if not key:
        print("CRITICAL: SUPABASE_SERVICE_ROLE_KEY is missing.")
        return

    admin_db = create_client(url, key)
    
    # List users (pagination might be needed if many users, but for now just get first page)
    # The SDK method is list_users(page=1, per_page=50)
    try:
        users_response = admin_db.auth.admin.list_users(page=1, per_page=50)
        
        # Handle response format (list vs object)
        if isinstance(users_response, list):
             users_list = users_response
        else:
             users_list = getattr(users_response, "users", [])

        found_users = []
        for user in users_list:
            if user.email == email:
                found_users.append(user)
        
        print(f"Found {len(found_users)} Auth User(s) for {email}:")
        for u in found_users:
            print(f" - ID: {u.id}")
            print(f"   Email: {u.email}")
            print(f"   Created: {u.created_at}")
            print(f"   Last Sign In: {u.last_sign_in_at}")
            print(f"   Emaii Confirmed: {u.email_confirmed_at}")
            print(f"   Metadata: {u.user_metadata}")
            print("-" * 30)
            
        # Also check Profile for this email
        print("\n--- Checking Profiles Table ---")
        profiles = admin_db.table("profiles").select("*").eq("email", email).execute()
        if profiles.data:
            print(f"Found {len(profiles.data)} Profile(s):")
            for p in profiles.data:
                print(f" - Profile ID: {p.get('id')}")
                print(f"   Org ID: {p.get('org_id')}")
                print(f"   Matches Auth User? {p.get('id') in [u.id for u in found_users]}")
        else:
            print("No Profile found for this email.")

    except Exception as e:
        print(f"Error auditing users: {e}")

if __name__ == "__main__":
    target_email = "admin@pacificaisystems.com"
    audit_auth_users(target_email)
