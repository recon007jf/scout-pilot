import os
import sys

# Ensure app can be imported
sys.path.append(os.getcwd())

from app.config import settings
from supabase import create_client, ClientOptions

def reset_admin_password():
    email = "admin@pacificaisystems.com"
    new_password = "AdminTest123!"

    print(f"[v0] Resetting password for {email}...")
    
    # Init Admin Client
    url = settings.SUPABASE_URL
    key = settings.SUPABASE_SERVICE_ROLE_KEY
    
    if not key:
        print("Error: SUPABASE_SERVICE_ROLE_KEY not set.")
        return

    # Use implicit flow to avoid PKCE warnings (as per our standard)
    try:
        admin_db = create_client(url, key, options=ClientOptions(flow_type="implicit"))
    except:
        # Fallback if ClientOptions import fails (though we verified it earlier)
        admin_db = create_client(url, key)

    # List users to find ID
    print(f"[v0] Searching for user {email}...")
    found_user = None
    
    try:
        page = 1
        while True:
            res = admin_db.auth.admin.list_users(page=page, per_page=50)
            # Handle potential response shapes
            users = res.users if hasattr(res, 'users') else (res if isinstance(res, list) else [])
            
            for u in users:
                if u.email == email:
                    found_user = u
                    break
            
            if found_user:
                break
            
            if not users or len(users) < 50:
                break
                
            page += 1
            
        if not found_user:
             print(f"[v0] User not found: {email}")
             return

        print(f"[v0] Found user ID: {found_user.id}")
        
        # Update Password
        # update_user_by_id(uid, attributes)
        admin_db.auth.admin.update_user_by_id(found_user.id, {"password": new_password})
        
        print("[v0] ✅ Password successfully updated!")
        print(f"[v0] You can now log in with:")
        print(f"[v0] Email: {email}")
        print(f"[v0] Password: {new_password}")

    except Exception as e:
        print(f"[v0] Error: {e}")

if __name__ == "__main__":
    reset_admin_password()
