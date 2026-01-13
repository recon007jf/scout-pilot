import os
import sys
from supabase import create_client

# Add parent dir to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings

def trigger_real_recovery():
    email = "admin@pacificaisystems.com"
    print(f"--- Triggering Real Recovery for: {email} ---")
    
    url = settings.SUPABASE_URL
    key = settings.SUPABASE_SERVICE_ROLE_KEY
    
    if not key:
        print("CRITICAL: SUPABASE_SERVICE_ROLE_KEY missing.")
        return

    admin_db = create_client(url, key)
    
    # Exact production configuration
    AUTH_REDIRECT = "https://v0-scout-ui.vercel.app/auth/callback?next=/auth/update-password"
    
    print(f"Redirect Target: {AUTH_REDIRECT}")

    try:
        print("Sending password reset email...")
        admin_db.auth.reset_password_email(
            email,
            options={"redirect_to": AUTH_REDIRECT}
        )
        print("✅ Success: Email sent via Supabase.")
        
    except Exception as e:
        print(f"❌ Error sending recovery email: {e}")

if __name__ == "__main__":
    trigger_real_recovery()
