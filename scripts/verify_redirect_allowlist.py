import os
import sys
from urllib.parse import urlparse, parse_qs

# Ensure app can be imported
sys.path.append(os.getcwd())

from app.config import settings
from supabase import create_client, ClientOptions

def verify_redirects():
    print("🔍 Verifying Supabase Redirect Allowlist...")
    
    url = settings.SUPABASE_URL
    key = settings.SUPABASE_SERVICE_ROLE_KEY
    
    # Use implicit flow
    admin_db = create_client(url, key, options=ClientOptions(flow_type="implicit"))
    
    target_email = "admin@pacificaisystems.com" 
    
    urls_to_test = [
        "https://v0-scout-ui.vercel.app/auth/callback",
        "https://v0-scout-ui.vercel.app/auth/update-password"
    ]
    
    success = True
    
    for test_url in urls_to_test:
        print(f"\nTesting Redirect: {test_url}")
        try:
            # Correct SDK Usage: generate_link(params={...})
            res = admin_db.auth.admin.generate_link(
                params={
                    "type": "magiclink",
                    "email": target_email,
                    "options": {
                        "redirect_to": test_url
                    }
                }
            )
            
            link = res.properties.action_link
            print(f"Generated Link: {link}")
            
            # Parse query params
            parsed = urlparse(link)
            qs = parse_qs(parsed.query)
            
            redirect_to = qs.get("redirect_to", [""])[0]
            
            if redirect_to == test_url:
                print(f"✅ PASSED: Redirect URL preserved.")
            else:
                print(f"❌ FAILED: Redirect URL mismatch.")
                print(f"   Expected: {test_url}")
                print(f"   Got:      {redirect_to}")
                success = False
                
        except Exception as e:
            print(f"❌ ERROR: Generation failed: {e}")
            success = False
            
    if success:
        print("\n✅ VERIFICATION COMPLETE: Both URLs are effectively Allowlisted.")
    else:
        print("\n❌ VERIFICATION FAILED: Review Supabase Dashboard Settings.")

if __name__ == "__main__":
    verify_redirects()
