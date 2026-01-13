from supabase import create_client
import inspect
from app.config import settings

def inspect_sdk():
    print("--- 1. Inspecting Supabase Client ---")
    try:
        # Initialize client (doesn't need real keys for inspection, but we have them)
        url = settings.SUPABASE_URL
        key = settings.SUPABASE_SERVICE_ROLE_KEY or settings.SUPABASE_KEY
        client = create_client(url, key)
        
        # Access auth (not admin)
        # reset_password_for_email is usually on the main auth interface, not admin
        reset_method = getattr(client.auth, "reset_password_email", None) or getattr(client.auth, "reset_password_for_email", None)
        
        print(f"Found Method: {reset_method}")
        if reset_method:
            print(f"Signature: {inspect.signature(reset_method)}")
            print(f"Docstring: {reset_method.__doc__}")
        else:
            print("Could not find reset_password method on client.auth either.")
            print(f"Available methods: {[m for m in dir(client.auth) if 'reset' in m]}")
        
        # Access auth.admin
        gen_method = client.auth.admin.generate_link
        
        print(f"Method: {gen_method}")
        print(f"Signature: {inspect.signature(gen_method)}")
        print(f"Docstring: {gen_method.__doc__}")
        
    except Exception as e:
        print(f"Error inspecting SDK: {e}") 
        
    # (Optional) Try to inspect options type if needed, but signature is key first.

        
    try:
        # Try to find the Type Definition
        from supabase_auth.types import InviteUserByEmailOptions
        print("\n--- 2. Inspecting InviteUserByEmailOptions ---")
        print(f"Fields: {InviteUserByEmailOptions.__annotations__}")
    except ImportError:
        print("\nCould not import InviteUserByEmailOptions directly. Trying via annotations...")
        # fallback
        pass

    try:
        from supabase.lib.client_options import ClientOptions
        print("\n--- 3. Inspecting ClientOptions ---")
        print(f"ClientOptions fields: {ClientOptions.__annotations__}")
    except ImportError:
        print("\nCould not import ClientOptions check 'supabase.client' or similar.")
        try:
             from supabase.client import ClientOptions
             print("\n--- 3. Inspecting ClientOptions (alternative path) ---")
             print(f"ClientOptions fields: {ClientOptions.__annotations__}")
        except:
             pass

if __name__ == "__main__":
    inspect_sdk()
