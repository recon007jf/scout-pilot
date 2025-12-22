import os
from supabase import create_client
from dotenv import load_dotenv

def verify_staging():
    print(">>> VERIFYING STAGING TABLE")
    
    BASE_PATH = "/Users/josephlf/.gemini/antigravity/scratch"
    load_dotenv(os.path.join(BASE_PATH, ".env"))
    
    url = os.getenv("SUPABASE_URL")
    # Try Service Key first
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_ANON_KEY")
    
    if not url or not key:
        print("   [ERROR] Credentials missing.")
        return

    supabase = create_client(url, key)
    
    try:
        # Get Count
        response = supabase.table("scout_drafts").select("*", count="exact").execute()
        count = response.count
        data = response.data
        
        print(f"   [RESULT] Table 'scout_drafts' contains {count} rows.")
        
        if count > 0:
            print("   [SAMPLE] Companies Staged:")
            for row in data[:20]:
                print(f"    - {row.get('company')} ({row.get('status')})")
                
            # Verify JSON
            check_json = data[0].get('draft_body')
            if isinstance(check_json, dict):
                 print("    [PASS] JSONB 'draft_body' is valid.")
            else:
                 print(f"    [FAIL] JSONB invalid: {type(check_json)}")
                 
    except Exception as e:
        print(f"   [ERROR] Verification Failed: {e}")

if __name__ == "__main__":
    verify_staging()
