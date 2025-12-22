import os
import json
from supabase import create_client
from dotenv import load_dotenv

def verify_enriched_brokers():
    print(">>> VERIFYING ENRICHED BROKERS")
    
    BASE_PATH = "/Users/josephlf/.gemini/antigravity/scratch"
    load_dotenv(os.path.join(BASE_PATH, ".env"))
    
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    print(f"Connecting to {url.split('//')[1].split('.')[0]}...") # confirm project again
    supabase = create_client(url, key)

    print("\n--- Final Database State (Enriched Brokers) ---")
    
    try:
        rows = supabase.table("target_brokers").select("status, full_name, work_email").eq("status", "ENRICHED").execute().data
        
        print(f"Total Enriched Count: {len(rows)}")
        for r in rows:
            print(f"- {r['full_name']}: {r['work_email']} [{r['status']}]")
            
        print("\n--- Failed/Skipped/Sent ---")
        # Just quick summary of others
        all_stats = supabase.table("target_brokers").select("status", count="exact").execute().data
        stats = {}
        for x in all_stats:
            s = x['status']
            stats[s] = stats.get(s, 0) + 1
        print(f"Distribution: {stats}")

    except Exception as e:
        print(f"[ERROR] Verification failed: {e}")

if __name__ == "__main__":
    verify_enriched_brokers()
