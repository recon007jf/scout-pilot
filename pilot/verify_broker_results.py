import os
from supabase import create_client
from dotenv import load_dotenv

def verify_broker_results():
    print(">>> VERIFYING BROKER PIVOT RESULTS")
    
    BASE_PATH = "/Users/josephlf/.gemini/antigravity/scratch"
    load_dotenv(os.path.join(BASE_PATH, ".env"))
    
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    supabase = create_client(url, key)

    print("\n--- Final Database State (Status Distribution) ---")
    # Using python to simulate GROUP BY since client doesn't do it easily for print
    # But strictly, we can just fetch all and aggregate.
    rows = supabase.table("target_brokers").select("status").execute().data
    
    status_counts = {}
    for r in rows:
        s = r["status"]
        status_counts[s] = status_counts.get(s, 0) + 1
        
    for s, c in status_counts.items():
        print(f"{s}: {c}")
        
    print("\n--- Zombie Check ---")
    # status='IN_PROGRESS' OR lock_id IS NOT NULL OR locked_at IS NOT NULL OR locked_by IS NOT NULL;
    
    # We will fetch count of bad rows
    # Logic: Status is IN_PROGRESS OR any lock field is set
    # Let's do it in python to be safe with OR syntax or just check specific conditions
    
    c1 = supabase.table("target_brokers").select("*", count="exact").eq("status", "IN_PROGRESS").execute().count
    c2 = supabase.table("target_brokers").select("*", count="exact").neq("lock_id", "null").execute().count
    # Fix for timestamp null check from previous learning
    c3 = supabase.table("target_brokers").select("*", count="exact").filter("locked_at", "not.is", "null").execute().count
    c4 = supabase.table("target_brokers").select("*", count="exact").neq("locked_by", "null").execute().count
    
    # Note: simple sum might double count if a row has multiple issues, but >0 is bad anyway.
    total_zombies = c1 + c2 + c3 + c4
    print(f"Zombies / Locked Rows: {total_zombies} (Detail: InProg={c1}, LockID={c2}, LockAt={c3}, LockBy={c4})")

if __name__ == "__main__":
    verify_broker_results()
