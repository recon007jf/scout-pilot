
import os
from supabase import create_client

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not url or not key:
    print("Error imports")
    exit(1)

sb = create_client(url, key)

print("--- DIAGNOSTIC: ID Comparison ---")

try:
    # ids from target_brokers
    res_tb = sb.table("target_brokers").select("id").limit(5).execute()
    tb_ids = [r['id'] for r in res_tb.data]
    print(f"Target Broker IDs (Sample): {tb_ids}")
    
    # ids from candidates
    res_cand = sb.table("candidates").select("id").limit(5).execute()
    c_ids = [r['id'] for r in res_cand.data]
    print(f"Candidate IDs (Sample): {c_ids}")
    
    # intersection
    if set(tb_ids) & set(c_ids):
        print("IDs OVERLAP.")
    else:
        print("IDs DO NOT OVERLAP.")
        
except Exception as e:
    print(f"Error: {e}")
