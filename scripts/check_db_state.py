from supabase import create_client
from app.config import settings

def check_scott():
    url = settings.SUPABASE_URL
    key = settings.SUPABASE_KEY
    db = create_client(url, key)
    
    print("--- Checking candidates ---")
    res = db.table("candidates").select("*").eq("full_name", "Scott Wood").execute()
    if res.data:
        c = res.data[0]
        print(f"Scott Wood (Candidate): ID={c['id']}")
        print(f"  Firm: {c['firm']}")
        print(f"  Image URL: {c.get('linkedin_image_url')}")
    else:
        print("Scott Wood not found in candidates.")

    print("\n--- Checking target_brokers ---")
    res_tb = db.table("target_brokers").select("*").eq("full_name", "Scott Wood").execute()
    if res_tb.data:
        tb = res_tb.data[0]
        print(f"Scott Wood (Broker): ID={tb['id']}")
        print(f"  Status: {tb.get('status')}")
        print(f"  profile_image: {tb.get('profile_image')}")
        print(f"  linkedin_image_url: {tb.get('linkedin_image_url')}")
    else:
        print("Scott Wood not found in target_brokers.")

if __name__ == "__main__":
    check_scott()
