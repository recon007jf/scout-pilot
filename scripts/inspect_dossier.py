import os
import sys
import json
from supabase import create_client
from dotenv import load_dotenv

sys.path.append(os.getcwd())
load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
db = create_client(url, key)

print("Fetching one dossier to Inspect Structure...")
try:
    # Fetch headers/keys only if possible, or just one row
    res = db.table("dossiers").select("*").limit(1).execute()
    if res.data:
        d = res.data[0]
        print(f"Full Row: {json.dumps(d, indent=2, default=str)}")
    else:
        print("No dossiers found.")
except Exception as e:
    print(f"Error: {e}")
