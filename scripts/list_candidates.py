
import os
import sys

# Add project root to path
sys.path.append(os.getcwd())

from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not url or not key:
    print("Missing params")
    sys.exit(1)

try:
    db = create_client(url, key)
    res = db.table("candidates").select("id").limit(1).execute()
    if res.data:
        print(f"VALID_ID: {res.data[0]['id']}")
    else:
        print("NO_DATA")
except Exception as e:
    print(f"ERROR: {e}")
