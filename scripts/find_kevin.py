import os
import sys

# Add project root to path
sys.path.append(os.getcwd())

from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

db = create_client(url, key)

print("Searching for Kevin Overbey...")
res = db.table("dossiers").select("id, full_name, role, firm").text_search("full_name", "'Kevin' & 'Overbey'").execute()

if res.data:
    print(f"FOUND: {res.data[0]}")
else:
    print("NOT FOUND")
