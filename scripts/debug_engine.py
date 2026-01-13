import os
import sys

# Add project root to path
sys.path.append(os.getcwd())

from app.core.draft_engine import DraftEngine
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
gemini_key = os.environ.get("GEMINI_API_KEY")

print(f"URL: {url}")
print(f"GEMINI KEY Present: {bool(gemini_key)}")

db = create_client(url, key)
engine = DraftEngine(db)

DOSSIER_ID = "3b446948-440a-4169-b700-284ef9f99f0e"

print(f"Running DraftEngine Atomic for {DOSSIER_ID}...")

try:
    output = engine.generate_draft_atomic(DOSSIER_ID, force_regenerate=True)
    print("SUCCESS!")
    print(f"Subject: {output.subject}")
    print(f"Body Preview: {output.body[:50]}...")
    print(f"Status: {output.status}")
except Exception as e:
    print(f"CRITICAL FAILURE: {e}")
    import traceback
    traceback.print_exc()
