import os
from dotenv import load_dotenv

# Try loading with explicit path
load_dotenv(".env")
google_key = os.getenv("GOOGLE_MAPS_SERVER_KEY")
pdl_key = os.getenv("PDL_API_KEY")

if google_key:
    print(f"✅ Google Key loaded: {google_key[:5]}...")
else:
    print("❌ Google Key NOT found.")

if pdl_key:
    print(f"✅ PDL Key loaded: {pdl_key[:5]}...")
else:
    print("❌ PDL Key NOT found.")
    # Debug what is found
    print(f"Current CWD: {os.getcwd()}")
    print(f"Files: {os.listdir('.')}")
