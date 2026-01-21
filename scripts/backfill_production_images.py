import os
from dotenv import load_dotenv
from supabase import create_client

# Load env vars
load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not url or not key:
    print("Error: Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
    exit(1)

supabase = create_client(url, key)

# Map names to images
assignments = {
    "Andrew Forchelli": "/professional-male-executive-business-headshot.jpg",
    "Steve Wolfenberger": "/professional-man-headshot-business-director.jpg",
    "David Osterhaus": "/professional-business-headshot-male.jpg",
    "Alex Michon": "/professional-business-headshot-asian-male.jpg",
    "Jennifer Hutchins": "/professional-business-headshot-female.jpg",
    "Neil Parton": "/professional-male-executive-vp-risk.jpg",
    "Maggie Osburn": "/professional-woman-headshot-insurance-executive.jpg",
    "Brian Hetherington": "/professional-man-headshot-benefits-director.jpg",
    "Scott Wood": "/professional-male-executive-business-headshot.jpg",
    # Fallbacks for others
    "Kevin Overbey": "/professional-man-headshot-benefits-director.jpg" # Already set, but good to keep in map
}

print(f"Starting backfill for {len(assignments)} targets...")

for name, image_path in assignments.items():
    try:
        # Update record
        res = supabase.table("target_brokers").update({"profile_image": image_path}).eq("full_name", name).execute()
        
        if res.data:
            print(f"✅ Updated {name}")
        else:
            print(f"⚠️  Target not found: {name}")
            
    except Exception as e:
        print(f"❌ Error updating {name}: {e}")

print("Backfill complete.")
