import os
import sys
from supabase import create_client, ClientOptions

# Ensure app can be imported
sys.path.append(os.getcwd())

from app.config import settings

def verify_data():
    print("🔍 Auditing Data for Morning Briefing Readiness...")
    
    url = settings.SUPABASE_URL
    key = settings.SUPABASE_SERVICE_ROLE_KEY
    if not key:
        print("❌ Error: SERVICE_ROLE_KEY missing.")
        return

    admin_db = create_client(url, key, options=ClientOptions(flow_type="implicit"))

    # 1. Check Tables Existence and Columns (via trivial select)
    tables = ["dossiers", "psyche_profiles", "leads_pilot"]
    
    for t in tables:
        print(f"\n--- Table: {t} ---")
        try:
            # Fetch 1 row to see keys
            res = admin_db.table(t).select("*").limit(1).execute()
            if res.data:
                keys = res.data[0].keys()
                print(f"✅ Exists. Columns: {list(keys)}")
                
                # Check for org_id
                if "org_id" in keys:
                    print(f"   Linkage: Has 'org_id' column.")
                else:
                    print(f"   ⚠️ WARNING: No 'org_id' column found so far (checking 1st row).")
            else:
                print(f"⚠️ Table exists but is EMPTY.")
                # Count
                count = admin_db.table(t).select("*", count="exact", head=True).execute()
                print(f"   Row Count: {count.count}")
                
        except Exception as e:
            print(f"❌ Error accessing table {t}: {e}")

    # 2. Check Specific Data for Admin Org
    # Org ID from previous context: df966238-4b56-4ed3-886c-157854d8ce90
    target_org = "df966238-4b56-4ed3-886c-157854d8ce90"
    print(f"\n--- Checking Data for Org: {target_org} ---")
    
    try:
        # Check dossiers count for this org
        # Note: If org_id column is missing, this query will fail or return 0 if we assume it exists
        # We'll try to select count eq org_id
        res = admin_db.table("dossiers").select("*", count="exact", head=True).eq("org_id", target_org).execute()
        print(f"Dossiers Count: {res.count}")
        
        # Check 'Warm' profiles (Briefing Source)
        # Assuming psyche_profiles is linked to dossiers, and dossiers have org_id?
        # Or psyche_profiles has org_id?
        # Let's check relation by querying ONE warm profile and seeing its data
        res_warm = admin_db.table("psyche_profiles").select("*, dossiers(*)").eq("risk_profile", "Warm").limit(1).execute()
        if res_warm.data:
            print(f"Found 'Warm' Profile sample.")
            # Check if dossier has org_id
            prof = res_warm.data[0]
            dossier = prof.get("dossiers")
            if dossier:
                print(f"   Linked Dossier Org: {dossier.get('org_id')}")
        else:
            print(f"No 'Warm' profiles found globally.")

    except Exception as e:
        print(f"❌ Query Error: {e}")

if __name__ == "__main__":
    verify_data()
