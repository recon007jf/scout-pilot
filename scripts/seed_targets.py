import sys
import os
# Ensure backend directory is in python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

import csv
from supabase import create_client, Client
from app.config import settings
from app.utils.logger import get_logger
from app.utils.identity import resolve_identity

logger = get_logger("seed_targets")

def seed_targets(csv_path: str, db_client: Client = None):
    logger.info("Starting Ingestion", extra={"csv_path": csv_path})
    
    if not os.path.exists(csv_path) and not db_client: 
        # Only strict file check if not testing? 
        # Actually for testing we pass a mock csv path usually or mock open. 
        # But for this DI refactor, just allow the client.
        pass

    if not os.path.exists(csv_path):
         logger.critical("CSV File Not Found", extra={"path": csv_path})
         # If testing, we might want to suppress sys.exit or handle it.
         # For MVP fast refactor:
         if not os.environ.get("TESTING"):
             sys.exit(1)

    # Connect to Supabase
    if db_client:
        supabase = db_client
    else:
        try:
            supabase: Client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
        except Exception as e:
            logger.critical(f"Failed to connect to Supabase: {e}")
            sys.exit(1)

    processed = 0
    upserted = 0
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            processed += 1
            
            # CSV: SYSTEM MASTER Schema (67MB)
            # Headers: SPONSOR_NAME,LIVES,PROVIDER_NAME_NORM,PROVIDER_STATE,Contact Full Name,Contact Email,Contact Mobile Phone 1,Contact Job Title,Company Name,firm_norm...
            
            full_name = row.get("Contact Full Name", "").strip() # Updated from "Full Name"
            # Fallback to Pilot/Andrew schema if missing
            if not full_name:
                 full_name = row.get("Full Name", "").strip()

            work_email = row.get("Contact Email", "").strip()
            if not work_email:
                 work_email = row.get("Work Email", "").strip()

            firm = row.get("firm_norm", "").strip() or row.get("Company Name", "").strip()
            if not firm:
                 firm = row.get("Firm", "").strip()

            role = row.get("Contact Job Title", "").strip()
            if not role:
                 role = row.get("Role", "").strip()

            tier = row.get("Funding_Confidence", "").strip()
            if not tier:
                 tier = row.get("Tier", "").strip()

            linkedin_url = row.get("LinkedIn_URL", "").strip()
            if not linkedin_url:
                 linkedin_url = row.get("LinkedIn URL", "").strip()
            
            if not full_name: 
                continue
                
            # Diamond Standard Identity Resolution
            identity_key, identity_type = resolve_identity(row)
            # ... rest of logic
            
            # ...
            
            dossier_record = {
                "identity_key": identity_key,
                "identity_type": identity_type,
                "full_name": full_name,
                "firm": firm,
                "role": role,
                "work_email": work_email,
                "linkedin_url": linkedin_url,
                "tier": tier,
                "raw_data": row # Store original for audit
            }
            
            try:
                # Idempotent Upsert
                # On Conflict: identity_key
                data = supabase.table("dossiers").upsert(
                    dossier_record, 
                    on_conflict="identity_key"
                ).execute()
                upserted += 1
                
                # Check for Psyche Profile and CREATE if missing (Stub for now)
                # In a real run, this would be where we init the baseline
                if data.data:
                    dossier_id = data.data[0]['id']
                    # Ensure psyche profile exists (Base Baseline)
                    # For MVP, we'll assume we can insert here on conflict do nothing
                    profile_record = {
                        "dossier_id": dossier_id,
                        "base_archetype": "Unknown", # Default
                        "risk_profile": "Cold-Safe" # Default
                    }
                    supabase.table("psyche_profiles").upsert(
                        profile_record, on_conflict="dossier_id"
                    ).execute()
                    
            except Exception as e:
                logger.error(f"Failed to upsert row {full_name}: {e}")

    logger.info("Ingestion Complete", extra={"processed": processed, "upserted": upserted})

if __name__ == "__main__":
    # Default Path: SYSTEM Golden Master (Source of Truth)
    # Relative to this script (backend/scripts/ -> backend/)
    CSV_PATH = os.path.join(os.path.dirname(__file__), '..', 'Master_Hunting_List_Production_v3_SYSTEM_ENRICHED_FUNDING_PATCHED.csv')
    seed_targets(CSV_PATH)
