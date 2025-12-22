import os
import csv
import sys
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables
BASE_PATH = "/Users/josephlf/.gemini/antigravity/scratch"
load_dotenv(os.path.join(BASE_PATH, ".env"))

url: str = os.environ.get("SUPABASE_URL")
# Try validation with multiple key names just in case
key: str = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not url or not key:
    print("[ERROR] Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in .env")
    sys.exit(1)

supabase: Client = create_client(url, key)

# EXACT PATH PROVIDED BY USER
CSV_FILE_PATH = '/Users/josephlf/.gemini/antigravity/scratch/backend/Scout_Data_Artifacts/pilot_inputs/enriched_brokers.csv'

def normalize_header(header):
    """Normalize CSV headers to handle Clay's variable naming."""
    return header.lower().strip().replace(" ", "_").replace("-", "_")

def main():
    # 1. Check if file exists at the specific path
    target_file = CSV_FILE_PATH
    if not os.path.exists(target_file):
        # Fallback: Check current directory just in case
        if os.path.exists("enriched_brokers.csv"):
            target_file = "enriched_brokers.csv"
            print(f"[WARN] File not found at absolute path. Found in current dir: {target_file}")
        else:
            print(f"[ERROR] Could not find file at: {CSV_FILE_PATH}")
            return
    else:
        print(f"[INFO] Found file at: {target_file}")

    print(f"[START] Reading CSV...")
    
    updated_count = 0
    failed_count = 0
    
    with open(target_file, mode='r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        
        # Map headers
        header_map = {normalize_header(h): h for h in reader.fieldnames}
        
        # Locate critical columns
        id_col = next((v for k, v in header_map.items() if 'record_id' in k), None)
        email_col = next((v for k, v in header_map.items() if 'work_email' in k or 'email' in k), None)
        linkedin_col = next((v for k, v in header_map.items() if 'linkedin' in k), None)
        
        if not id_col:
            print(f"[ERROR] Could not find 'Record Id' column. Found: {reader.fieldnames}")
            return

        print(f"[INFO] Mapping: ID='{id_col}', Email='{email_col}', LinkedIn='{linkedin_col}'")

        for row in reader:
            record_id = row.get(id_col, "").strip()
            email = row.get(email_col, "").strip() if email_col else ""
            linkedin = row.get(linkedin_col, "").strip() if linkedin_col else ""

            if not record_id:
                continue

            # Update Payload
            update_data = {}
            new_status = 'ENRICHMENT_FAILED'
            
            if email:
                update_data['work_email'] = email
                new_status = 'ENRICHED'
            
            if linkedin:
                update_data['linkedin_url'] = linkedin

            update_data['status'] = new_status
            update_data['clay_status'] = 'processed_by_receiver'
            
            # Execute Update
            try:
                res = supabase.table('target_brokers').update(update_data).eq('id', record_id).execute()
                if new_status == 'ENRICHED':
                    updated_count += 1
                    print(f"[SUCCESS] {record_id} -> {email}")
                else:
                    failed_count += 1
                    print(f"[SKIPPED] {record_id} (No Email)")
            except Exception as e:
                print(f"[ERROR] Update failed for {record_id}: {e}")

    print("-" * 30)
    print(f"[COMPLETE] Ingestion finished.")
    print(f"Total Enriched: {updated_count}")
    print(f"Total Failed/Skipped: {failed_count}")

if __name__ == "__main__":
    main()
