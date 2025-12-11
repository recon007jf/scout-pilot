import os
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv, set_key

def auto_discover_drive_files():
    """
    Searches the Service Account's accessible Drive for:
    1. master_5500.csv
    2. master_sched_a.csv
    
    If found, writes their IDs to .env so drive_loader.py can fetch them.
    """
    print("🔎 Starting Google Drive Auto-Discovery...")
    
    creds_file = 'credentials.json'
    if not os.path.exists(creds_file):
        print("❌ credentials.json NOT found. Auto-Discovery Failed.")
        return

    try:
        # Auth
        scope = ['https://www.googleapis.com/auth/drive.readonly']
        creds = Credentials.from_service_account_file(creds_file, scopes=scope)
        service = build('drive', 'v3', credentials=creds)
        
        # Define Targets (Now ZIPs based on user upload)
        targets = {
            'F_5500_2024_Latest.zip': 'DOL_5500_DRIVE_ID',
            'F_SCH_A_2024_Latest.zip': 'SCHED_A_DRIVE_ID',
            'F_SCH_C_2024_Latest.zip': 'SCHED_C_DRIVE_ID'
        }
        
        found = 0
        
        for filename, env_var in targets.items():
            # Check if already set
            if os.getenv(env_var):
                print(f"   ✅ {env_var} already set ({os.getenv(env_var)}). Skipping search.")
                found += 1
                continue
                
            # Search (Expanded to Shared Drives)
            print(f"   📡 Scanning All Drives for '{filename}'...")
            results = service.files().list(
                q=f"name = '{filename}' and trashed = false",
                fields="files(id, name)",
                pageSize=1,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
            
            files = results.get('files', [])
            
            if files:
                file_id = files[0]['id']
                print(f"   🎉 FOUND '{filename}'! ID: {file_id}")
                
                # Update .env (Local & Runtime)
                os.environ[env_var] = file_id
                
                # Persist to .env file for future strict usage
                try:
                    set_key(".env", env_var, file_id)
                except:
                    # If set_key fails (e.g. cloud environment), we just rely on os.environ for this session
                    pass
                found += 1
            else:
                print(f"   ⚠️ Could NOT find '{filename}' in Drive.")
                
        if found == 2:
            print("✅ Auto-Discovery Complete. All pipes connected.")
        else:
            print(f"⚠️ Auto-Discovery Partial. Found {found}/2 files.")
            
    except Exception as e:
        print(f"❌ Auto-Discovery Error: {e}")

if __name__ == "__main__":
    auto_discover_drive_files()
