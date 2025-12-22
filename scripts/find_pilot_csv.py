
import os
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

def find_recent_csvs():
    print("🔎 Searching Drive for recent CSVs (Andrew/Target/West)...")
    
    creds_file = 'credentials.json'
    if not os.path.exists(creds_file):
        print("❌ credentials.json missing.")
        return

    try:
        scope = ['https://www.googleapis.com/auth/drive.readonly']
        creds = Credentials.from_service_account_file(creds_file, scopes=scope)
        service = build('drive', 'v3', credentials=creds)
        
        # Broad Search: All CSVs modified recently
        queries = [
            "mimeType = 'text/csv' and modifiedTime > '2024-12-10T00:00:00'",
        ]
        
        found = []
        for q in queries:
            results = service.files().list(
                q=f"{q} and trashed = false",
                fields="files(id, name, modifiedTime)",
                pageSize=5,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
            found.extend(results.get('files', []))
            
        print(f"\n🎉 Found {len(found)} candidate(s):")
        unique_files = {f['id']: f for f in found}.values() # Dedup
        
        for f in unique_files:
            print(f"   - {f['name']} (ID: {f['id']}) [Mod: {f.get('modifiedTime')}]")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    find_recent_csvs()
