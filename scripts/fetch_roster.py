
import os
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import io
from googleapiclient.http import MediaIoBaseDownload

def fetch_roster_file():
    print("🔎 Broad Search for 'Regional Planning'...")
    
    creds_file = 'credentials.json'
    if not os.path.exists(creds_file):
        print("❌ credentials.json NOT found.")
        return

    try:
        scope = ['https://www.googleapis.com/auth/drive.readonly']
        creds = Credentials.from_service_account_file(creds_file, scopes=scope)
        service = build('drive', 'v3', credentials=creds)
        
        # Broad Search
        results = service.files().list(
            q="name contains 'Regional Planning' and trashed = false",
            fields="files(id, name)",
            pageSize=10,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        
        files = results.get('files', [])
        
        if files:
            print(f"🎉 Found {len(files)} candidate(s):")
            for f in files:
                print(f"   - {f['name']} (ID: {f['id']})")
                
            # Pick the best match or the first one
            target_file = files[0]
            print(f"⬇️ Downloading '{target_file['name']}' to data/roster_master.csv...")
            
            output_path = "data/roster_master.csv"
            
            request = service.files().get_media(fileId=target_file['id'])
            fh = io.FileIO(output_path, 'wb')
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            print("✅ Download Complete.")
            
        else:
            print(f"⚠️ Could NOT find any file matching 'Regional Planning' in Drive.")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    fetch_roster_file()
