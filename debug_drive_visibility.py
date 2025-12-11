from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

def list_all_files():
    creds_file = 'credentials.json'
    scope = ['https://www.googleapis.com/auth/drive.readonly']
    creds = Credentials.from_service_account_file(creds_file, scopes=scope)
    service = build('drive', 'v3', credentials=creds)
    
    print("📡 Listing ALL visible files (Top 20)...")
    results = service.files().list(
        pageSize=20,
        fields="files(id, name, mimeType)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    
    files = results.get('files', [])
    if not files:
        print("❌ No files found. Service Account sees NOTHING.")
    else:
        for f in files:
            print(f"📄 Found: {f['name']} ({f['id']}) [{f['mimeType']}]")

if __name__ == "__main__":
    list_all_files()
