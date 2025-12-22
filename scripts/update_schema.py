import gspread
from google.oauth2.service_account import Credentials
import os
from dotenv import load_dotenv
from schema import FORENSIC_COLUMNS

# Load environment variables
load_dotenv()

GOOGLE_SHEET_NAME = 'Scout Leads'
WORKSHEET_NAME = 'Sheet1'
CREDS_FILE = 'credentials.json'

def get_google_sheet_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    if not os.path.exists(CREDS_FILE):
        print(f"❌ ERROR: Could not find '{CREDS_FILE}'.")
        return None
    try:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        print(f"❌ Error authenticating with Google: {e}")
        return None

def update_schema():
    print("--- 🛡️ Starting Schema Upgrade ---")
    client = get_google_sheet_client()
    if not client: return

    try:
        sheet = client.open(GOOGLE_SHEET_NAME).worksheet(WORKSHEET_NAME)
        headers = sheet.row_values(1)
        
        print(f"Current Headers: {headers}")
        
        # Check and add missing columns
        new_columns_added = 0
        current_col_count = len(headers)
        
        for col_name in FORENSIC_COLUMNS:
            if col_name not in headers:
                print(f"   ➕ Adding column: '{col_name}'...")
                current_col_count += 1
                sheet.update_cell(1, current_col_count, col_name)
                new_columns_added += 1
            else:
                print(f"   ✅ Column '{col_name}' already exists.")
                
        if new_columns_added > 0:
            print(f"\n🎉 Success! Added {new_columns_added} new forensic columns.")
        else:
            print("\n✅ Schema is already up to date.")

    except Exception as e:
        print(f"❌ Error updating schema: {e}")

if __name__ == "__main__":
    update_schema()
