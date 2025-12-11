import gspread
from google.oauth2.service_account import Credentials
import json
import re
import time
import os

# --- CONFIGURATION ---
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

def clean_text(text):
    """Removes markdown formatting like **bold**, *italics*, etc."""
    if not isinstance(text, str): return str(text)
    # Remove bold/italic markers
    text = text.replace('**', '').replace('__', '')
    # Remove list markers if they are just asterisks (keep hyphens for clean lists)
    # text = text.replace('* ', '- ') 
    return text

def format_dossier(data):
    """Converts a dictionary to the standard plain text format."""
    disc = data.get('DISC_Type', 'Unknown')
    style = data.get('Communication_Style', 'Unknown')
    hook = clean_text(data.get('Hook', 'N/A'))
    
    pain_points = data.get('Pain_Points', [])
    if isinstance(pain_points, str):
        pain_points = [pain_points]
    
    pp_text = ""
    for pp in pain_points:
        pp_text += f"- {clean_text(pp)}\n"
    
    return f"""DISC: {disc}
Style: {style}
Hook: {hook}
Pain Points:
{pp_text.strip()}"""

def main():
    print("--- 🧹 Starting Dossier Cleanup ---")
    client = get_google_sheet_client()
    if not client: return

    try:
        sheet = client.open(GOOGLE_SHEET_NAME).worksheet(WORKSHEET_NAME)
        data = sheet.get_all_records()
        headers = sheet.row_values(1)
        
        if "Dossier Summary" not in headers:
            print("❌ 'Dossier Summary' column not found.")
            return
            
        dossier_col_idx = headers.index("Dossier Summary") + 1
        
        updates = 0
        for i, row in enumerate(data, start=2):
            raw_dossier = row.get('Dossier Summary', '')
            if not raw_dossier: continue
            
            new_dossier = raw_dossier
            needs_update = False
            
            # Case 1: It's JSON (starts with {)
            if isinstance(raw_dossier, str) and raw_dossier.strip().startswith('{'):
                try:
                    json_data = json.loads(raw_dossier)
                    new_dossier = format_dossier(json_data)
                    needs_update = True
                    print(f"   🔄 Converting JSON for Row {i}...")
                except json.JSONDecodeError:
                    pass
            
            # Case 2: It's Text but might have Markdown
            if not needs_update and isinstance(raw_dossier, str):
                if "**" in raw_dossier or "__" in raw_dossier:
                    new_dossier = clean_text(raw_dossier)
                    needs_update = True
                    print(f"   ✨ Cleaning Markdown for Row {i}...")

            if needs_update:
                sheet.update_cell(i, dossier_col_idx, new_dossier)
                updates += 1
                time.sleep(1) # Rate limit
                
        print(f"✅ Cleanup Complete! Updated {updates} rows.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
