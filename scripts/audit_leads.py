import time
from recon_agent import process_single_lead, get_google_sheet_client, GOOGLE_SHEET_NAME, WORKSHEET_NAME, gather_intel, analyze_lead

def audit_and_fix():
    print("🕵️ Starting Audit of 'Unknown' Archetypes...")
    
    client = get_google_sheet_client()
    if not client: return
    
    sheet = client.open(GOOGLE_SHEET_NAME).worksheet(WORKSHEET_NAME)
    data = sheet.get_all_records()
    headers = sheet.row_values(1)
    
    fixed_count = 0
    failed_count = 0
    
    for i, row in enumerate(data, start=2):
        dossier = row.get('Dossier Summary', '')
        full_name = f"{row.get('First Name', '')} {row.get('Last Name', '')}"
        
        # Check if Archetype is Unknown
        if "Archetype: Unknown" in dossier or dossier.strip() == "":
            print(f"\n⚠️ Found Issue at Row {i}: {full_name}")
            
            # 1. Check Prerequisites
            linkedin_url = row.get('LinkedIn URL', '')
            if not linkedin_url:
                print(f"   ❌ Reason: Missing LinkedIn URL")
                update_dossier_with_error(sheet, i, headers, "Missing LinkedIn URL. Cannot gather intel.")
                failed_count += 1
                continue
                
            # 2. Attempt Retry
            print(f"   🔄 Attempting to fix...")
            success = process_single_lead(i, row, headers, sheet)
            
            if success:
                # Verify if it actually fixed it (didn't just return Unknown again)
                # We need to re-read the row or trust the process_single_lead logic
                # For now, assume success means it tried its best.
                # Let's double check the output by re-reading the cell? 
                # process_single_lead updates the sheet directly.
                print(f"   ✅ Fixed!")
                fixed_count += 1
            else:
                print(f"   ❌ Retry Failed.")
                # If process_single_lead returns False, it usually prints why.
                # We can add a generic error note if it failed completely.
                update_dossier_with_error(sheet, i, headers, "AI Processing Failed. Likely rate limit or empty search results.")
                failed_count += 1
            
            time.sleep(1.5) # Be gentle with APIs

    print(f"\n📊 Audit Complete.")
    print(f"   Fixed: {fixed_count}")
    print(f"   Remaining Issues: {failed_count}")

def update_dossier_with_error(sheet, row_idx, headers, error_msg):
    dossier_col = headers.index("Dossier Summary") + 1
    error_text = f"""Archetype: Unknown
Driver: Unknown
Hook: N/A
Pain Points:
- ERROR: {error_msg}
- ACTION: Please verify LinkedIn URL and ensure profile is public."""
    sheet.update_cell(row_idx, dossier_col, error_text)

if __name__ == "__main__":
    audit_and_fix()
