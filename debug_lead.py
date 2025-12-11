from recon_agent import process_single_lead, get_google_sheet_client, GOOGLE_SHEET_NAME, WORKSHEET_NAME

def debug_specific_lead(target_name):
    print(f"🔎 Searching for {target_name}...")
    client = get_google_sheet_client()
    sheet = client.open(GOOGLE_SHEET_NAME).worksheet(WORKSHEET_NAME)
    data = sheet.get_all_records()
    headers = sheet.row_values(1)
    
    for i, row in enumerate(data, start=2):
        full_name = f"{row.get('First Name', '')} {row.get('Last Name', '')}"
        if target_name.lower() in full_name.lower():
            print(f"🎯 Found {full_name} at Row {i}")
            # Run processing
            process_single_lead(i, row, headers, sheet)
            return

    print(f"❌ Could not find lead: {target_name}")

if __name__ == "__main__":
    debug_specific_lead("Brian Hetherington")
