
import csv

PATH = "backend/Scout_Data_Artifacts/F_SCH_A_2023_latest.csv"

try:
    with open(PATH, "r", encoding="cp1252", errors="ignore") as f:
        headers = [h.upper().strip() for h in next(csv.reader(f))]
        print("HEADERS FOUND:")
        print(headers)
        
        target = "INS_CARRIER_NAME"
        if target in headers:
            print(f"\nSUCCESS: '{target}' found.")
        else:
            print(f"\nFAILURE: '{target}' NOT found.")
            
except Exception as e:
    print(f"Error: {e}")
