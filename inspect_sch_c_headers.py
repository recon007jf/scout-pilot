
import csv
import os

PATH = "Scout_Data_Artifacts/F_SCH_C_2022_latest.csv"

try:
    with open(PATH, 'r', encoding='cp1252', errors='ignore') as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)
        print(f"Total Headers: {len(headers)}")
        print(f"Headers: {headers}")
        
        # Fuzzy match checks
        print("\nTarget Column Candidates:")
        prov = [h for h in headers if 'PROV' in h.upper() or 'NAME' in h.upper()]
        code = [h for h in headers if 'CODE' in h.upper()]
        amt = [h for h in headers if 'AMT' in h.upper()]
        
        print(f"Provider Name candidates: {prov}")
        print(f"Service Code candidates: {code}")
        print(f"Amount candidates: {amt}")
        
except Exception as e:
    print(f"Error: {e}")
