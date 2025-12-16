
import requests
import zipfile
import io
import os
import csv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Hypothetical URL based on DOL naming conventions
URL = "https://askebsa.dol.gov/FOIA%20Files/2021/Latest/F_SCH_C_PART1_ITEM1_2021_Latest.zip"
ARTIFACT_DIR = "Scout_Data_Artifacts"
ZIP_PATH = os.path.join(ARTIFACT_DIR, "raw_SCH_C_PART1_2021.zip")

def create_session():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[404, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s

def run_smoke_test():
    os.makedirs(ARTIFACT_DIR, exist_ok=True)
    if os.path.exists(ZIP_PATH):
        os.remove(ZIP_PATH)
        
    print(f"Testing URL: {URL}")
    try:
        with create_session().get(URL, stream=True, timeout=300) as r:
            if r.status_code == 404:
                print("   [FAILED] URL not found (404).")
                return
            r.raise_for_status()
            
            with open(ZIP_PATH, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
            print("   [SUCCESS] Downloaded Zip.")
    except Exception as e:
        print(f"   [ERROR] Download Failed: {e}")
        return

    # Inspect
    try:
        with zipfile.ZipFile(ZIP_PATH, 'r') as z:
            print(f"\nZIP CONTENTS:")
            target_info = None
            for info in z.infolist():
                print(f" - {info.filename}: {info.file_size / (1024*1024):.2f} MB")
                if info.filename.lower().endswith('.csv') and 'layout' not in info.filename.lower():
                    if not target_info or info.file_size > target_info.file_size:
                        target_info = info

            if target_info:
                print(f"\nINSPECTING: {target_info.filename}")
                with z.open(target_info) as f:
                    f_text = io.TextIOWrapper(f, encoding='cp1252', errors='ignore')
                    reader = csv.reader(f_text)
                    headers = next(reader)
                    print(f"HEADERS ({len(headers)}): {headers}")
                    
                    print("\nFIRST 3 ROWS:")
                    for i, row in enumerate(reader):
                        if i >= 3: break
                        print(row)
                    
                    prov_col = next((h for h in headers if 'PROV' in h.upper() and 'NAME' in h.upper()), None)
                    print(f"\nVERIFICATION: Found Provider Name? {'YES (' + prov_col + ')' if prov_col else 'NO'}")

    except Exception as e:
        print(f"Inspection Failed: {e}")

if __name__ == "__main__":
    run_smoke_test()
