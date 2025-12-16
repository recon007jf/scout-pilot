
import requests
import zipfile
import io
import os
import csv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

URL = "https://askebsa.dol.gov/FOIA%20Files/2021/Latest/F_SCH_C_2021_Latest.zip"
ARTIFACT_DIR = "Scout_Data_Artifacts"
ZIP_PATH = os.path.join(ARTIFACT_DIR, "raw_SCH_C_2021.zip")

def create_session():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s

def run_smoke_test():
    os.makedirs(ARTIFACT_DIR, exist_ok=True)
    
    # 1. Force Clean Download to ensure no corruption
    if os.path.exists(ZIP_PATH):
        os.remove(ZIP_PATH)
        
    print(f"Downloading {URL}...")
    try:
        with create_session().get(URL, stream=True, timeout=300) as r:
            r.raise_for_status()
            with open(ZIP_PATH, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
    except Exception as e:
        print(f"Download Failed: {e}")
        return

    # 2. Inspect Zip
    print(f"Local Zip Size: {os.path.getsize(ZIP_PATH) / (1024*1024):.2f} MB")
    
    try:
        with zipfile.ZipFile(ZIP_PATH, 'r') as z:
            print(f"\nZIP CONTENTS:")
            csv_candidates = []
            for info in z.infolist():
                size_mb = info.file_size / (1024*1024)
                print(f" - {info.filename}: {size_mb:.2f} MB")
                if info.filename.lower().endswith('.csv') and 'layout' not in info.filename.lower():
                    csv_candidates.append(info)
            
            if not csv_candidates:
                print("No CSV found in zip.")
                return
            
            # Pick largest
            target_info = max(csv_candidates, key=lambda x: x.file_size)
            print(f"\nTARGETING LARGEST FILE: {target_info.filename} ({target_info.file_size / (1024*1024):.2f} MB)")
            
            with z.open(target_info) as f:
                f_text = io.TextIOWrapper(f, encoding='cp1252', errors='ignore')
                reader = csv.reader(f_text)
                
                headers = next(reader)
                print(f"\nHEADERS ({len(headers)}):")
                print(headers)
                
                print("\nFIRST 5 ROWS:")
                for i, row in enumerate(reader):
                    if i >= 5: break
                    print(row)
                    
                prov_col = next((h for h in headers if 'PROV' in h.upper() and 'NAME' in h.upper()), None)
                code_col = next((h for h in headers if 'CODE' in h.upper()), None)
                
                print("\nVERIFICATION:")
                print(f" - Contains SRVC_PROV_NAME? {'YES' if prov_col else 'NO'}")
                print(f" - Contains SRVC_CODE? {'YES' if code_col else 'NO'}")

    except Exception as e:
        print(f"Inspection Failed: {e}")

if __name__ == "__main__":
    run_smoke_test()
