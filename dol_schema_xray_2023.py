"""
MODULE: DOL_SCHEMA_XRAY_2023
OBJECTIVE: Verify 2023 Data Quality (Backfill Strategy)
"""
import os
import csv
import zipfile
import requests

YEAR = 2023 # TARGETING THE MATURE DATASET
OUT_DIR = "./_schema_xray_2023_tmp"

URLS = {
    "SCH_A": f"https://askebsa.dol.gov/FOIA%20Files/{YEAR}/Latest/F_SCH_A_{YEAR}_Latest.zip",
    "SCH_C": f"https://askebsa.dol.gov/FOIA%20Files/{YEAR}/Latest/F_SCH_C_{YEAR}_Latest.zip",
}

def stream_download(url, dest_path):
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    headers = {"User-Agent": "Mozilla/5.0"}
    with requests.get(url, headers=headers, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

def largest_data_csv(z):
    candidates = [n for n in z.namelist() if n.lower().endswith(".csv") and 'layout' not in n.lower()]
    return max(candidates, key=lambda n: z.getinfo(n).file_size) if candidates else None

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    print(f"STARTING SCHEMA X-RAY FOR {YEAR}")

    for label, url in URLS.items():
        zip_path = os.path.join(OUT_DIR, f"{label}_{YEAR}.zip")
        print(f"\nDownloading {label}...")
        try:
            stream_download(url, zip_path)
            with zipfile.ZipFile(zip_path, "r") as z:
                csv_name = largest_data_csv(z)
                if not csv_name:
                    print(f"[{label}] ERROR: No CSV found.")
                    continue
                
                # Check Size
                size_mb = z.getinfo(csv_name).file_size / 1024 / 1024
                print(f"[{label}] File: {csv_name} ({size_mb:.2f} MB)")
                
                # Check Headers
                with z.open(csv_name) as f:
                    text = (line.decode("cp1252", errors="ignore") for line in f)
                    headers = next(csv.reader(text), [])
                    
                    # TARGET CHECKS
                    has_broker = any("BROKER" in h for h in headers)
                    has_provider = any("PROV" in h for h in headers)
                    
                    print(f"[{label}] Has Broker/Provider Column? {'YES' if (has_broker or has_provider) else 'NO'}")
                    print(f"[{label}] First 10 Headers: {headers[:10]}")

        except Exception as e:
            print(f"[{label}] ERROR: {e}")
        finally:
            if os.path.exists(zip_path): os.remove(zip_path)

if __name__ == "__main__":
    main()
