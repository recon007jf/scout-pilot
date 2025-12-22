"""
MODULE: DOL_FILE_INVENTORY_DISKSAFE
OBJECTIVE: List every filename + file size inside 2024 Schedule A and Schedule C ZIPs
NOTES:
- Streams ZIP to disk (no RAM blowups)
- Prints full inventory (do not paraphrase output)
"""

import os
import zipfile
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

YEAR = 2024
OUT_DIR = "./_dol_zip_inventory_tmp"

URLS = {
    "SCH_A": f"https://askebsa.dol.gov/FOIA%20Files/{YEAR}/Latest/F_SCH_A_{YEAR}_Latest.zip",
    "SCH_C": f"https://askebsa.dol.gov/FOIA%20Files/{YEAR}/Latest/F_SCH_C_{YEAR}_Latest.zip",
}

def session_with_retries():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

def stream_download(url: str, dest_path: str) -> None:
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    headers = {"User-Agent": "Mozilla/5.0"}
    with session_with_retries().get(url, headers=headers, stream=True, timeout=180) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

def list_zip(zip_path: str, label: str) -> None:
    print(f"\n[{label}] ZIP PATH: {zip_path}")
    with zipfile.ZipFile(zip_path, "r") as z:
        infos = z.infolist()
        print(f"[{label}] FILE COUNT: {len(infos)}")
        for info in infos:
            size_mb = info.file_size / 1024 / 1024
            print(f"  - {info.filename} ({size_mb:.2f} MB)")

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    print(f"STARTING DOL ZIP INVENTORY FOR {YEAR}")

    for label, url in URLS.items():
        zip_path = os.path.join(OUT_DIR, f"{label}_{YEAR}.zip")
        print(f"\nDownloading {label} from: {url}")
        try:
            stream_download(url, zip_path)
            list_zip(zip_path, label)
        except Exception as e:
            print(f"\n[{label}] ERROR: {e}")
        finally:
            if os.path.exists(zip_path):
                os.remove(zip_path)

if __name__ == "__main__":
    main()
