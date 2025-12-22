"""
MODULE: DOL_SCHEMA_XRAY_DISKSAFE
OBJECTIVE: Print raw CSV headers for 2024 Schedule A and Schedule C (Latest)
NOTES:
- Streams ZIP to disk (no RAM blowups)
- Extracts only the largest CSV (data file)
- Prints header row only
"""

import os
import re
import csv
import zipfile
import requests

YEAR = 2024
OUT_DIR = "./_schema_xray_tmp"

URLS = {
    "SCH_A": f"https://askebsa.dol.gov/FOIA%20Files/{YEAR}/Latest/F_SCH_A_{YEAR}_Latest.zip",
    "SCH_C": f"https://askebsa.dol.gov/FOIA%20Files/{YEAR}/Latest/F_SCH_C_{YEAR}_Latest.zip",
}

def stream_download(url: str, dest_path: str) -> None:
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    headers = {"User-Agent": "Mozilla/5.0"}
    with requests.get(url, headers=headers, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

def largest_data_csv(z: zipfile.ZipFile, dataset_key: str) -> str | None:
    # Prefer files that look like the actual data table, ignore layouts/readmes
    candidates = []
    for name in z.namelist():
        lower = name.lower()
        if not lower.endswith(".csv"):
            continue
        if "layout" in lower or "readme" in lower:
            continue
        candidates.append(name)

    if not candidates:
        return None

    # Pick largest CSV as the data file
    return max(candidates, key=lambda n: z.getinfo(n).file_size)

def print_headers_from_zip(zip_path: str, label: str) -> None:
    with zipfile.ZipFile(zip_path, "r") as z:
        csv_name = largest_data_csv(z, label)
        if not csv_name:
            print(f"\n[{label}] ERROR: No data CSV found in zip. Contents sample: {z.namelist()[:20]}")
            return

        print(f"\n[{label}] Data CSV: {csv_name}")

        with z.open(csv_name) as f:
            text = (line.decode("cp1252", errors="ignore") for line in f)
            reader = csv.reader(text)
            headers = next(reader, [])
            print(f"[{label}] Header count: {len(headers)}")
            print(f"[{label}] Headers:")
            print(headers)

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    print(f"STARTING SCHEMA X-RAY FOR {YEAR}")

    for label, url in URLS.items():
        zip_path = os.path.join(OUT_DIR, f"{label}_{YEAR}.zip")
        print(f"\nDownloading {label} from: {url}")
        try:
            stream_download(url, zip_path)
            print_headers_from_zip(zip_path, label)
        except Exception as e:
            print(f"\n[{label}] ERROR: {e}")
        finally:
            if os.path.exists(zip_path):
                os.remove(zip_path)

if __name__ == "__main__":
    main()
