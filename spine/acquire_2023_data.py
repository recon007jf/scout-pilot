import os
import glob
import requests
import zipfile
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# TASK: ACQUIRE 2023 DATA (PRODUCTION GRADE)
# 1. Directory Safety: Ensures folders exist before operations.
# 2. Atomic Writes: Downloads to .part, only renames on success.
# 3. Integrity Check: Validates ZIP structure immediately after download.
# 4. Detailed Inspection: Reports file sizes to confirm "Gold Mine" status.

def session_with_retries():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

def is_zip_valid(path):
    """Returns True if the zip file is valid and uncorrupted."""
    try:
        with zipfile.ZipFile(path, "r") as z:
            return z.testzip() is None
    except zipfile.BadZipFile:
        return False
    except Exception:
        return False

def cleanup_partials(directory):
    # Ensure directory exists first to avoid errors
    os.makedirs(directory, exist_ok=True)
    
    print(f"... Checking for stale partial files in {directory} ...")
    partials = glob.glob(os.path.join(directory, "*.part"))
    for p in partials:
        try:
            os.remove(p)
            print(f"   [CLEANED] Removed stale file: {os.path.basename(p)}")
        except Exception as e:
            print(f"   [WARN] Could not remove {p}: {e}")

def download_file(url: str, out_path: str, chunk_size: int = 1024 * 1024):
    # Ensure dir exists (redundant safety)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    
    tmp_path = out_path + ".part"
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

    try:
        with session_with_retries().get(url, headers=headers, stream=True, timeout=600) as r:
            r.raise_for_status()
            total = int(r.headers.get("Content-Length", 0))
            downloaded = 0

            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total:
                            pct = (downloaded / total) * 100
                            print(f"\r   ... {os.path.basename(out_path)}: {pct:0.1f}%", end="")
        
        print("\n   [VERIFYING] Checking Zip Integrity...")
        if is_zip_valid(tmp_path):
            os.replace(tmp_path, out_path)
            print(f"   [OK] Integrity Passed & Saved: {out_path}")
        else:
            print(f"   [CRITICAL FAIL] Zip downloaded but failed integrity check. Deleting.")
            os.remove(tmp_path)

    except Exception as e:
        print(f"\n   [ERROR] Failed to download {os.path.basename(out_path)}: {e}")

def inspect_results(directory):
    print("\n" + "-"*30)
    print("INSPECTING DOWNLOADED ZIPS (Verification)")
    print("-"*30)
    
    zips = sorted(glob.glob(os.path.join(directory, "*.zip")))
    if not zips:
        print("   [ERROR] No zip files found.")
        return

    for z in zips:
        zip_size_mb = os.path.getsize(z) / 1024 / 1024
        print(f"\nZIP: {os.path.basename(z)} ({zip_size_mb:.2f} MB)")
        try:
            with zipfile.ZipFile(z, "r") as f:
                for info in f.infolist():
                    # We only care about CSVs that aren't "layout" files
                    if info.filename.lower().endswith(".csv") and "layout" not in info.filename.lower():
                        size_mb = info.file_size / 1024 / 1024
                        print(f"  -> Content: {info.filename} ({size_mb:.2f} MB)")
        except zipfile.BadZipFile:
            print("  [ERROR] Corrupt Zip file.")

def run_production_pipeline():
    # PATH FIX: dol_spine is outside scratch if not referenced relatively, but user user absolute path.
    # I will respect the user's absolute path as it worked before.
    BASE_PATH = "/Users/josephlf/.gemini/antigravity/dol_spine"
    RAW_DIR = os.path.join(BASE_PATH, "00_raw_2023")
    
    # 1. DEFINE TARGETS (The "All Fields" Data)
    # VERIFIED URLS (Dec 2025): Path is .../2023/Latest/F_..._Latest.zip
    targets = {
        "f_5500_2023.zip": "https://www.askebsa.dol.gov/FOIA%20Files/2023/Latest/F_5500_2023_Latest.zip",
        "f_sch_a_2023.zip": "https://www.askebsa.dol.gov/FOIA%20Files/2023/Latest/F_SCH_A_2023_Latest.zip",
        "f_sch_c_2023.zip": "https://www.askebsa.dol.gov/FOIA%20Files/2023/Latest/F_SCH_C_2023_Latest.zip",
    }

    print(">>> INITIATING 2023 PRODUCTION PIPELINE")
    
    # 2. PREP & CLEAN
    cleanup_partials(RAW_DIR)

    # 3. DOWNLOAD & VERIFY
    for filename, url in targets.items():
        out_path = os.path.join(RAW_DIR, filename)
        
        # Check if file exists AND is valid
        if os.path.exists(out_path):
            if is_zip_valid(out_path):
                print(f"   [SKIP] Valid file exists: {filename}")
                continue
            else:
                print(f"   [RE-DOWNLOAD] Existing file corrupt: {filename}")
                os.remove(out_path)

        print(f"   Downloading: {filename}")
        download_file(url, out_path)

    # 4. INSPECT CONTENTS
    inspect_results(RAW_DIR)
    
    print("\n[DONE] Pipeline Complete.")

if __name__ == "__main__":
    run_production_pipeline()
