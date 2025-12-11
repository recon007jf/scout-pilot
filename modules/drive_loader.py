import os
import io
import zipfile
import shutil
import glob
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.service_account import Credentials
from modules.ui_components import render_message

def get_drive_service():
    """Authenticates with Service Account"""
    creds_file = 'credentials.json'
    if not os.path.exists(creds_file):
        return None
    scope = ['https://www.googleapis.com/auth/drive.readonly']
    creds = Credentials.from_service_account_file(creds_file, scopes=scope)
    return build('drive', 'v3', credentials=creds)

def download_file_authenticated(service, file_id, output_path):
    """Downloads a file using the Drive API (Authenticated)"""
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(output_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            # Optional: print(f"Download {int(status.progress() * 100)}%.")
        return True, "Download Complete"
    except Exception as e:
        return False, str(e)

def extract_and_rename(zip_path, target_csv_path):
    """
    Unzips the file and renames the largest CSV found to target_csv_path.
    """
    extract_dir = "data/temp_extract"
    os.makedirs(extract_dir, exist_ok=True)
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
            
        # Find CSV
        csv_files = glob.glob(f"{extract_dir}/**/*.csv", recursive=True)
        if not csv_files:
            csv_files = glob.glob(f"{extract_dir}/*.csv")
            
        if csv_files:
            largest_csv = max(csv_files, key=os.path.getsize)
            shutil.move(largest_csv, target_csv_path)
            shutil.rmtree(extract_dir)
            return True, f"Extracted {os.path.basename(largest_csv)}"
        else:
             return False, "No CSV found in ZIP."
    except Exception as e:
        return False, str(e)

def download_data_from_drive():
    """
    Checks for presence of master_5500.zip and master_sched_a.zip.
    If missing, downloads (Authenticated).
    Does NOT Extract (Saves Disk/RAM).
    """
    dol_zip_path = "data/master_5500.zip"
    sched_a_zip_path = "data/master_sched_a.zip"
    sched_c_zip_path = "data/master_sched_c.zip"
    
    os.makedirs("data", exist_ok=True)
    
    # Init Service
    service = get_drive_service()
    if not service:
        render_message("Credentials missing. Cannot download.", "error")
        return

    # 1. DOL 5500 (ZIP)
    if not os.path.exists(dol_zip_path) or os.path.getsize(dol_zip_path) < 1000:
        file_id = os.getenv("DOL_5500_DRIVE_ID")
        if file_id:
            render_message(f"⬇️ Downloading DOL 5500 ZIP (Auth ID: {file_id})...", "info")
            ok, dl_msg = download_file_authenticated(service, file_id, dol_zip_path)
            
            if ok:
                render_message(f"✅ DOL 5500 Saved (Compressed).", "success")
            else:
                render_message(f"Download Failed: {dl_msg}", "error")
    else:
        render_message("✅ DOL 5500 Found (Cached).", "success")

    # 2. Schedule A (ZIP)
    if not os.path.exists(sched_a_zip_path) or os.path.getsize(sched_a_zip_path) < 1000:
         file_id = os.getenv("SCHED_A_DRIVE_ID")
         if file_id:
            render_message(f"⬇️ Downloading Sched A ZIP (Auth ID: {file_id})...", "info")
            ok, dl_msg = download_file_authenticated(service, file_id, sched_a_zip_path)
            
            if ok:
                 render_message(f"✅ Schedule A Saved (Compressed).", "success")
            else:
                 render_message(f"Download Failed: {dl_msg}", "error")
    else:
        render_message("✅ Sched A Found (Cached).", "success")

    # 3. Schedule C (ZIP)
    if not os.path.exists(sched_c_zip_path) or os.path.getsize(sched_c_zip_path) < 1000:
         file_id = os.getenv("SCHED_C_DRIVE_ID")
         if file_id:
            render_message(f"⬇️ Downloading Sched C ZIP (Auth ID: {file_id})...", "info")
            ok, dl_msg = download_file_authenticated(service, file_id, sched_c_zip_path)
            
            if ok:
                 render_message(f"✅ Schedule C Saved (Compressed).", "success")
            else:
                 render_message(f"Download Failed: {dl_msg}", "error")
    else:
        render_message("✅ Sched C Found (Cached).", "success")
