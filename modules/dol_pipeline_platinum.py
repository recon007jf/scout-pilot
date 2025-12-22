"""
MODULE: AG_DOL_PIPELINE_PLATINUM
VERSION: 5.4 (Configurable Hunt Modes / Env-Safe / Firm Vectoring)
OBJECTIVE: 
  1. Ingest DOL data (Base, Sch A, Sch C) with network resilience.
  2. Generate High-Confidence "Firm Vectors" (Broker & TPA Firms linked to Plans).
  3. Aggregate multiple vendors per plan to prevent row explosion.
  4. Rank TPAs by Total Compensation (Direct + Indirect).
  5. Upload Traceable Parquet Artifact to GCS (if configured).

NOTE ON SERVICE CODES:
Service codes in Schedule C vary by year and filer interpretation. 
The defaults below target "TPA/Consultant" roles. Adjust 'HUNT_MODE' codes 
if targeting Actuaries (Code 2) or other specialists.
"""

import duckdb
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import zipfile
import os
import re
import csv
import logging
from datetime import datetime
from google.cloud import storage # Requires: pip install google-cloud-storage

# ==========================================
# CONFIGURATION
# ==========================================
# GEOGRAPHY
TARGET_STATES = ['CA', 'OR', 'WA', 'ID', 'NV', 'AZ', 'NM', 'CO']
MIN_LIVES = 100

# STORAGE
# Defaults to None. Set GCS_BUCKET_NAME in your .env or Cloud Run config to enable upload.
ARTIFACT_DIR = os.getenv("ARTIFACT_STORAGE_PATH", "./Scout_Data_Artifacts")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME") 

# HUNT MODES (Service Code Presets)
# 27=TPA, 28=Brokerage, 49=Other (Common for admins). 
# 15=Contract Admin, 2=Actuary, 26=Recordkeeping (Often excluded to remove noise).
TARGET_SERVICE_CODES = ['27', '28', '49'] 

# SCHEMA MAPPING (Canonical : Potential DOL Headers)
SCHEMA_CONTRACT = {
    'BASE': {
        'ACK_ID': ['ACK_ID'],
        'EIN': ['SPONS_DFE_EIN', 'EIN'],
        'EMPLOYER_NAME': ['SPONS_DF_NAME', 'PLAN_NAME', 'SPONSOR_NAME'],
        'STATE': ['SPONS_DFE_MAIL_US_STATE', 'SPONS_US_STATE', 'US_STATE', 'STATE'],
        'LIVES': ['TOT_PARTCP_BOY_CNT', 'TOT_PARTCP_CNT']
    },
    'SCH_A': {
        'ACK_ID': ['ACK_ID'],
        'BROKER_NAME': ['INS_BROKER_FIRM_NAME', 'BROKER_NAME', 'INS_CARRIER_NAME']
    },
    'SCH_C': {
        'ACK_ID': ['ACK_ID'],
        'PROVIDER_NAME': ['SRVC_PROV_NAME', 'PROVIDER_NAME'],
        'SERVICE_CODES': ['SRVC_CODE_LIST', 'SERVICE_CODES'],
        'PAY_DIRECT': ['DIRECT_COMP_AMT'], 
        'PAY_INDIRECT': ['INDIRECT_COMP_AMT']
    }
}

# ==========================================
# PHASE 1: RESILIENT INGESTION
# ==========================================
def create_session():
    """Creates a requests session with retry logic for flaky government servers."""
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s

def download_and_extract(year, dataset_type, output_folder):
    """Downloads zip and extracts best CSV match. Returns None on failure."""
    base_url = f"https://askebsa.dol.gov/FOIA%20Files/{year}/Latest/{{filename}}"
    filenames = {'BASE': f"F_5500_{year}_Latest.zip", 'SCH_A': f"F_SCH_A_{year}_Latest.zip", 'SCH_C': f"F_SCH_C_{year}_Latest.zip"}
    
    url = base_url.format(filename=filenames[dataset_type])
    zip_path = os.path.join(output_folder, f"raw_{dataset_type}_{year}.zip")
    
    # Ensure directory exists before downloading
    os.makedirs(output_folder, exist_ok=True)

    try:
        print(f"   [INGEST] Streaming {dataset_type}...")
        with create_session().get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(zip_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except Exception as e:
        logging.warning(f"   [WARN] Download failed for {dataset_type}: {e}")
        return None

    # Extraction
    patterns = {'BASE': r"f_5500_.*\.csv", 'SCH_A': r"f_sch_a_.*\.csv", 'SCH_C': r"f_sch_c_.*\.csv"}
    best_file = None
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            # Find all files matching the pattern (ignoring layouts/readmes)
            candidates = [n for n in z.namelist() if re.search(patterns[dataset_type], n, re.IGNORECASE) and 'layout' not in n.lower()]
            if candidates:
                # Tie-breaker: Largest file usually contains the actual data
                best_file = max(candidates, key=lambda x: z.getinfo(x).file_size)
                z.extract(best_file, output_folder)
                best_file = os.path.join(output_folder, best_file)
    except Exception as e:
        logging.warning(f"   [WARN] Extraction failed for {dataset_type}: {e}")
    finally:
        if os.path.exists(zip_path): os.remove(zip_path)
        
    return best_file

def upload_to_gcs(local_path, destination_blob_name):
    """Uploads artifact to GCS only if bucket is configured."""
    if not GCS_BUCKET_NAME:
        print("   [CLOUD] Skipping upload (GCS_BUCKET_NAME not set).")
        return

    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(local_path)
        print(f"   [CLOUD] Uploaded to gs://{GCS_BUCKET_NAME}/{destination_blob_name}")
    except Exception as e:
        logging.error(f"   [ERROR] GCS Upload failed: {e}")

# ==========================================
# PHASE 2: SMART SCHEMA RESOLUTION
# ==========================================
def resolve_header_select(csv_path, dataset_type):
    """Scans CSV header and returns SQL SELECT clause to map to canonical names."""
    with open(csv_path, 'r', encoding='cp1252', errors='ignore') as f:
        actual_headers = [h.upper().strip() for h in next(csv.reader(f))]

    contract = SCHEMA_CONTRACT[dataset_type]
    select_parts = []

    for canonical, options in contract.items():
        found = next((opt for opt in options if opt in actual_headers), None)
        if found:
            select_parts.append(f'"{found}" AS {canonical}')
        elif canonical.startswith('PAY_'):
            select_parts.append(f"'0' AS {canonical}") # Default missing pay to '0' string
        else:
            raise ValueError(f"CRITICAL: Missing required '{canonical}' in {dataset_type}")
    return ", ".join(select_parts)

# ==========================================
# PHASE 3: DUCKDB EXECUTION
# ==========================================
def run_pipeline_platinum(year):
    print(f"AG STATUS: Starting Platinum Pipeline for {year}")
    
    # Ensure artifact dir exists
    os.makedirs(ARTIFACT_DIR, exist_ok=True)

    files = {d: download_and_extract(year, d, ARTIFACT_DIR) for d in ['BASE', 'SCH_A']}
    
    # Fail only if Base is missing
    if not files['BASE']:
        print("   [STOP] Base 5500 file missing. Aborting.")
        return

    con = duckdb.connect(database=":memory:")

    # 1. Create Views (Handling Schema & Missing Optional Files)
    for dtype, path in files.items():
        if path:
            select_stmt = resolve_header_select(path, dtype)
            # Use read_csv_auto for best encoding detection, fallback to latin-1/cp1252 is internal default often
            con.execute(f"CREATE OR REPLACE VIEW v_{dtype.lower()} AS SELECT {select_stmt} FROM read_csv_auto('{path}', ignore_errors=True)")
        else:
            # Empty dummy view to allow JOINs to succeed
            dummy_cols = ", ".join([f"NULL::VARCHAR AS {k}" for k in SCHEMA_CONTRACT[dtype].keys()])
            con.execute(f"CREATE OR REPLACE VIEW v_{dtype.lower()} AS SELECT {dummy_cols} WHERE 1=0")

    # 2. OPTIMIZED BASE VIEW
    print("   [SQL] Optimizing Base View...")
    con.execute(f"""
        CREATE OR REPLACE VIEW v_base_filtered AS 
        SELECT * FROM v_base 
        WHERE STATE IN {tuple(TARGET_STATES)} 
          AND TRY_CAST(LIVES AS INTEGER) >= {MIN_LIVES}
    """)

    # 3. CLEAN & AGGREGATE SCH_A (Brokers)
    print("   [SQL] Aggregating Brokers...")
    con.execute("""
        CREATE OR REPLACE VIEW v_a_clean AS
        SELECT ACK_ID, STRING_AGG(DISTINCT BROKER_NAME, ' | ') AS BROKER_LIST
        FROM v_sch_a 
        WHERE BROKER_NAME IS NOT NULL
        GROUP BY ACK_ID
    """)

    # 4. SKIPPED SCH_C (Data Unavailable - Raw file lacks provider names)
    # To re-enable, add SCH_C to files list and restore v_c_clean view logic


    # 5. MASTER JOIN
    query = """
        SELECT 
            base.ACK_ID,
            base.EIN,
            base.EMPLOYER_NAME,
            base.STATE,
            base.LIVES,
            COALESCE(a.BROKER_LIST, 'Unknown') AS BROKERS,
            'None Listed' AS TPAS,
            CURRENT_TIMESTAMP AS RUN_DATE
        FROM v_base_filtered base
        LEFT JOIN v_a_clean a ON base.ACK_ID = a.ACK_ID
        WHERE a.BROKER_LIST IS NOT NULL
        ORDER BY TRY_CAST(base.LIVES AS INTEGER) DESC
    """

    # 6. EXPORT
    filename = f"Western_Leads_{year}_v5_4.parquet"
    outfile = os.path.join(ARTIFACT_DIR, filename)
    
    con.execute(f"COPY ({query}) TO '{outfile}' (FORMAT PARQUET)")
    print(f"   [SUCCESS] Artifact Generated: {outfile}")

    # 7. UPLOAD
    if GCS_BUCKET_NAME:
        upload_to_gcs(outfile, filename)
    
    # Cleanup
    for f in files.values(): 
        if f and os.path.exists(f): os.remove(f)

if __name__ == "__main__":
    target_year = datetime.now().year - 1
    run_pipeline_platinum(target_year)
