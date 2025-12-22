"""
MODULE: AG_DOL_PIPELINE_BROKER_INTEL_2022
OBJECTIVE: Generate 'Western_Broker_Map_2022.parquet' (Pivot due to 2023 missing Broker Names)
VERSION: 1.0 (2022 Pivot / National Scope / Broker Name Restoration)
"""

import duckdb
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import zipfile
import os
import csv

# ==========================================
# CONFIGURATION (2022 PIVOT)
# ==========================================
TARGET_LIMIT = 500000 # National Scope - Increased limit significantly
MIN_LIVES = 100
ARTIFACT_DIR = os.getenv("ARTIFACT_STORAGE_PATH", "./Scout_Data_Artifacts")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

# 2022 URLs
URL_MAP_2022 = {
    'BASE': "https://askebsa.dol.gov/FOIA%20Files/2022/Latest/F_5500_2022_Latest.zip",
    'SCH_A': "https://askebsa.dol.gov/FOIA%20Files/2022/Latest/F_SCH_A_2022_Latest.zip",
    'SCH_C': "https://askebsa.dol.gov/FOIA%20Files/2022/Latest/F_SCH_C_2022_Latest.zip"
}

# BROKER-CENTRIC SCHEMA (Restoring INS_BROKER_FIRM_NAME)
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
        # PRIMARY TARGET: INS_BROKER_FIRM_NAME
        'BROKER_NAME': ['INS_BROKER_FIRM_NAME', 'BROKER_NAME'],
        'BROKER_ADDRESS': ['INS_BROKER_US_ADDRESS1', 'BROKER_ADDRESS1'], 
        'BROKER_CITY': ['INS_BROKER_US_CITY', 'BROKER_CITY'],
        'BROKER_STATE': ['INS_BROKER_US_STATE', 'BROKER_STATE']
    },
    'SCH_C': {
        'ACK_ID': ['ACK_ID'],
        'PROVIDER_NAME': ['SRVC_PROV_NAME', 'PROVIDER_NAME'],
        'SERVICE_CODES': ['SRVC_CODE_LIST', 'SERVICE_CODES'],
        'PAY_DIRECT': ['DIRECT_COMP_AMT'], 
        'PAY_INDIRECT': ['INDIRECT_COMP_AMT']
    }
}

TARGET_SERVICE_CODES = ['27', '28', '49'] 

# ==========================================
# PHASE 1: INGESTION
# ==========================================
def create_session():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s

def download_and_extract(dataset_key, output_folder):
    url = URL_MAP_2022[dataset_key]
    zip_filename = f"raw_{dataset_key}_2022.zip"
    zip_path = os.path.join(output_folder, zip_filename)
    os.makedirs(output_folder, exist_ok=True)
    
    if os.path.exists(zip_path) and os.path.getsize(zip_path) > 50000000:
        print(f"   [SKIP] Found existing large {dataset_key} (2022), skipping download.")
    else:
        print(f"   [INGEST] Streaming {dataset_key} from {url}...")
        try:
            with create_session().get(url, stream=True, timeout=300) as r:
                r.raise_for_status()
                with open(zip_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024*1024):
                        f.write(chunk)
        except Exception as e:
            print(f"   [ERROR] Failed to download {dataset_key}: {e}")
            return None

    best_file = None
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            candidates = [n for n in z.namelist() if n.lower().endswith('.csv') and 'layout' not in n.lower()]
            if candidates:
                best_file = max(candidates, key=lambda x: z.getinfo(x).file_size)
                z.extract(best_file, output_folder)
                best_file = os.path.join(output_folder, best_file)
    except Exception as e:
        print(f"   [ERROR] Extraction failed for {dataset_key}: {e}")
    
    return best_file

# ==========================================
# PHASE 2: PIPELINE LOGIC
# ==========================================

def norm_ack(x):
    """Normalize ACK_ID: Strip, remove trailing .0, preserve leading zeros."""
    s = "" if x is None else str(x)
    s = s.strip()
    if s.endswith(".0"): 
        s = s[:-2]
    return s

def resolve_header_select(csv_path, dataset_type):
    with open(csv_path, 'r', encoding='cp1252', errors='ignore') as f:
        actual_headers = [h.upper().strip() for h in next(csv.reader(f))]
        if dataset_type == 'SCH_A':
             print(f"   [DEBUG_HEADERS] SCH_A: {actual_headers}")

    contract = SCHEMA_CONTRACT[dataset_type]
    select_parts = []
    for canonical, options in contract.items():
        found = next((opt for opt in options if opt in actual_headers), None)
        if found:
            if canonical == 'ACK_ID':
                select_parts.append(f'norm_ack("{found}") AS {canonical}')
            else:
                select_parts.append(f'"{found}" AS {canonical}')
        elif canonical.startswith('PAY_'):
            select_parts.append(f"'0' AS {canonical}")
        else:
            if any(k in canonical for k in ['ADDRESS', 'CITY', 'STATE', 'ZIP']):
                 select_parts.append(f"NULL AS {canonical}") 
            else:
                # 2022 might have different headers, be robust
                pass
    return ", ".join(select_parts) if select_parts else None

def run_broker_intel():
    print("AG STATUS: STARTING BROKER INTEL MAP (2022 PIVOT)")
    
    files = {k: download_and_extract(k, ARTIFACT_DIR) for k in URL_MAP_2022.keys()}
    if not files['BASE']:
        print("   [STOP] Base file missing.")
        return

    con = duckdb.connect(database=":memory:")
    # Register Normalization UDF
    con.create_function("norm_ack", norm_ack, return_type="VARCHAR")

    for dtype, path in files.items():
        select = resolve_header_select(path, dtype) if path else None
        if select:
            con.execute(f"CREATE OR REPLACE VIEW v_{dtype.lower()} AS SELECT {select} FROM read_csv_auto('{path}', ignore_errors=True)")
            
            # DIAGNOSTICS
            if dtype in ['BASE', 'SCH_A']:
                count = con.execute(f"SELECT COUNT(ACK_ID) FROM v_{dtype.lower()} WHERE ACK_ID IS NOT NULL").fetchone()[0]
                sample = con.execute(f"SELECT ACK_ID FROM v_{dtype.lower()} LIMIT 3").fetchall()
                print(f"   [DIAGNOSTIC] {dtype} Non-Null ACK_IDs: {count}")
                print(f"   [DIAGNOSTIC] {dtype} ACK_ID Sample: {sample}")
        else:
            dummy_cols = ", ".join([f"NULL::VARCHAR AS {k}" for k in SCHEMA_CONTRACT[dtype].keys()])
            con.execute(f"CREATE OR REPLACE VIEW v_{dtype.lower()} AS SELECT {dummy_cols} WHERE 1=0")

    print("   [OPTIMIZE] Filtering for Target Employers (National Scope)...")
    # National Scope - Removed State Filter
    con.execute(f"""
        CREATE OR REPLACE VIEW v_base_filtered AS 
        SELECT * FROM v_base 
        WHERE TRY_CAST(LIVES AS INTEGER) >= {MIN_LIVES}
        ORDER BY TRY_CAST(LIVES AS INTEGER) DESC
        LIMIT {TARGET_LIMIT}
    """)
    print(f"   [DEBUG] v_base_filtered count: {con.execute('SELECT COUNT(*) FROM v_base_filtered').fetchone()[0]}")

    con.execute("""
        CREATE OR REPLACE VIEW v_a_clean AS
        SELECT 
            ACK_ID, 
            ANY_VALUE(BROKER_NAME) AS PRIMARY_BROKER_FIRM,
            ANY_VALUE(BROKER_CITY) AS BROKER_OFFICE_CITY,
            ANY_VALUE(BROKER_STATE) AS BROKER_OFFICE_STATE
        FROM v_sch_a 
        WHERE BROKER_NAME IS NOT NULL
        GROUP BY ACK_ID
    """)
    print(f"   [DEBUG] v_a_clean count: {con.execute('SELECT COUNT(*) FROM v_a_clean').fetchone()[0]}")

    clean_pay = "TRY_CAST(REPLACE(REPLACE(COALESCE(CAST({col} AS VARCHAR), '0'), '$', ''), ',', '') AS DOUBLE)"
    clean_codes = "list_filter(list_transform(str_split(replace(COALESCE(CAST(SERVICE_CODES AS VARCHAR), ''), ';', ','), ','), x->trim(x)), x->x!='')"
    code_check = " OR ".join([f"list_contains({clean_codes}, '{c}')" for c in TARGET_SERVICE_CODES])

    con.execute(f"""
        CREATE OR REPLACE VIEW v_c_clean AS
        SELECT 
            ACK_ID, 
            STRING_AGG(PROVIDER_NAME, ' | ' ORDER BY ({clean_pay.format(col='PAY_DIRECT')} + {clean_pay.format(col='PAY_INDIRECT')}) DESC) AS TPA_LIST
        FROM v_sch_c
        WHERE ({code_check}) AND PROVIDER_NAME IS NOT NULL
        GROUP BY ACK_ID
    """)
    print(f"   [DEBUG] v_c_clean count: {con.execute('SELECT COUNT(*) FROM v_c_clean').fetchone()[0]}")

    # JOIN DIAGNOSTICS
    join_count = con.execute("SELECT COUNT(*) FROM v_base_filtered b INNER JOIN v_a_clean a ON b.ACK_ID = a.ACK_ID").fetchone()[0]
    print(f"   [DIAGNOSTIC] JOIN OVERLAP (Base Filtered + Sch A): {join_count}")
    
    a_sample = con.execute("SELECT ACK_ID FROM v_a_clean LIMIT 3").fetchall()
    print(f"   [DIAGNOSTIC] v_a_clean ACK_ID Sample: {a_sample}")

    query = """
        SELECT 
            -- BROKER DATA (THE TARGET)
            COALESCE(a.PRIMARY_BROKER_FIRM, 'Unknown') AS TARGET_BROKER_FIRM,
            a.BROKER_OFFICE_CITY AS TARGET_BROKER_CITY,
            a.BROKER_OFFICE_STATE AS TARGET_BROKER_STATE,

            -- EMPLOYER DATA (THE INTEL/AMMO)
            base.EMPLOYER_NAME AS CLIENT_ACCOUNT_NAME,
            base.STATE AS CLIENT_STATE,
            base.LIVES AS CLIENT_LIVES,
            COALESCE(c.TPA_LIST, 'None Listed') AS CURRENT_TPAS,
            
            '2022 Broker Map (Pivot)' AS SOURCE,
            CURRENT_TIMESTAMP AS RUN_DATE
        FROM v_base_filtered base
        LEFT JOIN v_a_clean a ON base.ACK_ID = a.ACK_ID
        LEFT JOIN v_c_clean c ON base.ACK_ID = c.ACK_ID
        WHERE a.PRIMARY_BROKER_FIRM IS NOT NULL
        ORDER BY a.PRIMARY_BROKER_FIRM, TRY_CAST(base.LIVES AS INTEGER) DESC
    """

    filename = "Western_Broker_Map_2022.parquet"
    outfile = os.path.join(ARTIFACT_DIR, filename)
    con.execute(f"COPY ({query}) TO '{outfile}' (FORMAT PARQUET)")
    print(f"   [SUCCESS] Broker Map Generated: {outfile}")

    if GCS_BUCKET_NAME:
        try:
            from google.cloud import storage
            storage.Client().bucket(GCS_BUCKET_NAME).blob(filename).upload_from_filename(outfile)
            print("   [CLOUD] Uploaded to GCS")
        except Exception: pass

if __name__ == "__main__":
    run_broker_intel()
