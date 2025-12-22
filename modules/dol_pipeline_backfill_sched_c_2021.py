"""
MODULE: AG_DOL_PIPELINE_BROKER_INTEL_SCH_C_2021
OBJECTIVE: Generate 'Western_Broker_Map_SchedC_2021.parquet' using Schedule C Part 1 Item 1 (Providers).
VERSION: 1.0 (2021 Pivot / Schedule C / Found Provider Names)
"""

import duckdb
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import zipfile
import os
import csv

# ==========================================
# CONFIGURATION
# ==========================================
TARGET_LIMIT = 500000 # National Scope
MIN_LIVES = 100
ARTIFACT_DIR = os.getenv("ARTIFACT_STORAGE_PATH", "./Scout_Data_Artifacts")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

# 2021 URLS (Part 1 Item 1 Discovery)
URL_MAP_2021 = {
    'BASE': "https://askebsa.dol.gov/FOIA%20Files/2021/Latest/F_5500_2021_Latest.zip",
    'SCH_C_P1': "https://askebsa.dol.gov/FOIA%20Files/2021/Latest/F_SCH_C_PART1_ITEM1_2021_Latest.zip"
}

# SCHEMA
SCHEMA_CONTRACT = {
    'BASE': {
        'ACK_ID': ['ACK_ID'],
        'EIN': ['SPONS_DFE_EIN', 'EIN'],
        'EMPLOYER_NAME': ['SPONS_DF_NAME', 'PLAN_NAME', 'SPONSOR_NAME'],
        'STATE': ['SPONS_DFE_MAIL_US_STATE', 'SPONS_US_STATE', 'US_STATE', 'STATE'],
        'LIVES': ['TOT_PARTCP_BOY_CNT', 'TOT_PARTCP_CNT']
    },
    'SCH_C_P1': {
        'ACK_ID': ['ACK_ID'],
        'BROKER_NAME': ['PROVIDER_ELIGIBLE_NAME', 'SRVC_PROV_NAME'],
        'BROKER_CITY': ['PROVIDER_ELIGIBLE_US_CITY', 'SRVC_PROV_US_CITY'],
        'BROKER_STATE': ['PROVIDER_ELIGIBLE_US_STATE', 'SRVC_PROV_US_STATE'],
        # Service Codes might be in Part 1 Item 2 (Service Codes), but Part 1 Item 1 often implies eligibility.
        # Wait, the Smoke Test header list for Part 1 Item 1 did NOT list Service Codes.
        # It listed Name, Address, EIN.
        # Part 1 Item 2 likely links codes to rows? Or maybe just simple filtering on Name is enough for now.
        # User Instruction: "Filter for SEQUOIA, HUB, ALLIANT, etc." (Keyword Filter).
        # We will use primary keyword filter on BROKER_NAME.
    }
}

TARGET_BROKERS = ["SEQUOIA", "HUB", "ALLIANT", "MERCER", "GALLAGHER", "AON", "WTW", "WILLIS", "LOCKTON", "MARSH", "USI"]

# ==========================================
# PHASE 1: INGESTION
# ==========================================
def create_session():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s

def download_and_extract(dataset_key, output_folder):
    url = URL_MAP_2021[dataset_key]
    zip_filename = f"raw_{dataset_key}_2021.zip"
    zip_path = os.path.join(output_folder, zip_filename)
    os.makedirs(output_folder, exist_ok=True)
    
    # Always allow re-download if small or missing
    if not os.path.exists(zip_path):
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
            # Picking largest CSV
            candidates = [n for n in z.infolist() if n.filename.lower().endswith('.csv') and 'layout' not in n.filename.lower()]
            if candidates:
                best = max(candidates, key=lambda x: x.file_size)
                z.extract(best, output_folder)
                best_file = os.path.join(output_folder, best.filename)
    except Exception as e:
        print(f"   [ERROR] Extraction failed for {dataset_key}: {e}")
    
    return best_file

# ==========================================
# PHASE 2: PIPELINE LOGIC
# ==========================================

def norm_ack(x):
    """Normalize ACK_ID."""
    s = "" if x is None else str(x)
    s = s.strip()
    if s.endswith(".0"): 
        s = s[:-2]
    return s

def resolve_header_select(csv_path, dataset_type):
    with open(csv_path, 'r', encoding='cp1252', errors='ignore') as f:
        actual = [h.upper().strip() for h in next(csv.reader(f))]
        print(f"   [DEBUG_HEADERS] {dataset_type}: {actual}")

    contract = SCHEMA_CONTRACT[dataset_type]
    select_parts = []
    for canonical, options in contract.items():
        found = next((opt for opt in options if opt in actual), None)
        if found:
            if canonical == 'ACK_ID':
                select_parts.append(f'norm_ack("{found}") AS {canonical}')
            else:
                select_parts.append(f'"{found}" AS {canonical}')
        else:
             if any(k in canonical for k in ['CITY', 'STATE', 'ZIP']):
                 select_parts.append(f"NULL AS {canonical}") 
    return ", ".join(select_parts) if select_parts else None

def run_broker_intel():
    print("AG STATUS: STARTING BROKER INTEL MAP (SCH_C 2021 PIVOT)")
    
    files = {k: download_and_extract(k, ARTIFACT_DIR) for k in URL_MAP_2021.keys()}
    if not files['BASE'] or not files['SCH_C_P1']:
        print("   [STOP] Missing files.")
        return

    con = duckdb.connect(database=":memory:")
    con.create_function("norm_ack", norm_ack, return_type="VARCHAR")

    for dtype, path in files.items():
        select = resolve_header_select(path, dtype)
        if select:
            con.execute(f"CREATE OR REPLACE VIEW v_{dtype.lower()} AS SELECT {select} FROM read_csv_auto('{path}', ignore_errors=True)")
            
            # DIAGNOSTICS
            count = con.execute(f"SELECT COUNT(*) FROM v_{dtype.lower()}").fetchone()[0]
            print(f"   [DIAGNOSTIC] {dtype} Row Count: {count}")
    
    print("   [OPTIMIZE] Filtering for Target Employers...")
    con.execute(f"""
        CREATE OR REPLACE VIEW v_base_filtered AS 
        SELECT * FROM v_base 
        WHERE TRY_CAST(LIVES AS INTEGER) >= {MIN_LIVES}
        ORDER BY TRY_CAST(LIVES AS INTEGER) DESC
        LIMIT {TARGET_LIMIT}
    """)

    # Keyword Filter Construction
    keyword_clauses = " OR ".join([f"upper(BROKER_NAME) LIKE '%{k}%'" for k in TARGET_BROKERS])

    print("   [PROCESSING] Filtering Schedule C for Target Brokers...")
    con.execute(f"""
        CREATE OR REPLACE VIEW v_c_filtered AS
        SELECT 
            ACK_ID,
            upper(BROKER_NAME) as BROKER_NAME,
            BROKER_CITY,
            BROKER_STATE
        FROM v_sch_c_p1
        WHERE ({keyword_clauses})
    """)
    
    c_count = con.execute("SELECT COUNT(*) FROM v_c_filtered").fetchone()[0]
    print(f"   [DEBUG] Filtered Provider Count (Target Brokers): {c_count}")

    # JOIN
    join_count = con.execute("SELECT COUNT(*) FROM v_base_filtered b INNER JOIN v_c_filtered c ON b.ACK_ID = c.ACK_ID").fetchone()[0]
    print(f"   [DIAGNOSTIC] JOIN OVERLAP: {join_count}")

    query = """
        SELECT 
            c.BROKER_NAME AS TARGET_BROKER_FIRM,
            c.BROKER_CITY AS TARGET_BROKER_CITY,
            c.BROKER_STATE AS TARGET_BROKER_STATE,

            base.EMPLOYER_NAME AS CLIENT_ACCOUNT_NAME,
            base.STATE AS CLIENT_STATE,
            base.LIVES AS CLIENT_LIVES,
            'None Listed' AS CURRENT_TPAS,
            
            '2021 Sched C Map' AS SOURCE,
            CURRENT_TIMESTAMP AS RUN_DATE
        FROM v_base_filtered base
        INNER JOIN v_c_filtered c ON base.ACK_ID = c.ACK_ID
        ORDER BY c.BROKER_NAME, TRY_CAST(base.LIVES AS INTEGER) DESC
    """

    filename = "Western_Broker_Map_SchedC_2021.parquet"
    outfile = os.path.join(ARTIFACT_DIR, filename)
    con.execute(f"COPY ({query}) TO '{outfile}' (FORMAT PARQUET)")
    print(f"   [SUCCESS] Broker Map Generated: {outfile}")

if __name__ == "__main__":
    run_broker_intel()
