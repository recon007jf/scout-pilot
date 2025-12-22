"""
FILE: run_handshake_2023.py
STATUS: FINAL PRODUCTION (Safety Gates Enabled)
PURPOSE: Join 2023 Employer Leads with 2025 Broker Contacts.
"""

import pandas as pd
import numpy as np
import os
import sys
import re

# --- CONFIGURATION (Adapted for AG Environment) ---
BASE_PATH = "/Users/josephlf/.gemini/antigravity/scratch/backend/Scout_Data_Artifacts"
LEADS_FILE = os.path.join(BASE_PATH, 'Western_Leads_2023_Platinum.parquet')
PART1_FILE = os.path.join(BASE_PATH, 'F_SCH_A_PART1_2023_latest.csv')
CONTACTS_FILE = os.path.join(BASE_PATH, 'pilot_inputs/AndrewWestRegion_2025.csv')
MATCH_OUTPUT_FILE = os.path.join(BASE_PATH, 'raw_handshake_matches.parquet')
REPORT_OUTPUT_FILE = os.path.join(BASE_PATH, 'broker_normalization_report.csv')

CONTACT_COL_MAP = {
    'Company Name': 'broker_firm_name',
    'Contact Full Name': 'contact_name',
    'Contact Job Title': 'contact_title',
    'Contact Email': 'contact_email',
    'Contact State': 'contact_state',
    'Contact City': 'contact_city'
}

def normalize_company(name):
    """
    Conservative Normalization. Only strips distinct legal suffixes at end of string.
    """
    if pd.isna(name): return ""
    name = str(name).lower().strip()
    
    # Regex for legal suffixes at the END of the string
    patterns = [
        r',?\s*inc\.?$', r',?\s*llc\.?$', r',?\s*ltd\.?$', r',?\s*corp\.?$', 
        r',?\s*corporation$', r',?\s*co\.$', r',?\s*company$'
    ]
    for pat in patterns:
        name = re.sub(pat, '', name, flags=re.IGNORECASE)
            
    return name.strip()

def main():
    print(f"[START] Handshake Protocol (Part 1 Correction) initiated...")
    
    # 1. LOAD DATA
    if not os.path.exists(LEADS_FILE):
        print(f"[ERROR] Lead file not found: {LEADS_FILE}")
        sys.exit(1)
        
    if not os.path.exists(PART1_FILE):
        print(f"[ERROR] Schedule A Part 1 file not found: {PART1_FILE}")
        sys.exit(1)

    try:
        leads_raw = pd.read_parquet(LEADS_FILE)
        # Load Part 1 (Line 2/3 Brokers) - CSV
        part1_raw = pd.read_csv(PART1_FILE, low_memory=False)
        contacts_raw = pd.read_csv(CONTACTS_FILE)
        print(f"[INFO] Loaded {len(leads_raw):,} Leads, {len(part1_raw):,} Part 1 Records, and {len(contacts_raw):,} Contacts.")
    except Exception as e:
        print(f"[ERROR] Data load failed: {e}")
        sys.exit(1)

    # 2. MERGE LEADS + PART 1 (To get Real Brokers)
    print("[INFO] Joining Leads (Plan Data) with Part 1 (Broker Data) via ACK_ID...")
    
    # Leads Parquet has: ACK_ID, EIN, EMPLOYER_NAME, STATE, LIVES...
    # Part 1 CSV has: ACK_ID, INS_BROKER_NAME...
    
    # We want to explode the leads: One Plan -> Many Brokers
    merged_leads = pd.merge(
        leads_raw,
        part1_raw[['ACK_ID', 'INS_BROKER_NAME']],
        on='ACK_ID',
        how='inner'
    )
    
    print(f"[INFO] Exploded to {len(merged_leads):,} Plan-Broker pairs.")
    
    # Rename for internal consistency
    leads_df = merged_leads.rename(columns={
        'EIN': 'ein',
        'EMPLOYER_NAME': 'sponsor_name',
        'INS_BROKER_NAME': 'broker_firm_name',
        'STATE': 'employer_state'
    }).copy()
    
    # Synthesize Keys
    leads_df['plan_number'] = '001'
    leads_df['plan_year'] = '2023-01-01'
    leads_df['plan_key'] = leads_df['ein'].astype(str) + "_001_2023"

    # 3. PREPARE CONTACTS
    missing_contacts = [c for c in CONTACT_COL_MAP.keys() if c not in contacts_raw.columns]
    if missing_contacts:
        print(f"[CRITICAL] Missing columns in Contacts File: {missing_contacts}")
        sys.exit(1)
    contacts_df = contacts_raw.rename(columns=CONTACT_COL_MAP)[list(CONTACT_COL_MAP.values())].copy()

    # 4. NORMALIZATION & MATCHING
    print("[INFO] Normalizing Broker Names...")
    leads_df['normalized_broker_name'] = leads_df['broker_firm_name'].apply(normalize_company)
    contacts_df['normalized_broker_name'] = contacts_df['broker_firm_name'].apply(normalize_company)

    print("[INFO] Generating Collision Audit...")
    lead_stats = leads_df.groupby('normalized_broker_name').agg(
        raw_names_list=('broker_firm_name', lambda x: list(x.unique())[:5]),
        unique_raw_count=('broker_firm_name', 'nunique'),
        count_plans=('plan_key', 'nunique')
    ).reset_index()

    contact_stats = contacts_df.groupby('normalized_broker_name').agg(
        contact_count=('normalized_broker_name', 'count')
    ).reset_index()

    audit = pd.merge(lead_stats, contact_stats, on='normalized_broker_name', how='left')
    audit['contact_count'] = audit['contact_count'].fillna(0).astype(int)
    audit['is_mapped'] = audit['contact_count'] > 0
    
    # Save Report
    audit.sort_values('count_plans', ascending=False).to_csv(REPORT_OUTPUT_FILE, index=False)
    
    print("\n" + "!"*50)
    print("TOP MATCHED BROKERS (Validation):")
    print(audit[audit['is_mapped']].sort_values('count_plans', ascending=False).head(10)[['normalized_broker_name', 'count_plans']])
    print("!"*50 + "\n")

    # 5. EXECUTE FINAL JOIN
    print("[INFO] Joining Plans to Humans...")
    final_matches = pd.merge(
        leads_df,
        contacts_df,
        on='normalized_broker_name',
        how='inner',
        suffixes=('_lead', '_contact')
    )
    
    # 6. SAVE
    output_cols = [
        'plan_key', 'ein', 'sponsor_name', 'employer_state',
        'broker_firm_name', 'normalized_broker_name',
        'contact_name', 'contact_title', 'contact_email', 'contact_city'
    ]
    # Handle column names if they changed during merge (broker_firm_name -> broker_firm_name_lead)
    if 'broker_firm_name_lead' in final_matches.columns:
        final_matches['broker_firm_name'] = final_matches['broker_firm_name_lead']

    print(f"[SUCCESS] Handshake Matched {len(final_matches):,} pairs.")
    final_matches[output_cols].to_parquet(MATCH_OUTPUT_FILE)
    print(f"[SAVED] {MATCH_OUTPUT_FILE}")

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
