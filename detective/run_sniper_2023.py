"""
FILE: run_sniper_2023.py
STATUS: FINAL PRODUCTION (Replication Mode)
PURPOSE: Score Matches and Select Top 3 Contacts per Plan+Broker Relationship.
"""

import pandas as pd
import os
import sys
import re

# --- CONFIGURATION (Adapted for AG Environment) ---
BASE_PATH = "/Users/josephlf/.gemini/antigravity/scratch/backend/Scout_Data_Artifacts"
INPUT_FILE = os.path.join(BASE_PATH, 'raw_handshake_matches.parquet')
OUTPUT_FILE = os.path.join(BASE_PATH, 'Leads_Shortlist_2023.csv')
BACKUP_FILE = os.path.join(BASE_PATH, 'Leads_Shortlist_2023_LOW_THRESH.csv')

def is_valid_email(email):
    if pd.isna(email): return False
    s = str(email).strip().lower()
    return len(s) > 5 and '@' in s and '.' in s

def calculate_2021_score(row):
    score = 0
    title = str(row.get('contact_title', '')).lower()
    email = row.get('contact_email')
    
    if not is_valid_email(email): return 0

    # 1. SENIORITY
    if any(x in title for x in ['president', 'principal', 'partner', 'owner']): score += 30
    elif any(x in title for x in ['executive', 'head of', 'chief']): score += 25
    elif any(x in title for x in ['director', 'vp', 'vice president']): score += 20
    elif 'senior' in title: score += 10
        
    # 2. DEPARTMENT
    if any(x in title for x in ['benefit', 'health', 'employee']): score += 40
    elif any(x in title for x in ['property', 'casualty', 'risk']): score -= 20
        
    return score

def main():
    print("[START] Sniper Scope 2023 (Replication Mode)...")
    
    if not os.path.exists(INPUT_FILE):
        print(f"[ERROR] Input file missing: {INPUT_FILE}")
        sys.exit(1)

    df = pd.read_parquet(INPUT_FILE)
    print(f"[INFO] Processing {len(df):,} raw pairs...")

    # 1. SCORE
    df['sniper_score'] = df.apply(calculate_2021_score, axis=1)

    # 2. DEBUG DISTRIBUTION
    print("\n[DEBUG] Score Distribution (Before Filter):")
    print(df['sniper_score'].value_counts().sort_index())
    print("-" * 30)

    # 3. FILTER (Threshold 50)
    qualified_df = df[df['sniper_score'] >= 50].copy()

    # 4. GROUP & RANK
    print("[INFO] Selecting Top 3 Humans per Relationship...")
    qualified_df.sort_values(
        ['plan_key', 'normalized_broker_name', 'sniper_score'],
        ascending=[True, True, False],
        inplace=True
    )
    shortlist = qualified_df.groupby(['plan_key', 'normalized_broker_name']).head(3)

    # 5. EXPORT
    output_cols = [
        'sponsor_name', 'ein', 'broker_firm_name', 'normalized_broker_name',
        'contact_name', 'contact_title', 'contact_email', 'contact_city',
        'sniper_score', 'plan_key'
    ]
    
    final_df = shortlist[output_cols].rename(columns={
        'broker_firm_name': 'Broker_Firm_Raw',
        'normalized_broker_name': 'Broker_Firm_Norm',
        'contact_name': 'Broker_Name',
        'contact_title': 'Broker_Title',
        'contact_email': 'Broker_Email',
        'sniper_score': 'Match_Confidence'
    })
    
    print(f"\n[RESULT] Final Shortlist Rows: {len(final_df):,}")
    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"[COMPLETE] Saved to {OUTPUT_FILE}")

    # 6. SAFETY BACKUP
    if len(final_df) < 200:
        print("\n[WARNING] Yield < 200. Generating backup (Threshold 30)...")
        low_thresh = df[df['sniper_score'] >= 30].sort_values(['plan_key', 'normalized_broker_name', 'sniper_score'], ascending=[True, True, False])
        backup = low_thresh.groupby(['plan_key', 'normalized_broker_name']).head(3)[output_cols]
        backup.to_csv(BACKUP_FILE, index=False)
        print(f"[BACKUP] Saved {len(backup):,} rows to {BACKUP_FILE}")

if __name__ == "__main__":
    main()
