import pandas as pd
import os
import glob

print("=== DEEP-DIVE FOLDER SCAN ===")

# 1. Recursive Search for ALL CSVs
all_files = []
for root, dirs, files in os.walk('.'):
    for f in files:
        if f.endswith('.csv') and 'Master_Hunting' not in f and 'Target_Hunting' not in f:
            all_files.append(os.path.join(root, f))

file_stats = []

# 2. Analyze Each File
for fpath in sorted(all_files):
    try:
        # Read Header & Row Count
        df = pd.read_csv(fpath, nrows=5, low_memory=False) # Sample
        row_count = sum(1 for _ in open(fpath, 'rb')) - 1 # Fast row count
        
        cols = [c.upper() for c in df.columns]
        col_str = " ".join(cols)
        
        # Classification Logic
        file_type = "UNKNOWN"
        if '5500' in fpath or 'ACK_ID' in col_str:
            if 'SCH_A' in fpath.upper() or 'SCHEDULE_A' in fpath.upper(): file_type = "DOL_SCH_A"
            elif 'SCH_C' in fpath.upper() or 'SCHEDULE_C' in fpath.upper(): file_type = "DOL_SCH_C"
            else: file_type = "DOL_5500 (SPINE)"
        elif 'BROKER_LOCATION_CACHE' in fpath.upper():
            file_type = "TERRITORY_CACHE"
        elif 'CONTACT' in col_str and 'EMAIL' in col_str:
            if row_count > 50000: file_type = "LEADS_FULL_UNIVERSE (GOLD)"
            else: file_type = "LEADS_PARTIAL/PILOT"
        
        file_stats.append({
            'Path': fpath,
            'Rows': row_count,
            'Type': file_type,
            'Key_Cols': [c for c in df.columns if 'NAME' in c.upper() or 'LIVES' in c.upper()][:3]
        })
    except Exception as e:
        print(f"Skipping {fpath}: {e}")

# 3. Print Final Source of Truth Table
df_report = pd.DataFrame(file_stats)
if not df_report.empty:
    print(df_report[['Type', 'Rows', 'Path']].sort_values(by='Rows', ascending=False).to_string())
    
    # Auto-Select Best Files
    gold_leads = df_report[df_report['Type'] == 'LEADS_FULL_UNIVERSE (GOLD)']['Path'].tolist()
    if gold_leads:
        print(f"\n🏆 CONFIRMED GOLDEN SOURCE: {gold_leads[0]}")
    else:
        print("\n⚠️ NO GOLDEN SOURCE FOUND (Using largest available file)")
else:
    print("No CSVs found.")
