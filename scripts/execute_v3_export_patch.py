import pandas as pd
import numpy as np
import os

print("=== V3 EXPORT PATCH: FINAL REBUILD ===")

# 1. Load System Master (The Source of Truth)
sys_file = 'artifacts/Master_Hunting_List_Production_v3_SYSTEM.csv'
if not os.path.exists(sys_file):
    # Fallback search
    if os.path.exists('Master_Hunting_List_Production_v3_SYSTEM.csv'):
        sys_file = 'Master_Hunting_List_Production_v3_SYSTEM.csv'
    else:
        raise FileNotFoundError("CRITICAL: System Master not found. Cannot patch.")

df_sys = pd.read_csv(sys_file, low_memory=False)
print(f"Loaded System Master: {len(df_sys)} rows")

if df_sys.empty:
    print("! WARNING: System file has 0 rows. Creating empty Andrew export.")
    df_andrew = pd.DataFrame(columns=['Broker_Firm', 'Contact_Name', 'Email', 'Plan_Name']) 
else:
    # 2. Define Explicit Mapping (System -> Andrew)
    # Target_Column : System_Source_Column
    column_map = {
        'Broker_Firm': 'Target_Firm',
        'Contact_Name': 'Broker_Name',         # The fused contact name
        'Title': 'Job_Title',
        # Email handled separately for fallback logic
        'LinkedIn': 'LinkedIn_URL',
        'Client_Firm': 'Primary_Client',
        'Plan_Name': 'Plan_Name',
        'Lives': 'Lives',
        'Broker_State': 'firm_state',
        'Funding_Status': 'Funding_Status_Est',
        'Funding_Confidence': 'Funding_Confidence',
        'Sales_Angle': 'Sales_Angle',
        'Data_Source': 'Data_Provenance'
    }

    # 3. Create Andrew View
    df_andrew = pd.DataFrame()

    for target_col, sys_col in column_map.items():
        if sys_col in df_sys.columns:
            df_andrew[target_col] = df_sys[sys_col]
        else:
            # Warn but don't crash, fill empty
            if len(df_sys) > 0: # Only warn if we expected data
                # print(f"  ! WARNING: System column '{sys_col}' missing. Filling '{target_col}' with empty.") # Muted to reduce noise if common
                pass
            df_andrew[target_col] = ""

    # 4. CRITICAL FIX: Email Fallback Logic
    # Preference: Work_Email > Email > Empty
    
    # Get Work_Email (Primary)
    if 'Work_Email' in df_sys.columns:
        work_email = df_sys['Work_Email'].astype(str).replace('nan', '').str.strip()
    else:
        work_email = pd.Series([''] * len(df_sys))
        
    # Get Email (Fallback)
    if 'Email' in df_sys.columns:
        fallback_email = df_sys['Email'].astype(str).replace('nan', '').str.strip()
    else:
        fallback_email = pd.Series([''] * len(df_sys))
        
    # Apply Logic: If work_email is not empty, use it; else use fallback
    df_andrew['Email'] = np.where(work_email != '', work_email, fallback_email)
    
    print(f"  -> Email Fallback Applied. (Using Work_Email where available)")

# 5. Sanity Check & Save
if len(df_andrew) > 0:
    print("\nVerifying Critical Columns (Non-Empty Counts):")
    critical_cols = ['Contact_Name', 'Email', 'Plan_Name', 'Broker_Firm']
    for c in critical_cols:
        if c in df_andrew.columns:
            non_empty = df_andrew[c].notna() & (df_andrew[c].astype(str).str.strip() != '')
            count = non_empty.sum()
            pct = count / len(df_andrew)
            print(f"  - {c}: {count} / {len(df_andrew)} ({pct:.1%})")
        else:
            print(f"  - {c}: MISSING")

output_file = 'artifacts/Master_Hunting_List_Production_v3_ANDREW.csv'
if not os.path.exists('artifacts'): os.makedirs('artifacts')

df_andrew.to_csv(output_file, index=False)
print(f"\nSUCCESS: Regenerated {output_file}")
