import pandas as pd
import os

INPUT_FILE = "backend/data/input/f_5500_2023_latest.csv"

def main():
    if not os.path.exists(INPUT_FILE):
        print("File not found.")
        return

    print(f"Inspecting {INPUT_FILE}...")
    # Load just the relevant columns
    candidates = [
        'FUNDING_INSURANCE_IND', 
        'FUNDING_SEC412_IND', 
        'FUNDING_TRUST_IND', 
        'FUNDING_GEN_ASSET_IND',
        'TYPE_WELFARE_BNFT_CODE'
    ]
    
    try:
        df = pd.read_csv(INPUT_FILE, nrows=500, usecols=lambda x: x in candidates or x == 'SPONSOR_DFE_NAME')
        
        print("\n--- VALUE COUNTS ---")
        for col in candidates:
            if col in df.columns:
                print(f"\nCol: {col}")
                print(df[col].value_counts(dropna=False))
            else:
                print(f"\nCol: {col} NOT FOUND")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
