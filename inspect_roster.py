
import pandas as pd
import csv

def scan_roster():
    print("🔍 Scanning data/roster_master.csv for headers...")
    path = "data/roster_master.csv"
    
    try:
        # Read raw lines
        with open(path, 'r', encoding='latin-1') as f:
            lines = f.readlines()
            
        print(f"📄 Total Lines: {len(lines)}")
        
        # Scan first 50 lines for specific keywords
        keywords = ['Account', 'Name', 'Email', 'Region', 'Priority']
        
        header_idx = -1
        for i, line in enumerate(lines[:50]):
            clean_line = line.strip()
            # print(f"[{i}] {clean_line[:100]}...") # truncated
            
            # Check for matches
            match_count = sum(1 for k in keywords if k.lower() in clean_line.lower())
            if match_count >= 2:
                print(f"✅ FOUND Header Candidate at Line {i}: {clean_line}")
                header_idx = i
                break
                
        if header_idx != -1:
            # Try loading with pandas
            df = pd.read_csv(path, header=header_idx, encoding='latin-1')
            print("\n📊 First 5 Rows:")
            print(df[['Account', 'Name', 'Email', 'Priority Region']].head())
            print("\n✅ Columns found:", df.columns.tolist())
        else:
            print("❌ Start-of-Data not found via keyword scan.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    scan_roster()
