import zipfile
import csv
import io

SCHED_A_ZIP = "data/master_sched_c.zip"

print(f"inspecting {SCHED_A_ZIP}...")

try:
    with zipfile.ZipFile(SCHED_A_ZIP, 'r') as z:
        # Find first CSV
        csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]
        if not csv_files:
            print("No CSV found in Zip.")
            exit()
            
        csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]
        if not csv_files:
            print("No CSV found in Zip.")
            exit()
            
        print(f"Found {len(csv_files)} CSVs: {csv_files}")
        
        for target in csv_files:
            print(f"\nScanning: {target}")
            with z.open(target) as f:
                # Read first line
                wrapper = io.TextIOWrapper(f, encoding='utf-8', errors='replace')
                reader = csv.reader(wrapper)
                headers = next(reader)
                
                print(f"--- HEADERS ({target}) ---")
                for i, h in enumerate(headers):
                    print(f"{i}: {h}")
                
                 # Look for specific keywords
                print(f"--- MATCHES ({target}) ---")
                keywords = ['BROKER', 'AGENT', 'PRODUCER', 'FIRM', 'NAME', 'COMMISSION', 'PROVIDER']
                for k in keywords:
                    matches = [h for h in headers if k in h]
                    if matches: print(f"  '{k}': {matches}")

except Exception as e:
    print(f"Error: {e}")
