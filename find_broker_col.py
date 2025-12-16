
import csv
import os

PATH = "Scout_Data_Artifacts/F_SCH_A_2022_Latest.csv"
# The extract function might have flattened it or preserved name.
# Let's find the csv in the artifact dir.
# In download_and_extract it saves as `output_folder/filename`.
# ZIP content was extracted.
# Let's list the directory first to find the filename.

print("Searching headers...")
for root, dirs, files in os.walk("Scout_Data_Artifacts"):
    for f in files:
        if "SCH_A" in f and f.endswith(".csv"):
            full = os.path.join(root, f)
            print(f"Inspecting: {full}")
            try:
                with open(full, 'r', encoding='cp1252', errors='ignore') as csvfile:
                    reader = csv.reader(csvfile)
                    headers = next(reader)
                    print(f"Total Headers: {len(headers)}")
                    
                    matches = [h for h in headers if any(x in h.upper() for x in ['BROKER', 'FIRM', 'AGENT', 'NAME', 'PRODUCER'])]
                    print(f"Potential Candidates: {matches}")
            except Exception as e:
                print(f"Error: {e}")
