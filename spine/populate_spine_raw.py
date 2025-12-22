import os
import shutil
import re

# TASK: POPULATE SPINE RAW LAYER
# SOURCE: Scout_Data_Artifacts
# DEST: dol_spine/00_raw

RAW_ROOT = "/Users/josephlf/.gemini/antigravity/dol_spine/00_raw"
ARTIFACTS_DIR = "/Users/josephlf/.gemini/antigravity/scratch/Scout_Data_Artifacts"

MAPPING = {
    # 5500
    "f_5500_2021_latest.csv": ("F_5500", "2021"),
    "f_5500_2022_latest.csv": ("F_5500", "2022"),
    "f_5500_2023_latest.csv": ("F_5500", "2023"),
    
    # SCH C (Using PART 1 ITEM 1 for 2021 as confirmed "Gold")
    "F_SCH_C_PART1_ITEM1_2021_latest.csv": ("F_SCH_C_PART1_ITEM1", "2021"),
    
    # SCH C (2022/2023 - Assuming these are Part 1 Item 1 equivalent or Base? 
    # Actually, inspecting them in previous steps showed they might be Base or similar. 
    # For now, let's map them to a generic 'F_SCH_C' folder or specific if known.
    # The list showed 'F_SCH_C_2022_latest.csv' (3.5MB). This is small. 
    # Wait, 'F_SCH_C_PART1_ITEM1_2021' was 15MB. 
    # 2022/2023 might be the 'Base' Schedule C which lacks provider names (as discovered in Phase 1).
    # BUT, we must preserve them. I will map them to 'F_SCH_C_BASE' to be safe, or 'F_SCH_C' if ambiguous)
    "F_SCH_C_2022_latest.csv": ("F_SCH_C", "2022"),
    "F_SCH_C_2023_latest.csv": ("F_SCH_C", "2023"),
    
    # We also have ZIPs, but we care about the extracted CSVs for Bronze.
}

def populate_raw():
    print(f">>> MIGRATING ARTIFACTS TO SPINE RAW: {RAW_ROOT}")
    
    count = 0
    for filename, (subdir, year) in MAPPING.items():
        src = os.path.join(ARTIFACTS_DIR, filename)
        if os.path.exists(src):
            # AG FIX: Hive-style partitioning (year=YYYY)
            dest_dir = os.path.join(RAW_ROOT, subdir, f"year={year}")
            os.makedirs(dest_dir, exist_ok=True)
            
            dest = os.path.join(dest_dir, filename)
            if not os.path.exists(dest):
                print(f"  Copying {filename} -> {subdir}/{year}/...")
                shutil.copy2(src, dest)
                count += 1
            else:
                print(f"  [Exists] {filename}")
        else:
            print(f"  [Missing] {filename}")

    print(f"-" * 30)
    print(f"Migration Complete. Files Copied: {count}")

if __name__ == "__main__":
    populate_raw()
