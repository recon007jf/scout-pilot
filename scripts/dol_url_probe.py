"""
MODULE: DOL_URL_PROBE
OBJECTIVE: Verify existence of 2024 'Part' files and 2023 'All Fields' files.
"""
import requests

CANDIDATES = [
    # 1. 2024 "Latest" (Baseline - Known to exist)
    "https://askebsa.dol.gov/FOIA%20Files/2024/Latest/F_5500_2024_Latest.zip",
    "https://askebsa.dol.gov/FOIA%20Files/2024/Latest/F_SCH_A_2024_Latest.zip",
    "https://askebsa.dol.gov/FOIA%20Files/2024/Latest/F_SCH_C_2024_Latest.zip",

    # 2. 2024 Rumored "Part/Child" Zips (Testing existence)
    "https://askebsa.dol.gov/FOIA%20Files/2024/Latest/F_SCH_A_PART1_2024_Latest.zip",
    "https://askebsa.dol.gov/FOIA%20Files/2024/Latest/F_SCH_C_PART1_ITEM1_2024_Latest.zip",
    "https://askebsa.dol.gov/FOIA%20Files/2024/Data/F_SCH_A_PART1_2024.zip",
    "https://askebsa.dol.gov/FOIA%20Files/2024/Data/F_SCH_C_PART1_ITEM1_2024.zip",

    # 3. 2023 "All Fields" / Data Folder (Testing Backfill Strategy)
    # Testing lowercase vs uppercase naming conventions
    "https://askebsa.dol.gov/FOIA%20Files/2023/Data/f_5500_2023.zip",
    "https://askebsa.dol.gov/FOIA%20Files/2023/Data/f_sch_a_2023.zip",
    "https://askebsa.dol.gov/FOIA%20Files/2023/Data/f_sch_c_2023.zip",
    "https://askebsa.dol.gov/FOIA%20Files/2023/Data/F_5500_2023.zip",
    "https://askebsa.dol.gov/FOIA%20Files/2023/Data/F_SCH_A_2023.zip",
    "https://askebsa.dol.gov/FOIA%20Files/2023/Data/F_SCH_C_2023.zip",
]

def probe(url: str):
    try:
        # HEAD request checks metadata without downloading the file
        r = requests.head(url, timeout=30, allow_redirects=True, headers={"User-Agent":"Mozilla/5.0"})
        status = r.status_code
        length = r.headers.get("Content-Length", "unknown")
        
        # Calculate MB for readability
        if length != "unknown":
            size_str = f"{(int(length)/1024/1024):.2f} MB"
        else:
            size_str = "Unknown Size"
        
        # r.url captures the final URL if a redirect occurred
        print(f"[{status}] {size_str} | {r.url}")
    except Exception as e:
        print(f"[ERR] {e} | {url}")

if __name__ == "__main__":
    print("--- STARTING DOL URL PROBE ---")
    for u in CANDIDATES:
        probe(u)
