import json
import os
import re

# --- CONFIG ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
CACHE_FILE = os.path.join(PROJECT_ROOT, ".cache", "territory_resolution.json")

STATE_SUFFIXES = {
    "CA": "CALIFORNIA", "WA": "WASHINGTON", "OR": "OREGON", 
    "NV": "NEVADA", "AZ": "ARIZONA", "ID": "IDAHO", 
    "NM": "NEW MEXICO", "CO": "COLORADO"
}

def main():
    if not os.path.exists(CACHE_FILE):
        print("Cache file not found.")
        return
        
    print(f"Loading cache: {CACHE_FILE}")
    with open(CACHE_FILE, 'r') as f:
        cache = json.load(f)
        
    flipped = 0
    total = len(cache)
    
    for firm_key, data in cache.items():
        if data.get("firm_state_class") == "UNKNOWN":
            raw_name = data.get("firm_name_raw", "").upper()
            
            found_state = None
            
            # Check Suffixes (e.g. ", CA" or " CA")
            for code in STATE_SUFFIXES:
                # Regex: Word boundary or comma, then code, then end of string or space
                # e.g. "Hub - Phoenix, AZ" -> Match AZ
                if re.search(rf"[,\s]{code}\b", raw_name):
                    found_state = code
                    break
                    
            if found_state:
                print(f"  [PATCH] Flipping '{data['firm_name_raw']}' -> {found_state} (IN_TERRITORY)")
                data["firm_state"] = found_state
                data["firm_state_class"] = "IN_TERRITORY"
                # Use a specific method code so we know it was patched
                data["firm_state_method"] = "LOGIC_PATCH_SUFFIX" 
                data["firm_state_evidence"] = f"Explicit state code found in name: {found_state}"
                flipped += 1
                
    print(f"\n--- PATCH COMPLETE ---")
    print(f"Total Entries Scanned: {total}")
    print(f"Entries Flipped: {flipped}")
    
    if flipped > 0:
        with open(CACHE_FILE, 'w') as f:
            json.dump(cache, f)
        print("Cache updated successfully.")
    else:
        print("No changes needed.")

if __name__ == "__main__":
    main()
