import pandas as pd
import os
import re
import difflib

# --- CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))

INPUT_DIR = os.path.join(PROJECT_ROOT, "backend", "data", "input")
ARTIFACTS_DIR = os.path.join(PROJECT_ROOT, "artifacts")

# Target the specific file
TARGET_CSV = os.path.join(ARTIFACTS_DIR, "Scout_Fused_ANDREW_WEST_20251231.csv")

# DOL Paths (Canonical)
DOL_5500_PATH = os.path.join(INPUT_DIR, "f_5500_2023_latest.csv")
DOL_SCH_A_PATH = os.path.join(INPUT_DIR, "F_SCH_A_2023_latest.csv")

def normalize_key(text):
    if not isinstance(text, str): return ""
    return re.sub(r'[^a-zA-Z0-9]', '', text.upper())

def normalize_ein(x):
    """Ensures EIN is a 9-digit string."""
    if pd.isna(x) or x == "": return None
    s = re.sub(r'\D', '', str(x))
    if not s: return None
    return s.zfill(9)

class DOLLookup:
    def __init__(self):
        self.sponsor_map = {} # {NORM_NAME: {'Plan': ..., 'EIN': ...}}
        self.stop_loss_eins = set()
        self._load_5500()
        self._load_sch_a()

    def _load_5500(self):
        print(f"Loading DOL 5500 from {os.path.basename(DOL_5500_PATH)}...")
        if not os.path.exists(DOL_5500_PATH):
            print("CRITICAL: DOL 5500 file missing.")
            return

        try:
            chunk_iter = pd.read_csv(DOL_5500_PATH, iterator=True, chunksize=10000, low_memory=False)
            for chunk in chunk_iter:
                chunk.columns = [c.upper() for c in chunk.columns]
                
                name_col = next((c for c in chunk.columns if 'SPONSOR' in c and 'NAME' in c), None)
                plan_col = next((c for c in chunk.columns if 'PLAN' in c and 'NAME' in c), None)
                ein_col  = next((c for c in chunk.columns if 'EIN' in c), None)

                if not name_col or not plan_col: continue

                for _, row in chunk.iterrows():
                    nm = normalize_key(str(row[name_col]))
                    ein = normalize_ein(row[ein_col]) if ein_col else None
                    
                    if nm and nm not in self.sponsor_map:
                        self.sponsor_map[nm] = {
                            'Plan': row[plan_col],
                            'EIN': ein
                        }
        except Exception as e:
            print(f"Error loading 5500: {e}")

    def _load_sch_a(self):
        print(f"Loading DOL Sch A from {os.path.basename(DOL_SCH_A_PATH)}...")
        if not os.path.exists(DOL_SCH_A_PATH): return

        try:
            chunk_iter = pd.read_csv(DOL_SCH_A_PATH, iterator=True, chunksize=10000, low_memory=False)
            for chunk in chunk_iter:
                chunk.columns = [c.upper() for c in chunk.columns]
                ein_col = next((c for c in chunk.columns if 'EIN' in c), None)
                if not ein_col: continue

                for _, row in chunk.iterrows():
                    ein = normalize_ein(row[ein_col])
                    if not ein: continue
                    
                    # Heuristic: Scan row for Stop Loss keywords
                    row_str = " ".join([str(v) for v in row.values]).lower()
                    if "stop loss" in row_str or "stop-loss" in row_str or "stoploss" in row_str:
                        self.stop_loss_eins.add(ein)
        except Exception as e:
            print(f"Error loading Sch A: {e}")

    def lookup(self, sponsor_name):
        k = normalize_key(sponsor_name)
        data = self.sponsor_map.get(k)
        match_type = "EXACT"
        
        # Fuzzy Fallback
        if not data and self.sponsor_map:
            candidates = difflib.get_close_matches(k, self.sponsor_map.keys(), n=1, cutoff=0.92)
            if candidates:
                data = self.sponsor_map.get(candidates[0])
                match_type = "FUZZY_0.92"
        
        if not data: return None
        
        ein = normalize_ein(data.get('EIN'))
        has_sl = bool(ein and ein in self.stop_loss_eins)
        
        return {
            'Plan_Name': data['Plan'],
            'Has_Stop_Loss': has_sl,
            'Match_Type': match_type
        }

def main():
    if not os.path.exists(TARGET_CSV):
        print(f"CRITICAL: Target CSV not found: {TARGET_CSV}")
        return

    print("--- STARTING DOL ENRICHMENT PATCH (FINAL) ---")
    dol = DOLLookup()

    print(f"\nReading {os.path.basename(TARGET_CSV)}...")
    df = pd.read_csv(TARGET_CSV)
    
    # Defensive Initialization
    if 'Plan_Name' not in df.columns: df['Plan_Name'] = "Unknown"
    if 'DOL_Match_Type' not in df.columns: df['DOL_Match_Type'] = "Unknown"
    
    for col in ['Funding_Status_Est', 'Funding_Confidence', 'Funding_Evidence']:
        if col not in df.columns: df[col] = "Unknown"

    updated_count = 0
    stop_loss_count = 0

    for idx, row in df.iterrows():
        sponsor = row.get('Primary_Client')
        if not sponsor: continue

        intel = dol.lookup(sponsor)
        if intel:
            updated_count += 1
            
            # 1. Update Identity (Non-Destructive)
            current_plan = str(row.get('Plan_Name', '')).strip().lower()
            if current_plan in ['unknown', 'unknown plan', 'nan', '']:
                df.at[idx, 'Plan_Name'] = intel['Plan_Name']
                df.at[idx, 'DOL_Match_Type'] = intel['Match_Type']
            
            # 2. Update Funding (Only upgrade if Stop-Loss found)
            if intel['Has_Stop_Loss']:
                stop_loss_count += 1
                df.at[idx, 'Funding_Status_Est'] = "Self-Funded"
                df.at[idx, 'Funding_Confidence'] = "High"
                df.at[idx, 'Funding_Evidence'] = "DOL_SCH_A_STOP_LOSS"

    # Save
    out_file = TARGET_CSV.replace(".csv", "_ENRICHED.csv")
    df.to_csv(out_file, index=False)
    
    print(f"\n--- ENRICHMENT COMPLETE ---")
    print(f"Rows Processed: {len(df)}")
    print(f"Plans Identified: {updated_count}")
    print(f"Stop-Loss Confirmed: {stop_loss_count}")
    print(f"Saved to: {out_file}")

if __name__ == "__main__":
    main()
