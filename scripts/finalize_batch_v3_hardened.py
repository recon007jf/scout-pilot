import pandas as pd
import os
import re
import difflib

# --- CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
INPUT_DIR = os.path.join(PROJECT_ROOT, "backend", "data", "input")
ARTIFACTS_DIR = os.path.join(PROJECT_ROOT, "artifacts")

# INPUT: Original fused file (Clean Slate)
TARGET_CSV = os.path.join(ARTIFACTS_DIR, "Scout_Fused_ANDREW_WEST_20251231.csv")
# OUTPUT: Final V3 Definitive
OUTPUT_CSV = os.path.join(ARTIFACTS_DIR, "Scout_Fused_ANDREW_WEST_20251231_V3_HARDENED.csv")

DOL_5500_PATH = os.path.join(INPUT_DIR, "f_5500_2023_latest.csv")
DOL_SCH_A_PATH = os.path.join(INPUT_DIR, "F_SCH_A_2023_latest.csv")

def normalize_key(text):
    if not isinstance(text, str): return ""
    return re.sub(r'[^a-zA-Z0-9]', '', text.upper())

def normalize_ein(x):
    if pd.isna(x) or x == "": return None
    s = re.sub(r'\D', '', str(x))
    if not s: return None
    return s.zfill(9)

def score_plan_suitability(plan_name):
    """Rank plans: Health/Welfare > Generic > 401k/Pension."""
    p = str(plan_name).upper()
    score = 0
    if any(x in p for x in ['HEALTH', 'WELFARE', 'MEDICAL', 'BENEFIT']): score += 50
    if any(x in p for x in ['401', 'PENSION', 'RETIREMENT', 'SAVINGS']): score -= 1000
    return score

class DOLDefinitiveLookup:
    def __init__(self):
        self.sponsor_data = {} 
        self.stop_loss_eins = set()
        self._load_5500()
        self._load_sch_a()

    def _load_5500(self):
        print("Loading DOL 5500 (Definitive Mode - Indicator Patch)...")
        if not os.path.exists(DOL_5500_PATH): 
            print("CRITICAL: 5500 file not found.")
            return

        try:
            # 1. Inspect Candidate Columns
            # We know from probe they are FUNDING_*_IND
            # Map: ColName -> Code
            col_map = {
                'FUNDING_INSURANCE_IND': '1',
                'FUNDING_SEC412_IND': '2',
                'FUNDING_TRUST_IND': '3',
                'FUNDING_GEN_ASSET_IND': '5'
            }
            
            chunk_iter = pd.read_csv(DOL_5500_PATH, iterator=True, chunksize=20000, low_memory=False)
            
            for chunk in chunk_iter:
                chunk.columns = [c.upper() for c in chunk.columns]
                
                name_col = next((c for c in chunk.columns if 'SPONSOR' in c and 'NAME' in c), None)
                plan_col = next((c for c in chunk.columns if 'PLAN' in c and 'NAME' in c), None)
                ein_col  = next((c for c in chunk.columns if 'EIN' in c), None)
                
                if not name_col or not plan_col: continue

                # Pre-calculate codes for the chunk to speed up
                # fillna(0) ensures clean boolean checks
                for col in col_map:
                    if col not in chunk.columns: chunk[col] = 0
                    chunk[col] = chunk[col].fillna(0)

                for _, row in chunk.iterrows():
                    nm = normalize_key(str(row[name_col]))
                    if not nm: continue
                    
                    plan_name = str(row[plan_col])
                    score = score_plan_suitability(plan_name)
                    
                    if score > -500:
                        if nm not in self.sponsor_data: self.sponsor_data[nm] = []
                        
                        # Reconstruct Code Set
                        codes = set()
                        for col, code in col_map.items():
                            val = row[col]
                            # Check for 1.0, 1, '1', 'Y', 'True' - be robust
                            if str(val).upper() in ['1', '1.0', 'Y', 'TRUE', 'YES']:
                                codes.add(code)
                        
                        self.sponsor_data[nm].append({
                            'Plan': plan_name,
                            'EIN': normalize_ein(row[ein_col]) if ein_col else None,
                            'Codes': codes,
                            'Score': score
                        })
                        
        except Exception as e: print(f"Error 5500: {e}")

    def _load_sch_a(self):
        print("Loading DOL Sch A...")
        if not os.path.exists(DOL_SCH_A_PATH): return
        try:
            chunk_iter = pd.read_csv(DOL_SCH_A_PATH, iterator=True, chunksize=20000, low_memory=False)
            for chunk in chunk_iter:
                chunk.columns = [c.upper() for c in chunk.columns]
                ein_col = next((c for c in chunk.columns if 'EIN' in c), None)
                if not ein_col: continue
                for _, row in chunk.iterrows():
                    ein = normalize_ein(row[ein_col])
                    if not ein: continue
                    row_str = " ".join([str(v) for v in row.values]).lower()
                    if "stop loss" in row_str or "stop-loss" in row_str:
                        self.stop_loss_eins.add(ein)
        except Exception as e: print(f"Error Sch A: {e}")

    def lookup(self, sponsor_name):
        k = normalize_key(sponsor_name)
        candidates = self.sponsor_data.get(k)
        
        if not candidates:
            fuzzy = difflib.get_close_matches(k, self.sponsor_data.keys(), n=1, cutoff=0.92)
            if fuzzy: candidates = self.sponsor_data.get(fuzzy[0])
        
        if not candidates: return None
        
        best = max(candidates, key=lambda x: x['Score'])
        codes = best['Codes']
        
        # Tier 1: Stop Loss
        if best['EIN'] and best['EIN'] in self.stop_loss_eins:
             return {
                'Plan_Name': best['Plan'], 'Status': "Self-Funded", 
                'Confidence': "High", 'Evidence': "DOL Schedule A Stop-Loss",
                'Source': "STOP_LOSS"
            }
        
        # Tier 2: Funding Codes
        has_ins = '1' in codes
        has_self = any(x in codes for x in ['2', '3', '5'])
        
        # Logic Change: "Mixed" stays "Likely Self-Funded" but notes source
        if has_self and has_ins:
             return {
                'Plan_Name': best['Plan'], 'Status': "Likely Self-Funded", 
                'Confidence': "Medium", 'Evidence': f"DOL Funding Code Mixed ({''.join(sorted(codes))})",
                'Source': "DOL_CODE_MIXED"
            }
        elif has_self:
             return {
                'Plan_Name': best['Plan'], 'Status': "Likely Self-Funded", 
                'Confidence': "Medium", 'Evidence': f"DOL Funding Code ({''.join(sorted(codes))})",
                'Source': "DOL_CODE_SELF"
            }
        elif has_ins and len(codes) == 1:
             # STRICT: If only code 1, it's Fully Insured
             return {
                'Plan_Name': best['Plan'], 'Status': "Likely Fully Insured", 
                'Confidence': "Medium", 'Evidence': "DOL Funding Code (Insurance Only)",
                'Source': "DOL_CODE_INS"
            }
            
        return {'Plan_Name': best['Plan'], 'Status': None, 'Confidence': None, 'Evidence': None, 'Source': None}

def main():
    if not os.path.exists(TARGET_CSV): print("Missing Target CSV"); return
    print("--- V3 DEFINITIVE ENGINE START ---")
    
    dol = DOLDefinitiveLookup()
    df = pd.read_csv(TARGET_CSV)
    
    for c in ['Plan_Name', 'Funding_Status_Est', 'Funding_Confidence', 'Funding_Evidence', 'Funding_Source']:
        df[c] = "Unknown"

    updated = 0
    for idx, row in df.iterrows():
        sponsor = row.get('Primary_Client')
        if not sponsor: continue
        
        intel = dol.lookup(sponsor)
        if intel:
            df.at[idx, 'Plan_Name'] = intel['Plan_Name']
            if intel['Status']:
                df.at[idx, 'Funding_Status_Est'] = intel['Status']
                df.at[idx, 'Funding_Confidence'] = intel['Confidence']
                df.at[idx, 'Funding_Evidence'] = intel['Evidence']
                df.at[idx, 'Funding_Source'] = intel['Source']
                updated += 1
            else:
                # Tier 3 (Matched Plan, No Code)
                lives = int(float(str(row.get('Lives', 0)).strip() or 0))
                if lives >= 500:
                    df.at[idx, 'Funding_Status_Est'] = "Likely Self-Funded"
                    df.at[idx, 'Funding_Confidence'] = "Low"
                    df.at[idx, 'Funding_Evidence'] = "Size-based prior (Lives >= 500)"
                    df.at[idx, 'Funding_Source'] = "LIVES_FALLBACK"
                else:
                    df.at[idx, 'Funding_Status_Est'] = "Likely Fully Insured"
                    df.at[idx, 'Funding_Confidence'] = "Low"
                    df.at[idx, 'Funding_Evidence'] = "Size-based prior (Lives < 500)"
                    df.at[idx, 'Funding_Source'] = "LIVES_FALLBACK"
        else:
            # Tier 3 (No Match)
            lives = int(float(str(row.get('Lives', 0)).strip() or 0))
            if lives >= 500:
                df.at[idx, 'Funding_Status_Est'] = "Likely Self-Funded"
                df.at[idx, 'Funding_Confidence'] = "Low"
                df.at[idx, 'Funding_Evidence'] = "Size-based prior (Lives >= 500)"
                df.at[idx, 'Funding_Source'] = "LIVES_FALLBACK"
            else:
                df.at[idx, 'Funding_Status_Est'] = "Likely Fully Insured"
                df.at[idx, 'Funding_Confidence'] = "Low"
                df.at[idx, 'Funding_Evidence'] = "Size-based prior (Lives < 500)"
                df.at[idx, 'Funding_Source'] = "LIVES_FALLBACK"

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"V3 Definitive Complete. Updated {updated} rows with DOL Intelligence. Saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
