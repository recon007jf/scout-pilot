import pandas as pd
import os

# --- CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
ARTIFACTS_DIR = os.path.join(PROJECT_ROOT, "artifacts")

# Input: The file with DOL Plan Names (but maybe Unknown funding)
INPUT_CSV = os.path.join(ARTIFACTS_DIR, "Scout_Fused_ANDREW_WEST_20251231_ENRICHED.csv")
# Output: The Final V2 Deliverable
OUTPUT_CSV = os.path.join(ARTIFACTS_DIR, "Scout_Fused_ANDREW_WEST_20251231_ENRICHED_V2.csv")

# Preserved Statuses (Do not overwrite these)
ALLOWED_STATUSES = {"Likely Self-Funded", "Likely Fully Insured", "Self-Funded"}

def safe_int(x, default=0):
    try:
        if pd.isna(x): return default
        return int(float(str(x).strip()))
    except Exception: return default

def main():
    if not os.path.exists(INPUT_CSV):
        print(f"CRITICAL: Input file not found: {INPUT_CSV}")
        return

    print(f"Reading Enriched Batch: {os.path.basename(INPUT_CSV)}...")
    df = pd.read_csv(INPUT_CSV)

    # Defensive Initialization
    if "Funding_Status_Est" not in df.columns: df["Funding_Status_Est"] = "Unknown"
    if "Funding_Confidence" not in df.columns: df["Funding_Confidence"] = "Unknown"
    if "Funding_Evidence" not in df.columns: df["Funding_Evidence"] = "Unknown"

    updated_count = 0

    for idx, row in df.iterrows():
        current_status = str(row.get("Funding_Status_Est", "")).strip()

        # 1. Non-Destructive Check
        # If we already have a valid status (e.g. from Stop-Loss scan), skip.
        if current_status in ALLOWED_STATUSES:
            continue
        
        # Only overwrite if it looks like "Unknown" or blank
        if current_status.lower() not in {"", "unknown", "nan", "none"}:
            continue

        # 2. Apply Population-Informed Heuristic
        lives = safe_int(row.get("Lives", 0), default=0)

        if lives >= 500:
            df.at[idx, "Funding_Status_Est"] = "Likely Self-Funded"
            df.at[idx, "Funding_Confidence"] = "Low"
            # Explicitly label as a prior, using unicode >= symbol
            df.at[idx, "Funding_Evidence"] = "Size-based prior (Lives ≥500)"
        else:
            df.at[idx, "Funding_Status_Est"] = "Likely Fully Insured"
            df.at[idx, "Funding_Confidence"] = "Low"
            df.at[idx, "Funding_Evidence"] = "Size-based prior (Lives <500)"

        updated_count += 1

    # Save Final V2
    df.to_csv(OUTPUT_CSV, index=False)

    print("\n--- BATCH FINALIZED (V2) ---")
    print(f"Rows Processed: {len(df)}")
    print(f"Funding Status Updated: {updated_count}")
    print(f"Saved to: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
