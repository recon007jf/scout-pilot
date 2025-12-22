# AG PACKAGE: select_pilot_50.py
#
# Instructions for AG:
# 1) Confirm INPUT_FILE points to the actual location of Leads_Shortlist_2023.csv
# 2) Run: python select_pilot_50.py
# 3) Expected output: backend/Scout_Data_Artifacts/Pilot_50_For_Clay.csv
#
# Notes (Why this is safer than the prior version):
# - Prevents the classic "nan" string problem during normalization and filtering
# - Ensures we truly pick the TOP 50 by confidence across both email and non-email rows
#   (the prior concat approach could accidentally prioritize low-confidence rows just because they had an email)
# - Creates Clay_Context_Prompt without any NaN string-concat crashes
# - Ensures output directory exists (no silent path failure)

import os
import pandas as pd

# --- CONFIGURATION ---
# AG: Using absolute paths for safety
BASE_PATH = "/Users/josephlf/.gemini/antigravity/scratch/backend/Scout_Data_Artifacts"
INPUT_FILE = os.path.join(BASE_PATH, "Leads_Shortlist_2023.csv")
OUTPUT_FILE = os.path.join(BASE_PATH, "Pilot_50_For_Clay.csv")
TARGET_ROWS = 50

REQUIRED_COLS = ["Broker_Name", "Broker_Firm_Norm"]
OPTIONAL_COLS = [
    "Broker_Email",
    "Plan_Sponsor",
    "Plan_Sponsor_City",
    "Match_Confidence",
]

def _norm_series(s: pd.Series) -> pd.Series:
    """
    Normalize a text column safely:
    - fillna("")
    - strip/lower
    - remove literal 'nan' that appears after astype(str)
    """
    return (
        s.fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .replace({"nan": ""})
    )

def generate_pilot_list() -> None:
    print("--- STARTING PILOT SELECTION ---")

    # 1) Validate input path
    if not os.path.exists(INPUT_FILE):
        print(f"CRITICAL ERROR: Could not find input file at: {INPUT_FILE}")
        return

    # 2) Load
    print(f"Loading data from: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)
    print(f"Total Matches Available: {len(df):,}")

    # AG FIX: Normalize column names to match script expectations
    # 2023 Pivot output 'sponsor_name' but script expects 'Plan_Sponsor'
    rename_map = {}
    if 'sponsor_name' in df.columns and 'Plan_Sponsor' not in df.columns:
        rename_map['sponsor_name'] = 'Plan_Sponsor'
    # We don't have Plan_Sponsor_City in the 2023 output yet, but we have contact_city (Broker City)
    # We will leave Plan_Sponsor_City as missing (will become "Unknown")
    
    if rename_map:
        print(f"Renaming columns to match schema: {rename_map}")
        df = df.rename(columns=rename_map)

    # 3) Column checks
    missing_required = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing_required:
        print(f"CRITICAL ERROR: Missing required columns: {missing_required}")
        print(f"Available columns: {list(df.columns)}")
        return

    # 4) Basic filtering: require non-empty name + firm (not just non-null)
    df = df.copy()
    df["__broker_name_norm"] = _norm_series(df["Broker_Name"])
    df["__broker_firm_norm"] = _norm_series(df["Broker_Firm_Norm"])

    df = df[(df["__broker_name_norm"] != "") & (df["__broker_firm_norm"] != "")].copy()

    # Email may or may not exist
    if "Broker_Email" in df.columns:
        df["__broker_email_norm"] = _norm_series(df["Broker_Email"])
    else:
        df["__broker_email_norm"] = ""

    # 5) Sorting: Match_Confidence desc if available, else keep input order
    if "Match_Confidence" in df.columns:
        # Coerce to numeric safely; unknowns become -1 so they sort last
        df["__confidence_num"] = pd.to_numeric(df["Match_Confidence"], errors="coerce").fillna(-1)
        df = df.sort_values(by="__confidence_num", ascending=False)
    else:
        print("WARNING: 'Match_Confidence' column not found. Using input order (no confidence sort).")
        df["__confidence_num"] = 0

    # 6) Deduplication (Production logic):
    # Walk the sorted rows once, keep the first occurrence for:
    # - email if present
    # - else (name+firm)
    # This guarantees the highest-confidence row wins for each unique human.
    seen_emails = set()
    seen_name_firm = set()
    picked_idx = []

    for idx, row in df.iterrows():
        email_key = row["__broker_email_norm"]
        name_firm_key = (row["__broker_name_norm"], row["__broker_firm_norm"])

        if email_key:
            if email_key in seen_emails:
                continue
            seen_emails.add(email_key)
            picked_idx.append(idx)
        else:
            if name_firm_key in seen_name_firm:
                continue
            seen_name_firm.add(name_firm_key)
            picked_idx.append(idx)

        if len(picked_idx) >= TARGET_ROWS:
            break

    pilot_df = df.loc[picked_idx].copy()
    print(f"Unique Brokers selected: {len(pilot_df):,} (target was {TARGET_ROWS})")

    # 7) Build Clay context prompt safely (always Series)
    # Ensure Series for concatenation even if column is missing/nan
    if "Plan_Sponsor" in pilot_df.columns:
        plan_sponsor = pilot_df["Plan_Sponsor"].fillna("Unknown").astype(str)
    else:
        plan_sponsor = pd.Series(["Unknown"] * len(pilot_df), index=pilot_df.index)

    if "Plan_Sponsor_City" in pilot_df.columns:
        plan_city = pilot_df["Plan_Sponsor_City"].fillna("Unknown").astype(str)
    else:
        plan_city = pd.Series(["Unknown"] * len(pilot_df), index=pilot_df.index)

    pilot_df["Clay_Context_Prompt"] = "Plan Sponsor: " + plan_sponsor + ". City: " + plan_city

    # 8) Output columns (only keep what exists)
    columns_to_keep = [
        "Broker_Name",
        "Broker_Firm_Norm",
        "Broker_Email",
        "Plan_Sponsor",
        "Plan_Sponsor_City",
        "Match_Confidence",
        "Clay_Context_Prompt",
    ]
    final_cols = [c for c in columns_to_keep if c in pilot_df.columns]
    pilot_df = pilot_df[final_cols].copy()

    # 9) Ensure output directory exists
    out_dir = os.path.dirname(OUTPUT_FILE)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    # 10) Export
    pilot_df.to_csv(OUTPUT_FILE, index=False)

    print("-" * 30)
    print(f"SUCCESS! Generated: {OUTPUT_FILE}")
    print(f"Rows: {len(pilot_df):,}")
    print(f"Columns: {final_cols}")
    print("-" * 30)

if __name__ == "__main__":
    generate_pilot_list()
