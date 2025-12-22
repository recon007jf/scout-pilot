import pandas as pd
import re
import os
import sys

# --- CONFIGURATION (AG ADAPTED) ---
BASE_PATH = "/Users/josephlf/.gemini/antigravity/scratch/backend/Scout_Data_Artifacts"
INPUT_FILE = os.path.join(BASE_PATH, 'Pilot_50_For_Clay-8-1_Enriched.csv')
OUTPUT_FILE = os.path.join(BASE_PATH, 'Pilot_50_Ready_To_Send.csv')

def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def clean_sponsor_name(name):
    if not isinstance(name, str):
        return ""
    s = name.strip()

    # Remove only very common trailing noise, do not remove meaningful words like "Insurance"
    # Keep this conservative for the pilot.
    s = re.sub(r'\b(health\s*&\s*welfare|health\s+and\s+welfare|benefit(s)?|welfare)\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\b(plan|wrap|trust|fund)\b', '', s, flags=re.IGNORECASE)

    # Remove common legal suffixes
    s = re.sub(r'\b(inc|inc\.|llc|l\.l\.c\.|corp|corp\.|co|co\.|ltd|ltd\.)\b', '', s, flags=re.IGNORECASE)

    # Cleanup whitespace and punctuation
    s = re.sub(r'\s+', ' ', s).strip()
    s = s.strip(' -–—,;:')

    return s

def first_name(full_name):
    if not isinstance(full_name, str) or not full_name.strip():
        return "there"
    name = full_name.strip()

    # Handle "Last, First"
    if ',' in name:
        parts = [p.strip() for p in name.split(',') if p.strip()]
        if len(parts) >= 2:
            name = parts[1]

    # Remove common titles
    name = re.sub(r'^(dr\.|mr\.|mrs\.|ms\.)\s+', '', name, flags=re.IGNORECASE)

    # First token only
    return name.split()[0]

def build_subject(sponsor_clean):
    # Safest subject for deliverability: sponsor only
    return sponsor_clean if sponsor_clean else "Quick question"

def build_body(fname, sponsor_clean):
    # Reply-bait, minimal claims, single question
    # Anchors to a filing fact without overstating.
    sponsor_phrase = sponsor_clean if sponsor_clean else "this plan"
    return (
        f"Hi {fname},\n\n"
        f"Saw you listed as broker of record on the {sponsor_phrase} filing.\n"
        f"Are you still the right person to talk to for the renewal?\n\n"
        f"Best,\n"
        f"Andrew"
    )

def run():
    if not os.path.exists(INPUT_FILE):
        print(f"CRITICAL: Input file not found: {INPUT_FILE}")
        return

    df = pd.read_csv(INPUT_FILE)
    print(f"Loaded rows: {len(df):,}")

    # Detect columns (Clay exports vary)
    broker_name_col = pick_col(df, ['Broker Name', 'Broker_Name', 'BrokerName', 'Contact Full Name', 'Full Name', 'Name'])
    email_col = pick_col(df, ['Email', 'Work Email', 'Work_Email', 'Broker_Email', 'email'])
    sponsor_col = pick_col(df, ['Plan Sponsor', 'Plan_Sponsor', 'Sponsor', 'Sponsor Name', 'Account Name', 'Employer'])
    firm_col = pick_col(df, ['Firm', 'Broker_Firm_Norm', 'Broker Firm', 'Company Name', 'Company'])
    title_col = pick_col(df, ['Title', 'Job Title', 'Broker_Title', 'Contact Job Title', 'title'])
    # Updated to include your specific file's header
    linkedin_col = pick_col(df, ['LinkedIn URL', 'LinkedIn', 'linkedin_url', 'Person Linkedin Url', 'Linkedin', 'Linkedin - Links', 'Url - Experience'])

    missing_essentials = [x for x in [('Broker Name', broker_name_col), ('Email', email_col), ('Plan Sponsor', sponsor_col)] if x[1] is None]
    if missing_essentials:
        print("CRITICAL: Missing required columns:")
        for label, col in missing_essentials:
            print(f"  - {label} (not found)")
        print(f"Available columns: {list(df.columns)}")
        return

    # Clean essentials
    df[broker_name_col] = df[broker_name_col].fillna('').astype(str).str.strip()
    df[email_col] = df[email_col].fillna('').astype(str).str.strip()
    df[sponsor_col] = df[sponsor_col].fillna('').astype(str).str.strip()

    # Filter rows we can actually send
    sendable = df[
        (df[broker_name_col].str.len() > 2) &
        (df[email_col].str.len() > 5) &
        (df[sponsor_col].str.len() > 2)
    ].copy()

    skipped = len(df) - len(sendable)
    print(f"Sendable rows: {len(sendable):,}")
    print(f"Skipped rows (missing essentials): {skipped:,}")

    # Pilot status: honest signal, not fake certainty
    def status_row(row):
        has_title = bool(title_col and str(row.get(title_col, '')).strip())
        
        # Robust check for linkedin URL presence
        val_li = str(row.get(linkedin_col, '')).lower().strip()
        # Some Clay exports put "None" or "No Profile Found" in text
        has_linkedin = bool(linkedin_col and val_li and val_li not in ['nan', 'none', '❌ no profile found', ''])

        if has_linkedin and has_title:
            return "Verified Signal (LinkedIn + Title)"
        if has_linkedin:
            return "Verified Signal (LinkedIn)"
        if has_title:
            return "Weak Signal (Title only)"
        return "Unverified (No LinkedIn/Title)"

    sendable['Pilot_Status'] = sendable.apply(status_row, axis=1)

    # Draft generation
    def draft_row(row):
        fname = first_name(row.get(broker_name_col, ''))
        sponsor_clean = clean_sponsor_name(row.get(sponsor_col, ''))
        subj = build_subject(sponsor_clean)
        body = build_body(fname, sponsor_clean)
        return pd.Series([subj, body])

    sendable[['Draft_Subject', 'Draft_Body']] = sendable.apply(draft_row, axis=1)

    # Output columns
    out_cols = [
        broker_name_col, email_col, 'Pilot_Status',
        'Draft_Subject', 'Draft_Body',
        sponsor_col
    ]
    if firm_col:
        out_cols.append(firm_col)
    if title_col:
        out_cols.append(title_col)
    if linkedin_col:
        out_cols.append(linkedin_col)

    # Rename to stable headers for downstream use
    rename_map = {
        broker_name_col: 'Broker Name',
        email_col: 'Email',
        sponsor_col: 'Plan Sponsor'
    }
    if firm_col:
        rename_map[firm_col] = 'Firm'
    if title_col:
        rename_map[title_col] = 'Title'
    if linkedin_col:
        rename_map[linkedin_col] = 'LinkedIn URL'

    out_df = sendable[out_cols].rename(columns=rename_map)
    out_df.to_csv(OUTPUT_FILE, index=False)

    print(f"SUCCESS: {OUTPUT_FILE} generated.")
    print(out_df['Pilot_Status'].value_counts(dropna=False))

if __name__ == "__main__":
    run()
