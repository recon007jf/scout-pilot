import pandas as pd
import os
import glob
import re

# TASK: SILVER ETL - HEALTH PLANS
# GOAL: Standardize Form 5500 Bronze -> Silver "Master Health Plan Table"
# SOURCE: 10_bronze_parquet/F_5500/year=*/
# DEST:   20_silver_standardized/health_plans/

def etl_silver_health_plans():
    SPINE_ROOT = "/Users/josephlf/.gemini/antigravity/dol_spine"
    SOURCE_BASE = os.path.join(SPINE_ROOT, "10_bronze_parquet", "F_5500")
    DEST_BASE = os.path.join(SPINE_ROOT, "20_silver_standardized", "health_plans")
    
    print(f">>> SILVER ETL INITIATED: HEALTH PLANS")
    
    # 1. COLUMN MAPPING (Handle Drift Across Years)
    # We define the Target Name -> List of Candidate Source Names
    SCHEMA_MAP = {
        "ACK_ID": ["ACK_ID"],
        "FORM_PLAN_YEAR_BEGIN_DATE": ["FORM_PLAN_YEAR_BEGIN_DATE", "PLAN_EFF_DATE"],
        "EIN": ["SPONS_DFE_EIN", "SPONSOR_DFE_EIN", "EIN"],
        "PLAN_NUM": ["PLAN_NUM", "PLAN_NO", "PN", "SPONS_DFE_PN"], # Critical for joining
        "PLAN_NAME": ["PLAN_NAME", "PLAN_NM"],
        "SPONSOR_NAME": ["SPONSOR_DFE_NAME", "SPONSOR_NAME", "SPONS_DFE_NAME"],
        "SPONSOR_STATE": ["SPONS_DFE_MAIL_US_STATE", "SPONSOR_US_STATE", "SPONSOR_STATE"],
        "SPONSOR_ZIP": ["SPONS_DFE_MAIL_US_ZIP", "SPONSOR_US_ZIP", "SPONSOR_ZIP"],
        "WELFARE_CODE": ["TYPE_WELFARE_BNFT_CODE"],
        "LIVES": ["TOT_ACT_PARTCP_CNT", "TOT_PARTCP_CNT", "PARTICIPANTS", "TOT_PARTCP_BOY_CNT", "TOT_ACTIVE_PARTCP_CNT"], # AG FIX: Added variants
        # Funding Flags
        "INS_IND": ["FUNDING_INSURANCE_IND"],
        "TRUST_IND": ["FUNDING_TRUST_IND"],
        "GEN_IND": ["FUNDING_GEN_ASSET_IND"]
    }
    
    # Exclusions
    EXCLUDE_TERMS = re.compile(r"(401K|401\(K\)|PENSION|RETIREMENT|DEFINED BENEFIT|SAVINGS PLAN|PROFIT SHARING)", re.IGNORECASE)

    # Helper: Normalize Schema
    def standardize_schema(df, year):
        cols_upper = {c.upper(): c for c in df.columns}
        renamed = {}
        missing = []

        for target, candidates in SCHEMA_MAP.items():
            found_col = None
            for cand in candidates:
                if cand.upper() in cols_upper:
                    found_col = cols_upper[cand.upper()]
                    break
            
            if found_col:
                renamed[found_col] = target
            else:
                # Funding/Lives might be optional in bad data, but we flag it
                # if target not in ["LIVES", "INS_IND", "TRUST_IND", "GEN_IND"]:
                #      print(f"   [WARN] Year {year} missing critical col: {target}")
                missing.append(target)
        
        # Rename and Select
        df_clean = df.rename(columns=renamed)
        
        # Ensure all targets exist (fill NA if missing optional)
        for t in SCHEMA_MAP.keys():
            if t not in df_clean.columns:
                df_clean[t] = None
        
        # Preserve meta if it exists, or create? Bronze usually has it if ingested via my script
        meta_cols = [c for c in df.columns if c.startswith("_meta")]
        
        final_cols = list(SCHEMA_MAP.keys()) + meta_cols
        # Intersection to be safe
        final_cols = [c for c in final_cols if c in df_clean.columns]
        
        return df_clean[final_cols]

    # 2. ITERATE YEARS
    year_dirs = glob.glob(os.path.join(SOURCE_BASE, "year=*"))
    
    if not year_dirs:
        print("CRITICAL: No years found in Bronze Health Plans. Run Ingest first.")
        return

    for y_dir in sorted(year_dirs):
        year_str = os.path.basename(y_dir).split("=")[-1]
        print(f"\n... Processing Year: {year_str}")
        
        parquet_files = glob.glob(os.path.join(y_dir, "*.parquet"))
        
        for p_file in parquet_files:
            try:
                # Load Bronze
                df = pd.read_parquet(p_file)
                
                # Standardize
                df_std = standardize_schema(df, year_str)
                
                # FILTER 1: Health (4A)
                df_std["WELFARE_CODE"] = df_std["WELFARE_CODE"].astype(str)
                df_std["PLAN_NAME"] = df_std["PLAN_NAME"].astype(str)
                
                mask_health = (
                    df_std["WELFARE_CODE"].str.contains("4A", na=False) &
                    (~df_std["PLAN_NAME"].str.contains(EXCLUDE_TERMS, na=False))
                )
                df_health = df_std[mask_health].copy()
                
                if df_health.empty:
                    print(f"   [SKIP] No health plans found in {os.path.basename(p_file)}")
                    continue

                # CALCULATE 2: Self-Funded Status
                def check_sf(row):
                    try:
                        ins = int(float(row["INS_IND"])) if pd.notna(row["INS_IND"]) else 0
                        trust = int(float(row["TRUST_IND"])) if pd.notna(row["TRUST_IND"]) else 0
                        gen = int(float(row["GEN_IND"])) if pd.notna(row["GEN_IND"]) else 0
                        return ((trust == 1) or (gen == 1)) and not ((ins == 1) and (trust == 0) and (gen == 0))
                    except:
                        return False

                df_health["IS_SELF_FUNDED"] = df_health.apply(check_sf, axis=1)
                
                # CLEAN TYPES
                df_health["LIVES"] = pd.to_numeric(df_health["LIVES"], errors='coerce').fillna(0).astype(int)
                df_health["EIN"] = df_health["EIN"].astype(str).str.replace(r"\.0$", "", regex=True)
                df_health["PLAN_NUM"] = df_health["PLAN_NUM"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(3)

                # WRITE SILVER
                out_dir = os.path.join(DEST_BASE, f"year={year_str}")
                os.makedirs(out_dir, exist_ok=True)
                out_name = os.path.basename(p_file).replace(".parquet", "_silver.parquet")
                
                df_health.to_parquet(os.path.join(out_dir, out_name), index=False)
                
                print(f"   [OK] Wrote {len(df_health):,} Health Plans (Self-Funded: {sum(df_health['IS_SELF_FUNDED'])})")

            except Exception as e:
                print(f"   [ERROR] Failed file {os.path.basename(p_file)}: {e}")

    print("-" * 30)
    print("SILVER ETL (HEALTH PLANS) COMPLETE")
    print("-" * 30)

if __name__ == "__main__":
    etl_silver_health_plans()
