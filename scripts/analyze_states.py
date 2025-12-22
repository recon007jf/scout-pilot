
import duckdb
import os

CSV_PATH = "./Scout_Data_Artifacts/f_5500_2023_latest.csv"

def analyze():
    if not os.path.exists(CSV_PATH):
        print("CSV NOT FOUND")
        return

    con = duckdb.connect()
    # Read first to find the state column if name varies, but my pipeline debug said it found one.
    # I'll use the same logic as the pipeline
    con.execute(f"CREATE VIEW v_raw AS SELECT * FROM read_csv_auto('{CSV_PATH}', ignore_errors=True)")
    
    # Get columns
    cols = [c[0] for c in con.execute("DESCRIBE v_raw").fetchall()]
    print(f"Columns: {cols}")
    
    # Try to find the state column used
    state_candidates = ['SPONS_DFE_MAIL_US_STATE', 'SPONS_US_STATE', 'US_STATE', 'STATE']
    target_col = next((c for c in state_candidates if c in cols), None)
    
    if not target_col:
        print("CRITICAL: No State column found in CSV!")
        return

    print(f"Using State Col: {target_col}")
    
    # Count by State
    print("Top 20 States:")
    res = con.execute(f"SELECT {target_col}, COUNT(*) as cnt FROM v_raw GROUP BY {target_col} ORDER BY cnt DESC LIMIT 20").fetchall()
    for row in res:
        print(row)

    # Check for Western States specifically
    west = ['CA', 'OR', 'WA', 'ID', 'NV', 'AZ', 'NM', 'CO']
    print(f"\nChecking Western Block: {west}")
    res = con.execute(f"SELECT {target_col}, COUNT(*) FROM v_raw WHERE {target_col} IN {tuple(west)} GROUP BY {target_col}").fetchall()
    print(res)

if __name__ == "__main__":
    analyze()
