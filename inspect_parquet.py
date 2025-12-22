
import duckdb
import os
import glob

def inspect_artifact():
    # Find the latest Western Leads parquet file
    files = glob.glob("./Scout_Data_Artifacts/Western_Leads_*.parquet")
    if not files:
        print("No Parquet artifact found!")
        return

    latest_file = max(files, key=os.path.getmtime)
    print(f"Analyzing Artifact: {latest_file}")
    
    size_mb = os.path.getsize(latest_file) / (1024 * 1024)
    print(f"File Size: {size_mb:.2f} MB")

    con = duckdb.connect()
    con.execute(f"CREATE VIEW leads AS SELECT * FROM read_parquet('{latest_file}')")
    
    # Row Count
    total_rows = con.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    print(f"Total Rows: {total_rows}")

    # Unique EINs
    unique_eins = con.execute("SELECT COUNT(DISTINCT EIN) FROM leads").fetchone()[0]
    print(f"Unique EINs: {unique_eins}")

    # Broker Coverage
    sw_brokers = con.execute("SELECT COUNT(*) FROM leads WHERE BROKERS != 'Unknown'").fetchone()[0]
    print(f"Plans with Brokers: {sw_brokers} ({sw_brokers/total_rows*100:.1f}%)")

    # TPA Coverage (Should be 0/Low)
    sw_tpas = con.execute("SELECT COUNT(*) FROM leads WHERE TPAS != 'None Listed'").fetchone()[0]
    print(f"Plans with TPAS: {sw_tpas} ({sw_tpas/total_rows*100:.1f}%)")

    print("\n--- SAMPLE ROWS ---")
    results = con.execute("SELECT * FROM leads LIMIT 5").fetchall()
    headers = [d[0] for d in con.description]
    print(headers)
    for r in results:
        print(r)

if __name__ == "__main__":
    inspect_artifact()
