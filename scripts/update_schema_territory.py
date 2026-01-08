import sqlite3

DB_PATH = "leads_pilot.db"

def migrate_schema():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    print("🔌 Connected to database.")
    
    columns_to_add = [
        ("geo_lat", "FLOAT"),
        ("geo_lng", "FLOAT"),
        ("geo_precision", "TEXT"),
        ("geo_source", "TEXT"),
        ("geo_last_geocoded_at", "DATETIME"),
        ("geo_error", "TEXT"),
        ("geo_address_hash", "TEXT")
    ]
    
    # Get existing columns
    c.execute("PRAGMA table_info(leads_pilot)")
    existing_cols = [row[1] for row in c.fetchall()]
    
    print(f"📊 Current columns: {len(existing_cols)}")
    
    for col_name, col_type in columns_to_add:
        if col_name not in existing_cols:
            print(f"➕ Adding column: {col_name} ({col_type})...")
            try:
                c.execute(f"ALTER TABLE leads_pilot ADD COLUMN {col_name} {col_type}")
            except sqlite3.OperationalError as e:
                print(f"⚠️ Error adding {col_name}: {e}")
        else:
            print(f"✅ Column {col_name} already exists.")
            
    conn.commit()
    conn.close()
    print("🚀 Schema migration complete.")

if __name__ == "__main__":
    migrate_schema()
