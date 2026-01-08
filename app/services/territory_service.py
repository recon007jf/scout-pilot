import os
import sqlite3
import hashlib
import time
import requests
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB_PATH = "leads_pilot.db"
GOOGLE_MAPS_SERVER_KEY = os.getenv("GOOGLE_MAPS_SERVER_KEY")

class TerritoryService:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path

    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def get_territory_points(self, filters=None):
        """
        Returns data contract for UI:
        { id, company, contact, location: {lat, lng}, status, revenue, lastContact }
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Query tailored to actual leads_pilot schema
        query = """
            SELECT 
                rowid as id,
                "employer_name" as company,
                "broker_human_name" as contact,
                geo_lat,
                geo_lng,
                "verification_status" as status,
                "assets_amount" as revenue,
                "created_at" as lastContact,
                "state"
            FROM leads_pilot
            WHERE geo_lat IS NOT NULL AND geo_lng IS NOT NULL
        """
        
        # Apply filters if needed (stub for now)
        if filters:
            pass # TODO: Add logic
            
        cursor.execute(query)
        rows = cursor.fetchall()
        
        
        results = []
        for row in rows:
            # Safe defaults
            raw_status = row['status'] if row['status'] else 'prospect'
            # Map simplified status logic
            status = 'prospect'
            if raw_status.lower() in ['verified', 'active']:
                status = 'active'
            elif raw_status.lower() in ['churned', 'risk']:
                status = 'at-risk'
                
            revenue = row['revenue'] if row['revenue'] else 0
            
            results.append({
                "id": str(row['id']),
                "company": row['company'] or "Unknown Firm",
                "contact": row['contact'] or "Unknown Contact",
                "title": "Decision Maker", # Stub, not in simplified DB
                "location": {
                    "lat": row['geo_lat'],
                    "lng": row['geo_lng']
                },
                "address": row['state'] or "",
                "status": status,
                "revenue": revenue,
                "lastContact": row['lastContact'] or "",
                "region": row['state'] # Using state as region for now
            })
            
        conn.close()
        return {"accounts": results}

    def geocode_lead(self, lead_id):
        """
        Fetches coordinates for a specific lead_id.
        Logic:
        1. Get address from DB.
        2. Hash address.
        3. Check if hash matches geo_address_hash (skip if same).
        4. Call Google API.
        5. Update DB.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Adjusted for actual schema: We only have 'state' reliably, maybe 'employer_name'
        cursor.execute('SELECT "employer_name", "state", geo_address_hash FROM leads_pilot WHERE rowid = ?', (lead_id,))
        row = cursor.fetchone()
        
        if not row:
            conn.close()
            return {"success": False, "error": "Lead not found"}

        # Construct Address
        # We will Geocode "Employer Name, State" to get a rough location, or just "State" if name missing
        parts = [row['employer_name'], row['state']]
        clean_parts = [p for p in parts if p and str(p).strip()]
        
        if not clean_parts:
             # Not enough info
             self._update_error(conn, lead_id, "Insufficient address data")
             conn.close()
             return {"success": False, "error": "Insufficient address data"}
             
        address_string = ", ".join([str(p) for p in clean_parts])
        
        # Check Cache
        current_hash = hashlib.md5(address_string.encode()).hexdigest()
        if row['geo_address_hash'] == current_hash:
            conn.close()
            return {"success": True, "cached": True}

        # Call API
        if not GOOGLE_MAPS_SERVER_KEY:
            # Non-blocking failure - just log and return
            print(f"⚠️  Skipping Lead {lead_id}: GOOGLE_MAPS_SERVER_KEY not found in environment.")
            return {"success": False, "error": "Missing API Key"}
            
        try:
            url = "https://maps.googleapis.com/maps/api/geocode/json"
            params = {
                "address": address_string,
                "key": GOOGLE_MAPS_SERVER_KEY
            }
            response = requests.get(url, params=params)
            data = response.json()
            
            if data['status'] == 'OK':
                result = data['results'][0]
                lat = result['geometry']['location']['lat']
                lng = result['geometry']['location']['lng']
                precision = result['geometry']['location_type']
                
                # Update DB
                update_sql = """
                    UPDATE leads_pilot SET
                        geo_lat = ?,
                        geo_lng = ?,
                        geo_precision = ?,
                        geo_source = 'google',
                        geo_last_geocoded_at = ?,
                        geo_address_hash = ?,
                        geo_error = NULL
                    WHERE rowid = ?
                """
                cursor.execute(update_sql, (lat, lng, precision, datetime.now(), current_hash, lead_id))
                conn.commit()
                conn.close()
                return {"success": True, "lat": lat, "lng": lng}
            else:
                error_msg = f"Google API Error: {data['status']}"
                self._update_error(conn, lead_id, error_msg)
                conn.close()
                return {"success": False, "error": error_msg}

        except Exception as e:
            self._update_error(conn, lead_id, str(e))
            conn.close()
            return {"success": False, "error": str(e)}

    def _update_error(self, conn, lead_id, error_msg):
        cursor = conn.cursor()
        cursor.execute("UPDATE leads_pilot SET geo_error = ? WHERE rowid = ?", (error_msg, lead_id))
        conn.commit()

    def bulk_geocode_pending(self, limit=50):
        """
        Geocodes rows that have no lat/lng and no error, or where hash changed.
        Simple version: Just target NULLS for now.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Select candidates: Missing lat OR missing hash (new/changed)
        # PRIORITIZE records that haven't failed recently
        query = """
            SELECT rowid 
            FROM leads_pilot 
            WHERE (geo_lat IS NULL OR geo_address_hash IS NULL)
            AND (geo_error IS NULL OR geo_error = '')
            LIMIT ?
        """
        cursor.execute(query, (limit,))
        rows = cursor.fetchall()
        conn.close()
        
        results = {"processed": 0, "success": 0, "errors": 0}
        
        print(f"🌍 Starting batch geocoding for {len(rows)} leads...")
        
        for row in rows:
            res = self.geocode_lead(row[0])
            results["processed"] += 1
            if res.get("success"):
                results["success"] += 1
            else:
                results["errors"] += 1
                print(f"⚠️ Lead {row[0]} failed: {res.get('error')}")
            
            # Rate limit politeness
            time.sleep(0.1)
            
        return results

if __name__ == "__main__":
    # Test run
    service = TerritoryService()
    print("Running bulk geocode...")
    stats = service.bulk_geocode_pending(limit=5)
    print("Stats:", stats)
    
    print("Fetching points...")
    points = service.get_territory_points()
    print(f"Found {len(points)} valid points.")
    if len(points) > 0:
        print("Sample:", points[0])
