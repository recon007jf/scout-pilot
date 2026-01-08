import json
from app.services.territory_service import TerritoryService

def main():
    service = TerritoryService()
    print("🗺️ Fetching Territory Points...")
    points = service.get_territory_points()
    
    output_file = "territory_data.json"
    with open(output_file, 'w') as f:
        json.dump(points, f, indent=2)
        
    print(f"✅ Data dumped to {output_file}")
    print(f"Total records: {len(points)}")
    
    # helper for V0
    print("\n--- SAMPLE RECORD ---")
    if points.get("accounts"):
        print(json.dumps(points["accounts"][0], indent=2))
    else:
        print("[] (No geocoded data found yet - requires Server Key run)")

if __name__ == "__main__":
    main()
