from app.services.territory_service import TerritoryService
import time

def main():
    service = TerritoryService()
    print("🚀 Starting Geocoding Backfill...")
    
    # Process in batches until done or error limit hit
    total_processed = 0
    while True:
        stats = service.bulk_geocode_pending(limit=20)
        total_processed += stats['processed']
        
        print(f"Batch Result: {stats}")
        
        if stats['processed'] == 0:
            print("✅ No more pending leads to geocode.")
            break
            
        if stats['errors'] > 10:
            print("⚠️ Too many errors in this batch. Stopping for safety.")
            break
            
        time.sleep(1) # Cool down between batches

    print(f"🏁 Backfill Complete. Total processed: {total_processed}")

if __name__ == "__main__":
    main()
