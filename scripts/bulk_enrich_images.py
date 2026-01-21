
import os
import requests
import json
import time
from app.main import get_service_db

SERPER_API_KEY = os.getenv("SERPER_API_KEY")

def fetch_image_from_serper(name, firm):
    query = f"{name} {firm} LinkedIn"
    print(f"   🔍 Query: {query}")
    
    url = "https://google.serper.dev/images"
    payload = json.dumps({"q": query})
    headers = {
        'X-API-KEY': SERPER_API_KEY,
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.post(url, headers=headers, data=payload, timeout=10)
        results = response.json()
        
        if 'images' in results and len(results['images']) > 0:
            # Prefers official LinkedIn media
            for img in results['images']:
                img_url = img.get('imageUrl')
                if "media.licdn.com/dms/image" in img_url:
                    return img_url
            # Fallback
            return results['images'][0].get('imageUrl')
    except Exception as e:
        print(f"   ❌ API Error: {e}")
    return None

def run_bulk_enrichment():
    db = get_service_db()
    
    # Get all targets with missing images
    print("🚀 Starting Bulk Image Enrichment...")
    res = db.table("target_brokers").select("id, full_name, firm").is_("profile_image", "null").limit(60).execute()
    targets = res.data
    
    print(f"📊 Found {len(targets)} candidates missing images.")
    
    count = 0
    for t in targets:
        count += 1
        print(f"\n[{count}/{len(targets)}] Processing: {t['full_name']} @ {t['firm']}")
        
        img_url = fetch_image_from_serper(t['full_name'], t['firm'])
        
        if img_url:
            db.table("target_brokers").update({"profile_image": img_url}).eq("id", t['id']).execute()
            print(f"   ✅ Saved: {img_url[:50]}...")
        else:
            print("   ⚠️ No image found.")
            
        # Rate limit safety
        time.sleep(0.5)

if __name__ == "__main__":
    run_bulk_enrichment()
