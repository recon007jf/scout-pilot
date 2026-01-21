
import os
import requests
import json
from app.main import get_service_db

# Targets to fix
TARGETS = [
    {"name": "Lee Sommars", "firm": "Lockton"}
]

SERPER_API_KEY = os.getenv("SERPER_API_KEY")

def fetch_image(name, firm):
    # More specific query
    query = f"{name} {firm} LinkedIn Profile"
    print(f"🔍 Searching: {query}")
    
    url = "https://google.serper.dev/images"
    payload = json.dumps({"q": query})
    headers = {
        'X-API-KEY': SERPER_API_KEY,
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.post(url, headers=headers, data=payload)
        results = response.json()
        
        if 'images' in results and len(results['images']) > 0:
            print(f"   Found {len(results['images'])} candidates.")
            
            # STRICT FILTER: Must be a profile photo
            for img in results['images']:
                img_url = img.get('imageUrl')
                if "media.licdn.com/dms/image" in img_url and "profile-displayphoto" in img_url:
                    print(f"   ✅ Found Strict Profile Image: {img_url}")
                    return img_url
            
            # Relaxed Filter
            for img in results['images']:
                img_url = img.get('imageUrl')
                if "media.licdn.com/dms/image" in img_url:
                    print(f"   ⚠️ Fallback to generic media.licdn: {img_url}")
                    return img_url

    except Exception as e:
        print(f"   ❌ Error: {e}")
    return None

def run():
    db = get_service_db()
    
    for t in TARGETS:
        print(f"\nProcessing {t['name']}...")
        res = db.table("target_brokers").select("*").ilike("full_name", f"%{t['name']}%").execute()
        
        if not res.data:
            print("   ❌ Not found in DB")
            continue
            
        record = res.data[0]
        # Use actual firm from record for better search
        firm = record['firm']
        
        img_url = fetch_image(t['name'], firm)
        
        if img_url:
            db.table("target_brokers").update({"profile_image": img_url}).eq("id", record['id']).execute()
            print("   🎉 Database Updated.")
        else:
            print("   ❌ No image found.")

if __name__ == "__main__":
    run()
