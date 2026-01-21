import os
import sys
from app.core.image_proxy import ImageProxyEngine

def debug_images():
    engine = ImageProxyEngine()
    
    # Candidates from the screenshot that are missing images
    targets = [
        {"name": "Jennifer Ochs", "company": "Lockton Companies"},
        {"name": "Ricky G", "company": "USI Insurance Services"}, # "Ricky G" might be hard to match
        {"name": "Scott Wood", "company": "Alera Group"},
        {"name": "Grace Bennett", "company": "Alliant Insurance Services"}
    ]
    
    print("--- DEBUGGING IMAGE FETCH ---")
    for t in targets:
        print(f"\nTarget: {t['name']} @ {t['company']}")
        
        # 1. Simulate the Fetch
        try:
            # We want to see the RAW results from Serper if possible, but the engine methods shield us.
            # We'll call fetch_image and inspect the result.
            result = engine.fetch_image(t['name'], t['company'])
            print(f"  Result: {result}")
            
            img_url = result.get("imageUrl")
            if img_url:
                # 2. Simulate Verification
                print(f"  Verifying Accessibility for: {img_url}")
                is_accessible = engine.verify_accessibility(img_url)
                print(f"  Accessible? {is_accessible}")
            else:
                print("  [NULL] No URL returned by Engine.")
                
        except Exception as e:
            print(f"  [ERROR] {e}")

if __name__ == "__main__":
    debug_images()
