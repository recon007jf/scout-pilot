from modules.attribution import AttributionEngine
import logging

# Setup Logging
logging.basicConfig(level=logging.ERROR)

def test_v2():
    print("🌊 Testing Hybrid V2 Attribution (Deep Discovery with Gemini)...")
    engine = AttributionEngine()
    
    # CASE 1: Validation Check
    print("\n--- TEST 1: Internal Match + Validation ---")
    res1 = engine.find_attribution("Generic Co", "CA", "Gallagher", "Glendale", "CA")
    print(f"Result: {res1['name']} | Score: {res1['confidence_score']} ({res1['confidence_label']})")
    print(f"Method: {res1['method']}")
    
    # CASE 2: Deep Discovery (Testing if Gemini or Heuristic kicks in)
    print("\n--- TEST 2: Deep Discovery (Gemini Integration) ---")
    # "Tech Corp" + "Unknown Firm" won't trigger internal match
    # It will trigger naive mock data in utils.serper if not using real SERP
    res2 = engine.find_attribution("Tech Corp", "WA", "Unknown Firm", "Seattle", "WA")
    
    print(f"Result: {res2['name']} | Score: {res2['confidence_score']} ({res2['confidence_label']})")
    print(f"Method: {res2['method']}")
    print(f"Notes: {res2.get('notes')}")
    
    if res2['source'] == "External Discovery":
        print("✅ PASS: External Discovery returned a result.")
        if "Gemini" in res2['method']:
            print("🚀 SUCCESS: Gemini AI was used for analysis!")
        else:
            print("⚠️ NOTE: Gemini AI was NOT used (Fallback to Heuristic). Check Creds/Install.")
    else:
        print("❌ FAIL: Discovery Failed.")

if __name__ == "__main__":
    test_v2()
