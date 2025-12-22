from modules.attribution import AttributionEngine

def test_waterfall():
    print("🌊 Testing Hybrid Attribution Waterfall...")
    engine = AttributionEngine()
    
    # CASE 1: INTERNAL LOOKUP (Golden Record)
    print("\n--- TEST 1: Internal Lookup (Gallagher - Glendale) ---")
    res1 = engine.find_attribution("Generic Employer", "CA", "Gallagher", "Glendale", "CA")
    print(f"Result: {res1['name']} | Source: {res1['source']}")
    
    if res1['name'] == "Neil Parton":
        print("✅ PASS: Correctly identified via Roster.")
    else:
        print("❌ FAIL: Did not find Neil Parton.")

    # CASE 2: FALLBACK TO TRIANGULATION
    print("\n--- TEST 2: Discovery Fallback (No Roster Match) ---")
    # Using 'Seattle Childrens' to trigger the Mock Serper response we set up earlier
    res2 = engine.find_attribution("Seattle Children's Hospital", "WA", "Some Unknown Firm", "Seattle", "WA")
    print(f"Result: {res2['name']} | Source: {res2['source']}")
    
    if "Triangulation" in res2['source']:
        print("✅ PASS: Fell back to Triangulation.")
    else:
        print("❌ FAIL: Did not fall back correctly.")

if __name__ == "__main__":
    test_waterfall()
