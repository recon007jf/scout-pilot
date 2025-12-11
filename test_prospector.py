from prospector_agent import extract_general_assets_leads
import json

def test_logic():
    print("🧪 Testing Prospector Logic with Dummy Data...")
    
    f5500 = "dummy_5500.csv"
    sched_a = "dummy_sched_a.csv"
    
    leads = extract_general_assets_leads(f5500, sched_a)
    
    # Test Verification
    from prospector_agent import verify_leads_batch
    leads = verify_leads_batch(leads)
    
    print("\n🔍 Results:")
    print(json.dumps(leads, indent=2))
    
    # Assertions
    if len(leads) == 1:
        lead = leads[0]
        if lead['Broker Name'] == "Sarah Jenkins" and lead['Client Name'] == "TechCorp Inc.":
            print("\n✅ SUCCESS: Correctly identified Sarah Jenkins at TechCorp (Self-Funded).")
        else:
            print("\n❌ FAIL: Identified wrong lead.")
    else:
        print(f"\n❌ FAIL: Expected 1 lead, found {len(leads)}.")

if __name__ == "__main__":
    test_logic()
