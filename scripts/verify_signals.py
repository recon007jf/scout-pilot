import os
import sys
from pprint import pprint

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from app.core.signal_analyst import SignalAnalyst
from app.core.event_scout import EventScout

def verify():
    print("--- VERIFYING SIGNALS INTEL ---")
    
    # 1. Test Signal Analyst (Reactive)
    print("\n1. Testing SignalAnalyst (Serper -> Tavily -> LLM)...")
    analyst = SignalAnalyst()
    firm = "Lockton" 
    
    try:
        signals = analyst.scan_and_analyze(firm)
        print(f"   Buffer: Found {len(signals)} signals for {firm}.")
        for s in signals:
            print(f"   [SIGNAL] {s['signal_type']}: {s['title']}")
            print(f"            Analysis: {s['analysis']}")
    except Exception as e:
        print(f"   [FAIL] SignalAnalyst: {e}")

    # 2. Test Event Scout (Proactive)
    print("\n2. Testing EventScout (Location/Tribe/Executive)...")
    scout = EventScout()
    
    # Mock Candidates
    candidates = [
        # Match Geo
        {"full_name": "Denver CEO", "city": "Colorado Springs", "state": "CO", "firm": "Some Broker", "title": "CEO"},
        # Match Tribe
        {"full_name": "Rosetta Advisor", "city": "Chicago", "state": "IL", "firm": "Health Rosetta Advisors", "title": "VP"},
        # Match None
        {"full_name": "Random Guy", "city": "Nowhere", "state": "XY", "firm": "Generic", "title": "Associate"}
    ]
    
    for c in candidates:
        print(f"   Checking {c['full_name']}...")
        hooks = scout.check_events(c)
        if hooks:
            for h in hooks:
                print(f"     [HOOK] {h['match_reason']}: {h['hook_text']}")
        else:
            print("     [No Hooks]")

if __name__ == "__main__":
    verify()
