from modules.triangulation import TriangulationEngine
import sys

# MOCK OR REAL?
# It relies on utils.serper.search_google which checks env vars.
# We assume env var is set or it falls back to mock.

def test_engine():
    engine = TriangulationEngine()
    
    # Target: A large WA employer likely to have public signal
    target_employer = "Seattle Children's Hospital"
    target_state = "WA"
    
    print(f"🧪 Testing Triangulation for: {target_employer}...")
    
    signals, candidates = engine.gather_regional_signals(target_employer, target_state)
    
    print(f"\n📡 Signals Found: {len(signals)}")
    for s in signals:
        print(f"   - [{s['confidence']}] {s['source']}: {s['value']} ({s.get('link')[:40]}...)")
        
    winner, score, reason = engine.score_candidates(candidates)
    
    print(f"\n🏆 Winner: {winner}")
    print(f"📊 Score: {score}")
    print(f"📝 Reason: {reason}")
    
    if winner:
        print("\n✅ Verification PASSED: Identify broker candidates.")
    else:
        print("\n⚠️ Verification WARNING: No candidates found (Check API key or Mock data).")

if __name__ == "__main__":
    test_engine()
