
import csv
import difflib
import io
import os

# --- MOCK PREP LOGIC TO GET GOVT FIRMS ---
# We'll just read pilot_static_data.csv if available to see what firms were extracted
# This avoids re-running the huge 5500 scan
def get_govt_firms():
    firms = set()
    path = "data/pilot_static_data.csv"
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                firm = row.get('broker_firm')
                if firm and firm != "Unknown":
                    firms.add(firm)
    else:
        print("❌ pilot_static_data.csv missing. Cannot load Govt Firms.")
    return sorted(list(firms))

def get_roster_firms():
    firms = set()
    path = "data/roster_master.csv"
    if os.path.exists(path):
        with open(path, 'r', encoding='latin-1') as f:
            # Skip likely header offset
            lines = f.readlines()
            start_idx = 0
            for i, line in enumerate(lines[:50]):
                if 'Account' in line and 'Name' in line:
                    start_idx = i
                    break
            
            f.seek(0)
            for _ in range(start_idx): f.readline()
            
            reader = csv.DictReader(f)
            for row in reader:
                acct = row.get('Account')
                if acct: firms.add(acct.strip())
    else:
        print("❌ roster_master.csv missing.")
    return sorted(list(firms))

def run_diagnostics():
    print("🔬 STARTING MATCHING DIAGNOSTICS...\n")
    
    govt_firms = get_govt_firms()
    roster_firms = get_roster_firms()
    
    print(f"🏛️  GOVERNEMENT FIRMS ({len(govt_firms)}):")
    for g in govt_firms[:10]: print(f"   - {g}")
    print("   ...(truncated)\n")
    
    print(f"📋 ROSTER FIRMS ({len(roster_firms)}):")
    for r in roster_firms[:10]: print(f"   - {r}")
    print("   ...(truncated)\n")
    
    print("⚔️  FUZZY MATCH BATTLE (Threshold 0.6):")
    
    matches_found = 0
    # Test top 20 Govt Firms against ALL Roster Firms
    for g_firm in govt_firms:
        best_match = None
        best_score = 0
        
        for r_firm in roster_firms:
            # 1. Direct
            if r_firm.lower() in g_firm.lower():
                best_match = r_firm
                best_score = 1.0
                break
            
            # 2. Fuzzy
            ratio = difflib.SequenceMatcher(None, r_firm.lower(), g_firm.lower()).ratio()
            if ratio > best_score:
                best_score = ratio
                best_match = r_firm
        
        status = "❌ FAIL"
        if best_score > 0.6:
            status = "✅ PASS"
            matches_found += 1
            
        print(f"   [{status}] Govt: '{g_firm}'  vs  Roster: '{best_match}' (Score: {best_score:.2f})")
    
    print(f"\n📊 SUMMARY: {matches_found}/{len(govt_firms)} Matches Found.")
    if matches_found == 0:
        print("⚠️  CRITICAL: ZERO MATCHES. Lower threshold or normalize strings.")

if __name__ == "__main__":
    run_diagnostics()
