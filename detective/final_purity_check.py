import pandas as pd
import os

def final_purity_check():
    # PATH - AG FIX: scratch artifact location
    BASE_PATH = "/Users/josephlf/.gemini/antigravity/scratch/backend/Scout_Data_Artifacts"
    f = os.path.join(BASE_PATH, "Leads_Shortlist_Sniper.csv")
    
    if not os.path.exists(f): 
        print(f"File not found: {f}")
        return
    
    df = pd.read_csv(f)
    print(f"SCANNING {len(df)} TARGETS...")
    
    # 1. Financial Keywords
    bad_words = ["VANGUARD", "FIDELITY", "EMPOWER", "RETIREMENT", "401K", "PENSION"]
    # Cast to str to be safe
    bad_brokers = df[df['broker_2021'].astype(str).str.upper().apply(lambda x: any(w in x for w in bad_words))]
    
    # 2. P&C Keywords in Titles
    bad_titles = df[df['Contact Job Title'].astype(str).str.upper().apply(lambda x: any(w in str(x) for w in ["PROPERTY", "CASUALTY", "RISK"]))]
    
    print(f" - Financial Brokers Found: {len(bad_brokers)}")
    print(f" - P&C Titles Found:        {len(bad_titles)}")
    
    if len(bad_brokers) > 0:
        print("   >>> Samples (Brokers):")
        print(bad_brokers['broker_2021'].unique()[:5])

    if len(bad_titles) > 0:
        print("   >>> Samples (Titles):")
        print(bad_titles['Contact Job Title'].unique()[:5])

    if len(bad_brokers) == 0 and len(bad_titles) == 0:
        print("✅ CLEAN. READY FOR EXPORT.")
    else:
        print("⚠️ FOUND IMPURITIES. (Check output above).")

if __name__ == "__main__":
    final_purity_check()
