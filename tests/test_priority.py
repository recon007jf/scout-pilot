import pandas as pd
from prospector_agent import build_anchor_universe
import os

# Create dummy CSV
dummy_csv = "dummy_5500_priority.csv"
data = {
    'SPONSOR_NAME': ['Comp A', 'Comp B', 'Comp C', 'Comp D', 'Comp E'],
    'SPONSOR_US_ADDRESS_STATE': ['NV', 'CA', 'OR', 'WA', 'AZ'], # High: CA/WA (Comp B, D) -> Med: OR/AZ (C, E) -> Low: NV (A)
    'TOT_PARTCP_BOY_CNT': [500, 500, 500, 500, 500],
    'FUNDING_ARRANGEMENT': ['3', '3', '3', '3', '3']
}
df = pd.DataFrame(data)
df.to_csv(dummy_csv, index=False)

try:
    print("Testing Priority Sort (Enable Mode)...")
    anchors = build_anchor_universe(dummy_csv, priority_mode=True)
    
    print("\nResult Order:")
    for a in anchors:
        print(f"- {a['State']} ({a['Client Name']})")
        
    states = [a['State'] for a in anchors]
    
    # Expected: CA, WA, OR/AZ/NV order
    # CA=1, WA=2, OR=3, AZ=3, NV=4
    # Note: OR and AZ are tied (3), so their relative order depends on stability, but they must be after WA and before NV.
    
    if states[0] == 'CA' and states[1] == 'WA' and states[-1] == 'NV':
        print("\n✅ PASS: Sorting matches Priority Hierachy.")
    else:
        print("\n❌ FAIL: Sorting incorrect.")
        
finally:
    if os.path.exists(dummy_csv):
        os.remove(dummy_csv)
