import sys
import os
print("DEBUG: CWD IS " + os.getcwd())
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
print("DEBUG: PATH IS " + str(sys.path))
try:
    from scout.broker_hunter import BrokerHunter
    print("DEBUG: IMPORT SUCCESS")
except ImportError as e:
    print(f"DEBUG: IMPORT FAILED: {e}")
