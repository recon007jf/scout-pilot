
from recon_agent import run_forensic_audit
import time

print("🔍 Starting Local Forensic Audit Debug...")
try:
    issues = run_forensic_audit()
    print(f"\n✅ Audit Finished. Issues Found: {issues}")
except Exception as e:
    print(f"\n❌ CRITICAL ERROR during Audit: {e}")
