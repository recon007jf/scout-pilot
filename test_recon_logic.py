
import os
import sys
import json
from dotenv import load_dotenv

# Ensure backend path is in sys.path
sys.path.append(os.path.abspath("backend"))

from recon_agent import analyze_lead, write_email

# Dummy Data
dummy_intel = """
Summary: John Smith is a VP at TechCorp.
Posts: 
- "Sick of these PBMs hiding data. Use tracking codes!"
- "Another year of spread pricing nonsense. We need transparency."
- "Just reviewed a contract that had 40% hidden fees. Outrageous."
"""

dummy_client_context = {
    'source': 'F5500_General_Assets',
    'client_name': 'TechCorp Systems',
    'funding_type': 'General Assets'
}

def test_analysis():
    print("\n--- Testing Analyze Lead (Expecting 'Pain Responder') ---")
    result = analyze_lead(dummy_intel, "John Smith", "TechCorp")
    print("\n[Analysis Result]:")
    print(json.dumps(result, indent=2))
    return result

def test_email_gen(analysis):
    print("\n--- Testing Email Generation (General Assets Context) ---")
    email = write_email(analysis, "John", dummy_client_context)
    print("\n[Generated Email]:")
    print(email)

if __name__ == "__main__":
    load_dotenv()
    analysis = test_analysis()
    if analysis:
        test_email_gen(analysis)
