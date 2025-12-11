from recon_agent import analyze_lead, write_email
import json

# Mock Intel (Simulating a "Controller" type)
mock_intel_controller = """
BIO:
CEO of Apex Benefits. "We build custom self-funded solutions for employers who want to take back control."
Specializes in Reference Based Pricing and direct contracting.
"Stop letting the BUCA carriers dictate your costs."

POSTS:
"Another win for a client who left Cigna. Saved 20% in year one by unbundling."
"Transparency is key. If you can't see your claims data, you're being robbed."
"""

# Mock Intel (Simulating a "Guardian" type)
mock_intel_guardian = """
BIO:
VP of Employee Benefits at SafeHarbor Insurance.
"Helping employers navigate the complex world of benefits with peace of mind."
Focus on compliance, long-term strategy, and member satisfaction.

POSTS:
"Great seminar on ERISA compliance today. Keeping our clients safe is job #1."
"Employee retention starts with a great benefits package. It's about trust."
"""

def test_lead(name, intel):
    print(f"\n--- Testing {name} ---")
    print("1. Analyzing...")
    analysis = analyze_lead(intel, name, "Test Firm")
    print(json.dumps(analysis, indent=2))
    print(f"\nEvidence: {analysis.get('Archetype_Evidence', 'N/A')}")
    
    print("\n2. Writing Email...")
    email = write_email(analysis, name.split()[1]) # Pass "Controller" or "Guardian" as first name
    print(f"\nEMAIL:\n{email}")

if __name__ == "__main__":
    test_lead("Mr. Controller", mock_intel_controller)
    test_lead("Mrs. Guardian", mock_intel_guardian)
