import os
import csv
import json
from datetime import datetime
# Note: In production, we would import supabase here.
# For this logic test, we are mocking the inputs to prove the decision tree works.

# --- CONFIGURATION (The "Safe Mode" Constants) ---
SESSION_BUDGET_CAP = 5.00  # $5.00 Hard Limit
COST_COMPANY_ENRICH = 0.10
COST_PERSON_ENRICH = 0.55

# Gating Logic
MIN_LIVES_VALUE_GATE = 50        # Below 50 lives = HOLD
MIN_LIVES_FULLY_INSURED = 1000   # Fully Insured < 1000 lives = HOLD
MIN_COMPANY_CONFIDENCE = 0.50    # Below 50% match = HOLD

# --- MOCK PDL FUNCTIONS ---
# These simulate what the API *would* return for these specific inputs.
def mock_pdl_company(name, state):
    # Simulate: High confidence for real companies, low for fake/shells
    if "GLOBAL ENTERPRISE" in name.upper(): return {"confidence": 0.9, "name": "Global Enterprise Holdings"} 
    if "SMITH FAMILY" in name.upper(): return {"confidence": 0.8, "name": "The Smith Family Trust"}
    if "SPACE" in name.upper(): return {"confidence": 0.99, "name": "SpaceX"}
    if "VALLEY" in name.upper(): return {"confidence": 0.95, "name": "Valley Iron Works"}
    if "LEGACY" in name.upper(): return {"confidence": 0.98, "name": "Legacy Health"}
    if "ACME" in name.upper(): return {"confidence": 0.90, "name": "Acme Manufacturing"}
    return {"confidence": 0.0, "name": None}

def mock_pdl_person(company_name):
    # Simulate: Found for large/clean co, missing for small/messy
    if company_name == "SpaceX": return {"full_name": "Bret Johnsen", "title": "CFO"}
    if company_name == "Legacy Health": return {"full_name": "Anna Loomis", "title": "CFO"}
    if company_name == "Acme Manufacturing": return {"full_name": "Mike Williams", "title": "VP Finance"}
    return None # Not found for others

# --- MAIN EXECUTION ---
def run_batch():
    print(f"🚀 Starting MVP Stress Test (Budget: ${SESSION_BUDGET_CAP})")
    
    # 1. The Hostile Input (Internal)
    input_rows = [
        {"name": "SPACE EXPLORATION TECHNOLOGIES CORP", "lives": 12000, "funding": 4},
        {"name": "VALLEY IRON WORKS INC", "lives": 85, "funding": 4},
        {"name": "LEGACY HEALTH SYSTEM", "lives": 15000, "funding": 1},
        {"name": "THE SMITH FAMILY TRUST", "lives": 12, "funding": 4},
        {"name": "ACME HOLDINGS LLC DBA ACME MFG", "lives": 250, "funding": 4},
        {"name": "GLOBAL ENTERPRISE HOLDINGS INC", "lives": 5, "funding": 4}
    ]

    total_cost = 0.0
    results = []

    for row in input_rows:
        row_cost = 0.0
        decision = "PENDING"
        reason = "N/A"
        
        # --- GATE 1: COMPANY ENRICHMENT ($0.10) ---
        if total_cost + COST_COMPANY_ENRICH > SESSION_BUDGET_CAP:
            results.append({**row, "action": "SKIPPED_BUDGET", "cost": 0.0})
            continue

        # Simulate API Call
        company_data = mock_pdl_company(row["name"], "CA") 
        row_cost += COST_COMPANY_ENRICH
        
        # LOGIC CHECK: Invalid Entity (Trust/Holding + Small Lives)
        is_shell = ("TRUST" in row["name"] or "FAMILY" in row["name"] or "HOLDINGS" in row["name"]) and row["lives"] < 50
        
        if company_data["confidence"] < MIN_COMPANY_CONFIDENCE:
            decision = "HOLD_INVALID_ENTITY"
            reason = "Low Confidence Match"
        elif is_shell:
            decision = "HOLD_INVALID_ENTITY"
            reason = "Ambiguous Shell Entity"
        
        # --- GATE 2: VALUE FILTER ($0.00) ---
        if decision == "PENDING":
            if row["lives"] < MIN_LIVES_VALUE_GATE:
                decision = "HOLD_LOW_VALUE"
                reason = "Lives < 50"
            elif row["funding"] == 1 and row["lives"] < MIN_LIVES_FULLY_INSURED:
                decision = "HOLD_LOW_VALUE"
                reason = "Fully Insured & Small"

        # --- GATE 3: PERSON ENRICHMENT ($0.55) ---
        target_person = None
        if decision == "PENDING":
            if total_cost + row_cost + COST_PERSON_ENRICH > SESSION_BUDGET_CAP:
                 decision = "SKIPPED_BUDGET"
            else:
                # Simulate API Call
                target_person = mock_pdl_person(company_data["name"])
                row_cost += COST_PERSON_ENRICH

                # --- GATE 4: FINAL DECISION ---
                if target_person:
                    decision = "SEND"
                    reason = "High Confidence Match"
                else:
                    decision = "REVIEW"
                    reason = "No Decision Maker Found"

        # Record Result
        total_cost += row_cost
        results.append({
            "Client": row["name"],
            "Resolved": company_data["name"],
            "Target": target_person["full_name"] if target_person else "Unknown",
            "Action": decision,
            "Reason": reason,
            "Cost": f"${row_cost:.2f}"
        })

    # Output CSV to Console
    print("\n--- CSV OUTPUT ---")
    print("Client,Resolved,Target,Action,Reason,Cost")
    for r in results:
        print(f"{r['Client']},{r['Resolved']},{r['Target']},{r['Action']},{r['Reason']},{r['Cost']}")

    print(f"\nTotal Run Cost: ${total_cost:.2f}")

if __name__ == "__main__":
    run_batch()
