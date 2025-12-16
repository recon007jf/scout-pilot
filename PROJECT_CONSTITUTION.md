# PROJECT CONSTITUTION & BUSINESS LOGIC INVARIANTS

## 1. THE GOLDEN RULE (FAIL-SAFE)

**Andrew Oram (Point C) is a CHANNEL SELLER.**

* **Customer:** The Insurance Broker (e.g., Marsh, Aon, Gallagher).
* **Product:** Self-funding solutions sold *through* the Broker to their clients.
* **Prohibited Action:** Direct outreach to the Employer/Plan Sponsor.

## 2. THE "BLACKLIST" PROTOCOL

* **Constraint:** Bypassing a Broker to contact an Employer directly is a catastrophic failure.
* **Consequence:** Immediate blacklisting by the brokerage community.
* **ENFORCEMENT:** Any feature, automation, or output that enables direct-to-employer outreach is invalid by definition.
* **Logic Implication:**
  * The "Client" (Employer) data is **INTEL ONLY**.
  * The "Target" (Contact) is the **BROKER**.

## 3. DATA REALITY CHECK

* DOL Form 5500 does NOT contain Broker Emails.
* DOL Form 5500 does NOT contain Sponsor Emails (2023 Layout).
* **Workflow:**
  * Step 1: Generate "Broker Firm -> Employer Map" (This Pipeline).
  * Step 2: Automated Enrichment via Apollo/ZoomInfo to find Broker Emails (Downstream).

## 4. SYSTEM OBJECTIVE

The goal of this software is to **Empower Brokers**, not replace them.
