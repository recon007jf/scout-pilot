import os
import re
import pandas as pd

# TASK: AGENT BERNAYS v2.1 (SNIPER SCOPE - FIXED GROUPING)
# GOAL:
# 1) Score contacts (Seniority + Benefits relevance + Contactability)
# 2) REDUCE: Keep Top 3 contacts per (Plan + Broker Firm + Broker State)
# 3) Enrich with Psych Archetypes

class AgentBernays:
    def __init__(self):
        self.black_book = {
            "MERCER": {"archetype": "The Bureaucrat", "hook": "Stop paying for the brand name. Get agile, executive-level attention."},
            "AON": {"archetype": "The Giant", "hook": "Your plan isn't generic. Your strategy shouldn't be either."},
            "GALLAGHER": {"archetype": "The Aggressor", "hook": "Service that lasts longer than the honeymoon phase."},
            "WTW": {"archetype": "The Academic", "hook": "Practical results, not just actuarial theory."},
            "LOCKTON": {"archetype": "The Private Club", "hook": "Institutional expertise with boutique responsiveness."},
            "HUB": {"archetype": "The Roll-Up", "hook": "A unified team, not a patchwork of acquisitions."},
            "ALLIANT": {"archetype": "The Specialist", "hook": "Stable partnership, not a revolving door."},
            "USI": {"archetype": "The Optimizer", "hook": "Custom strategies that go beyond the standard playbook."},
            "MARSH": {"archetype": "The Empire", "hook": "Big-firm power without big-firm inertia."}
        }
        self.default_profile = {"archetype": "The Incumbent", "hook": "A fresh set of eyes often finds 15% savings."}

        # Domain relevance signals
        self.good_domain_terms = [
            "EMPLOYEE BENEFIT", "EMPLOYEE BENEFITS", "BENEFIT", "BENEFITS", "EB", "HEALTH", "WELFARE",
            "MEDICAL", "GROUP", "TOTAL REWARDS", "HR", "H&W", "HEALTH & WELFARE"
        ]
        self.bad_domain_terms = [
            "PROPERTY", "CASUALTY", "P&C", "COMMERCIAL", "RISK", "LIABILITY", "MARINE", "SURETY"
        ]

        # Seniority signals
        self.senior_terms_95 = ["CHRO", "CFO", "CEO", "PRESIDENT", "PRINCIPAL", "PARTNER", "MANAGING DIRECTOR", "OWNER", "FOUNDER"]
        self.senior_terms_80 = ["EVP", "EXECUTIVE VICE", "SVP", "SENIOR VICE"]
        self.senior_terms_70 = ["VP", "VICE PRESIDENT", "DIRECTOR", "HEAD", "LEADER", "REGIONAL"]

        # Mid-level but often relevant
        self.mid_terms = ["PRODUCER", "ADVISOR", "CONSULTANT", "BROKER", "ACCOUNT EXECUTIVE"]

        # Lower priority
        self.low_terms = ["ASSISTANT", "COORDINATOR", "ANALYST", "INTERN", "ADMIN", "RECEPTION", "OPERATIONS"]

    def _u(self, x):
        return "" if pd.isna(x) else str(x).strip().upper()

    def _has_any(self, text, terms):
        return any(t in text for t in terms)

    def _valid_email(self, email):
        if pd.isna(email):
            return False
        e = str(email).strip()
        return ("@" in e) and (" " not in e) and (len(e) >= 6)

    def calculate_score(self, row):
        title = self._u(row.get("Contact Job Title", ""))
        email = row.get("Contact Email", None)

        score = 50

        # 1) Seniority
        if self._has_any(title, self.senior_terms_95):
            score += 40
        elif self._has_any(title, self.senior_terms_80):
            score += 30
        elif self._has_any(title, self.senior_terms_70):
            score += 20
        elif self._has_any(title, self.mid_terms):
            score += 10

        # 2) Relevance (benefits vs P&C)
        if self._has_any(title, self.good_domain_terms):
            score += 20
        if self._has_any(title, self.bad_domain_terms):
            score -= 25

        # 3) Actionability (email required)
        if not self._valid_email(email):
            score -= 60

        # 4) Penalties for clearly low-priority roles
        if self._has_any(title, self.low_terms):
            score -= 20

        return score

    def get_psych_profile(self, broker_name):
        bn = self._u(broker_name)
        for key, profile in self.black_book.items():
            if key in bn:
                return profile
        return self.default_profile


def run_sniper_scope():
    print(">>> AGENT BERNAYS v2.1 (SNIPER SCOPE) INITIATED")

    # AG FIX: Point to scratch artifacts
    BASE_PATH = "/Users/josephlf/.gemini/antigravity/scratch/backend/Scout_Data_Artifacts"
    
    # INPUT: The 131k Pairs from Handshake
    INPUT_FILE = os.path.join(BASE_PATH, "Leads_With_Human_Contacts.csv")
    OUTPUT_FILE = os.path.join(BASE_PATH, "Leads_Shortlist_Sniper.csv")

    if not os.path.exists(INPUT_FILE):
        print(f"[ERROR] Input file not found: {INPUT_FILE}")
        return

    print("... Loading raw matches ...")
    df = pd.read_csv(INPUT_FILE, low_memory=False)
    print(f"   Loaded {len(df):,} rows.")

    # Note: Column names in CSV from run_the_handshake were: 
    # SPONSOR_NAME, LIVES, PROVIDER_NAME_NORM (as lead_firm_col), PROVIDER_STATE...
    # The user logic expects: 'sponsor_name', 'lives', 'broker_2021', 'broker_state'
    # I need to rename/check
    
    # Handshake output columns: [SPONSOR_NAME, LIVES, PROVIDER_NAME_NORM, PROVIDER_STATE, Contact Full Name...]
    # Renaming for compatibility
    rename_map = {
        "SPONSOR_NAME": "sponsor_name",
        "LIVES": "lives",
        "PROVIDER_NAME_NORM": "broker_2021",
        "PROVIDER_STATE": "broker_state"
    }
    df.rename(columns=rename_map, inplace=True)

    required = ["sponsor_name", "lives", "broker_2021", "broker_state", "Contact Full Name", "Contact Job Title", "Contact Email"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"[ERROR] Missing required columns in input: {missing}")
        print(f"Available: {list(df.columns)}")
        return

    bernays = AgentBernays()

    # Ensure numeric lives
    df["lives"] = pd.to_numeric(df["lives"], errors="coerce").fillna(0).astype(int)

    # 1) Score
    print("... Scoring contacts ...")
    df["sniper_score"] = df.apply(bernays.calculate_score, axis=1)

    # 2) Filter: keep only good candidates
    df_q = df[df["sniper_score"] >= 50].copy()
    print(f"   Qualified candidates (score >= 50): {len(df_q):,}")

    # 3) Reducer: Top 3 per (Plan + Firm + State)
    print("... Reducing to Top 3 per (plan + firm + state) ...")
    df_q.sort_values(
        by=["sponsor_name", "broker_2021", "broker_state", "sniper_score", "lives"],
        ascending=[True, True, True, False, False],
        inplace=True
    )
    df_short = df_q.groupby(["sponsor_name", "broker_2021", "broker_state"], as_index=False).head(3).copy()

    # 4) Psych enrichment
    print("... Applying psych profiles ...")
    psych = df_short["broker_2021"].apply(bernays.get_psych_profile)
    df_short["psych_archetype"] = psych.apply(lambda x: x["archetype"])
    df_short["psych_hook"] = psych.apply(lambda x: x["hook"])

    # 5) Final sort for action: big plans + best human targets
    df_short.sort_values(by=["lives", "sniper_score"], ascending=[False, False], inplace=True)

    # 6) Save
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    cols = [
        "sponsor_name", "lives", "broker_2021", "broker_state",
        "Contact Full Name", "Contact Job Title", "Contact Email",
        "sniper_score", "psych_archetype", "psych_hook"
    ]
    final_df = df_short[cols].copy()
    final_df.to_csv(OUTPUT_FILE, index=False)

    print("-" * 40)
    print("SNIPER SCOPE COMPLETE")
    print(f"Original Matches: {len(df):,}")
    print(f"Qualified Rows:   {len(df_q):,}")
    print(f"Final Shortlist:  {len(final_df):,}")
    print(f"Saved:            {OUTPUT_FILE}")
    print("-" * 40)
    print("\n[TOP 5 TARGETS PREVIEW]")
    print(final_df.head(5).to_string(index=False))

if __name__ == "__main__":
    run_sniper_scope()
