from typing import Dict, Any

class GhostwriterEngine:
    """
    Core engine for generating email drafts.
    Phase 1: Template-based Generation (Lite).
    Phase 2: LLM-based Generation (Full).
    """

    def generate_draft(self, candidate: Dict[str, Any]) -> str:
        """
        Generates a draft email body for a candidate.
        This function is called during the Async Prep Job, NOT at render time.
        """
        name = candidate.get("full_name") or "there"
        company = candidate.get("firm") or "your firm"
        role = candidate.get("role") or "your role"
        
        # Simple Template (Phase 1 Approved)
        subject = f"Subject: Question regarding {company} benefits plan"
        
        body = f"""Hi {name},

I noticed {company} hasn't updated its benefits strategy recently. Given your role as {role}, I thought you might be interested in our new benchmarking data.

Would you be open to a 5-minute chat?

Best,
Andrew"""

        return f"{subject}\n\n{body}"
