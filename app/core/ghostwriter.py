from typing import Dict, Any, Optional
from app.core.llm import LLMClient

class GhostwriterEngine:
    """
    Core engine for generating email drafts.
    Now upgraded to use LLM for Intelligence-infused drafts.
    """
    
    def __init__(self):
        self.llm = LLMClient()

    def generate_draft(self, candidate: Dict[str, Any], context: Optional[str] = None) -> str:
        """
        Generates a draft email body for a candidate.
        If 'context' is provided (Signals/Events), it uses the LLM to write a custom hook.
        Otherwise, falls back to the safe template.
        """
        name = candidate.get("full_name") or "there"
        company = candidate.get("firm") or "your firm"
        role = candidate.get("role") or "your role"
        
        # 1. INTELLIGENCE MODE (LLM)
        if context and self.llm.client:
            system_prompt = """
            You are Andrew, a strategic Benefits Consultant.
            Write a short, professional cold email to a prospective client.
            
            TONE:
            - Concise (under 100 words).
            - Peer-to-peer (not "salesy").
            - Insight-led.
            
            GOAL:
            - Use the provided CONTEXT (News/Events) to create a natural "bridge".
            - Connect their situation to "benchmarking" or "strategy".
            - Ask for a brief chat.
            
            Output strictly the email body (including Subject line).
            """
            
            user_text = f"""
            TARGET: {name}, {role} at {company}.
            
            CONTEXT/INTELLIGENCE:
            {context}
            
            Write the email.
            """
            
            result = self.llm.analyze_text(system_prompt, user_text)
            content = result.get("content")
            if content:
                return content

        # 2. TEMPLATE MODE (Fallback)
        # Simple Template (Phase 1 Approved)
        subject = f"Subject: Question regarding {company} benefits plan"
        
        body = f"""Hi {name},

I noticed {company} hasn't updated its benefits strategy recently. Given your role as {role}, I thought you might be interested in our new benchmarking data.

Would you be open to a 5-minute chat?

Best,
Andrew"""

        return f"{subject}\n\n{body}"
