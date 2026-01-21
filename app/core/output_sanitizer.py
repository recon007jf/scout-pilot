"""
Output Sanitizer (Post-Rewrite Safety Gate)

Purpose: Check rewritten email for safety violations before display/send.
Uses regex-based checks (no LLM required).

If violations found:
- Block send
- Require manual edit
"""

import re
from typing import List
from pydantic import BaseModel
from app.utils.logger import get_logger

logger = get_logger("output_sanitizer")


class SafetyResult(BaseModel):
    """Result of safety check."""
    is_safe: bool
    violations: List[str]  # e.g. ["profanity:fuck", "accusation:you_never"]
    blocked: bool  # True if output should be blocked from sending


class OutputSanitizer:
    """
    Post-rewrite safety gate.
    
    Checks for:
    - Profanity
    - Accusations
    - Emotional escalation
    - First-person blame patterns
    """
    
    # Profanity wordlist (expandable)
    PROFANITY_WORDS = [
        "fuck", "fucking", "fucked", "shit", "shitty", "damn", "damned",
        "hell", "ass", "asshole", "bitch", "crap", "crappy", "wtf", "wth",
        "bullshit", "pissed", "piss"
    ]
    
    # Accusation patterns
    ACCUSATION_PATTERNS = [
        r"\byou never\b",
        r"\byou didn't\b",
        r"\byou failed\b",
        r"\byou ignored\b",
        r"\byou refused\b",
        r"\byour fault\b",
        r"\byou lied\b",
        r"\byou promised\b",
    ]
    
    # Emotional escalation phrases
    ESCALATION_PHRASES = [
        "extremely disappointed",
        "completely unacceptable",
        "totally unprofessional",
        "very frustrated",
        "deeply concerned",
        "seriously upset",
        "absolutely ridiculous",
    ]
    
    def check_safety(self, text: str) -> SafetyResult:
        """
        Check text for safety violations.
        
        Args:
            text: Email body to check
            
        Returns:
            SafetyResult with is_safe, violations, and blocked status
        """
        violations = []
        text_lower = text.lower()
        
        # Check profanity
        for word in self.PROFANITY_WORDS:
            pattern = rf"\b{re.escape(word)}\b"
            if re.search(pattern, text_lower):
                violations.append(f"profanity:{word}")
        
        # Check accusations
        for pattern in self.ACCUSATION_PATTERNS:
            if re.search(pattern, text_lower):
                match = re.search(pattern, text_lower)
                violations.append(f"accusation:{match.group()}")
        
        # Check emotional escalation
        for phrase in self.ESCALATION_PHRASES:
            if phrase in text_lower:
                violations.append(f"escalation:{phrase}")
        
        # Determine if blocked
        # Block on any profanity or accusation
        blocked = any(v.startswith("profanity:") or v.startswith("accusation:") for v in violations)
        
        is_safe = len(violations) == 0
        
        if violations:
            logger.warning(f"Safety violations found: {violations}")
        
        return SafetyResult(
            is_safe=is_safe,
            violations=violations,
            blocked=blocked
        )


# Convenience function
def check_output_safety(text: str) -> SafetyResult:
    """Convenience wrapper for safety check."""
    sanitizer = OutputSanitizer()
    return sanitizer.check_safety(text)
