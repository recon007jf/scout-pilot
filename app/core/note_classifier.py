"""
Note Intent Classifier (Stage 1 of Two-Stage Pipeline)

Purpose: Interpret raw notes safely using LLM.
Output: Structured intent + normalized summary.

RULE: Raw note text dies here. It is NEVER forwarded to Stage 2 (Rewrite).
"""

from typing import Optional
from pydantic import BaseModel
import google.generativeai as genai
from app.config import settings
from app.utils.logger import get_logger

logger = get_logger("note_classifier")

# Configure Gemini
try:
    genai.configure(api_key=settings.GEMINI_API_KEY)
except Exception as e:
    logger.warning(f"AI Config Failed: {e}")


class NoteIntent(BaseModel):
    """Structured output from note classification."""
    intent: str  # FACTUAL_CORRECTION, STRATEGIC_GUIDANCE, TONE_ADJUSTMENT, EMOTIONAL_VENT, IGNORE_FOR_OUTREACH
    summary: str  # Normalized, professional summary (NO raw text, NO profanity)
    confidence: float = 0.8


# System instruction for Stage 1 classifier
CLASSIFIER_SYSTEM_INSTRUCTION = """
You are an internal note rewriter for a sales outreach system.

Your job is to take ANY internal note and produce a PROFESSIONAL, USABLE version of it.
NOTHING IS EVER IGNORED. Every note gets reworded and sanitized.

INTENT TYPES (for categorization only - ALL intents get reworded):
- FACTUAL_CORRECTION: Note contains factual updates (role change, left company, new info)
- STRATEGIC_GUIDANCE: Note gives direction on messaging (focus on X, mention Y)
- TONE_ADJUSTMENT: Note indicates desired tone change (be firmer, softer, more direct)
- PERSONAL_NOTE: Note contains personal/social content (birthdays, events, personal requests)
- EMOTIONAL_CONTENT: Note contains strong emotions - reword to professional equivalent

CRITICAL RULE: NOTHING IS EVER IGNORED.
- Even inappropriate content gets REWORDED professionally
- "stripper with huge tits" becomes "a surprise celebration"
- "WTF why didn't you call" becomes "follow up on previous conversation"
- Personal notes about parties become "on a personal note, there's an upcoming celebration"

OUTPUT RULES:
1. Your 'summary' must be a USABLE, PROFESSIONAL rewrite of the note
2. NEVER say "no outreach impact" or "ignore" - ALWAYS produce usable content
3. Sanitize profanity/inappropriate content into professional equivalents
4. The summary should capture the INTENT and be usable in an email
5. Even bizarre requests should be reworded professionally

OUTPUT FORMAT (strict JSON):
{
  "intent": "INTENT_TYPE",
  "summary": "Professional rewrite that captures the intent",
  "confidence": 0.9
}
"""


class NoteIntentClassifier:
    """
    Stage 1: Classify raw notes using LLM.
    
    Raw notes are allowed here for interpretation.
    Output is a sanitized NoteIntent that can be passed to Stage 2.
    """
    
    def __init__(self):
        self.model = genai.GenerativeModel(
            settings.GEMINI_MODEL_NAME,
            system_instruction=CLASSIFIER_SYSTEM_INSTRUCTION
        )
    
    def classify(self, note_text: str) -> Optional[NoteIntent]:
        """
        Classify a raw note into structured intent.
        
        Args:
            note_text: Raw user note (may contain profanity, emotions, etc.)
            
        Returns:
            NoteIntent with sanitized summary, or None if classification fails.
        """
        if not note_text or not note_text.strip():
            return None
        
        try:
            prompt = f"""
Classify this internal note:

---
{note_text}
---

Output valid JSON only.
"""
            
            response = self.model.generate_content(prompt)
            raw_output = response.text.strip()
            
            # Parse JSON
            if raw_output.startswith("```json"):
                raw_output = raw_output[7:]
            if raw_output.endswith("```"):
                raw_output = raw_output[:-3]
            
            import json
            data = json.loads(raw_output)
            
            intent = NoteIntent(
                intent=data.get("intent", "IGNORE_FOR_OUTREACH"),
                summary=data.get("summary", "No guidance available."),
                confidence=data.get("confidence", 0.8)
            )
            
            # Safety: Never let profanity leak into summary
            # Use word boundaries to avoid false positives (e.g., "ass" in "assist")
            import re
            PROFANITY_PATTERNS = [
                r'\bwtf\b', r'\bfuck\b', r'\bshit\b', r'\bdamn\b', 
                r'\bhell\b', r'\bass\b', r'\bbitch\b', r'\bcrap\b'
            ]
            summary_lower = intent.summary.lower()
            for pattern in PROFANITY_PATTERNS:
                if re.search(pattern, summary_lower):
                    word = pattern.replace(r'\b', '')
                    logger.warning(f"Profanity leaked into summary, sanitizing: {word}")
                    intent.summary = "Internal note - requires manual review"
                    # DON'T change intent to EMOTIONAL_VENT - still process it
                    break
            
            logger.info(f"Note classified: intent={intent.intent}, confidence={intent.confidence}")
            return intent
            
        except Exception as e:
            logger.error(f"Note classification failed: {e}")
            return NoteIntent(
                intent="IGNORE_FOR_OUTREACH",
                summary="Classification failed - no guidance available.",
                confidence=0.0
            )


# Convenience function
def classify_note(note_text: str) -> Optional[NoteIntent]:
    """Convenience wrapper for note classification."""
    classifier = NoteIntentClassifier()
    return classifier.classify(note_text)
