from pydantic import BaseModel
from typing import List, Optional
import json
import google.generativeai as genai
from app.config import settings
from app.domain.product_facts import PRODUCT_FACTS, get_product_context_str
from app.utils.logger import get_logger

logger = get_logger("draft_engine")

# Configure Gemini
try:
    genai.configure(api_key=settings.GEMINI_API_KEY)
except Exception as e:
    logger.warning(f"AI Config Failed (Check Key): {e}")

class DraftOutput(BaseModel):
    subject: str
    body: str # The draft content
    metadata: dict # Confidence, assumptions, etc.

class DraftEngine:
    def __init__(self):
        self.model = genai.GenerativeModel('gemini-pro')

    def generate_draft(self, broker_profile: dict, analysis: dict) -> DraftOutput:
        """
        Generates a draft using Point C Truths.
        Returns Pydantic Object.
        """
        
        # Context Injection
        name = broker_profile.get("full_name", "Broker")
        firm = broker_profile.get("firm", "your firm")
        analysis_summary = analysis.get("signal_reasoning", "General outreach")
        
        prompt = f"""
        CONTEXT:
        You are Andrew writing to a benefits broker named {name} at {firm}.
        The reason for reaching out: {analysis_summary}
        
        {get_product_context_str()}
        
        TASK:
        Write a short, direct email draft.
        
        CONSTRAINTS:
        - Output MUST be valid JSON matching: {{ "subject": "...", "body": "...", "metadata": {{ "confidence": 0.9, "unknowns": [] }} }}
        - No markdown formatting. No "Here is the email". Just JSON.
        """
        
        try:
            response = self.model.generate_content(prompt)
            raw_text = response.text.strip()
            
            # Clean potential markdown code blocks
            if raw_text.startswith("```json"):
                raw_text = raw_text[7:]
            if raw_text.endswith("```"):
                raw_text = raw_text[:-3]
                
            data = json.loads(raw_text)
            return DraftOutput(**data)
            
        except Exception as e:
            logger.error(f"Draft Generation Failed: {e}")
            # Fallback
            return DraftOutput(
                subject=f"Partnership with Point C / {firm}",
                body="[AI Generation Failed - Manual Edit Required]",
                metadata={"error": str(e)}
            )
