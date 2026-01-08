# Point C Product Truths
# Version: 1.0 (Jan 5, 2026)
# Usage: Injected into AI Prompts to prevent hallucination.

PRODUCT_FACTS = {
    "hook": "Self-funded administration for employers (100-2,000 lives).",
    "networks": "Access to Cigna, Aetna, and Anthem networks.",
    "differentiator": "Boutique-level responsiveness + custom plan flexibility (unlike UMR/Meritain).",
    "target_audience": "Benefits Brokers and Consultants managing mid-market employers.",
    "voice_constraints": [
        "No 'I hope this finds you well'.",
        "No marketing fluff.",
        "Direct, peer-to-peer tone.",
        "Focus on saving them work/friction."
    ]
}

def get_product_context_str() -> str:
    """Returns a formatted string of facts for prompt injection."""
    return f"""
    PRODUCT CONTEXT (ABSOLUTE TRUTH):
    - Core Offering: {PRODUCT_FACTS['hook']}
    - Network Access: {PRODUCT_FACTS['networks']}
    - Competitive Edge: {PRODUCT_FACTS['differentiator']}
    
    VOICE CONSTRAINTS:
    - {PRODUCT_FACTS['voice_constraints'][0]}
    - {PRODUCT_FACTS['voice_constraints'][1]}
    - {PRODUCT_FACTS['voice_constraints'][2]}
    """
