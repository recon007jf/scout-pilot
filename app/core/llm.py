import os
import json
from typing import Dict, Any, Optional
import google.generativeai as genai
from app.utils.logger import get_logger

logger = get_logger("llm_core")

class LLMClient:
    def __init__(self):
        self.api_key = os.environ.get("GEMINI_API_KEY")
        if not self.api_key:
            logger.warning("GEMINI_API_KEY not found. LLM features will fail.")
            self.model = None
        else:
            genai.configure(api_key=self.api_key)
            # Use a model that supports JSON mode if possible, or standard
            self.model = genai.GenerativeModel('gemini-2.0-flash')

    def analyze_text(self, system_prompt: str, user_text: str, json_schema: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Analyzes text and returns structured JSON.
        """
        if not self.model:
            return {"error": "No LLM Client configured"}

        try:
            # Gemini Check: Combine System + User for simple prompting
            # Or use system_instruction if supported (it is in newer versions)
            
            prompt = f"{system_prompt}\n\n---\n\n{user_text}"
            
            generation_config = {"temperature": 0.0}
            if json_schema:
                generation_config["response_mime_type"] = "application/json"
                # Append schema to prompt to ensure adherence
                prompt += f"\n\nReturn valid JSON matching this schema: {json.dumps(json_schema)}"

            response = self.model.generate_content(
                prompt,
                generation_config=generation_config
            )
            
            content = response.text
            
            if json_schema:
                try:
                    # Clean markdown if present
                    if content.startswith("```json"):
                        content = content.replace("```json", "").replace("```", "")
                    
                    return json.loads(content)
                except json.JSONDecodeError:
                    logger.error(f"LLM returned invalid JSON: {content}")
                    return {"error": "Invalid JSON response", "raw": content}
                    
            return {"content": content}

        except Exception as e:
            logger.error(f"LLM Analysis Failed: {e}")
            return {"error": str(e), "impact_rating": "IGNORE"}
