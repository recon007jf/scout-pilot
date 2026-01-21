import os
import json
import requests
from typing import List, Dict, Any, Optional
from tavily import TavilyClient
from app.core.llm import LLMClient
from app.utils.logger import get_logger

logger = get_logger("signal_analyst")

class SignalAnalyst:
    """
    The Newsroom Orchestrator.
    Waterfall:
    1. Serper (Scanner) -> "Is there volume?"
    2. Tavily (Researcher) -> "Get the text." (Only if volume found)
    3. LLM (Analyst) -> "What does it mean?"
    """
    
    def __init__(self):
        self.serper_key = os.environ.get("SERPER_API_KEY")
        self.tavily_key = os.environ.get("TAVILY_API_KEY")
        self.llm = LLMClient()
        self.tavily = TavilyClient(api_key=self.tavily_key) if self.tavily_key else None

    def scan_and_analyze(self, firm_name: str) -> List[Dict[str, Any]]:
        """
        Main entry point. Returns a list of processed signals ready for the DB.
        """
        logger.info(f"SignalAnalyst: Scanning for {firm_name}...")
        
        # 1. SCAN (Serper)
        # We look for recent high-impact news.
        if not self.serper_key:
            logger.warning("No SERPER_API_KEY. Skipping Scan.")
            return []

        # Query optimized for business impact
        query = f"{firm_name} insurance acquisition funding merger regulatory lawsuit"
        
        try:
            # Short timeout, we want speed
            url = "https://google.serper.dev/news"
            payload = json.dumps({"q": query, "num": 5, "tbs": "qdr:m"}) # qdr:m = past month
            headers = {'X-API-KEY': self.serper_key, 'Content-Type': 'application/json'}
            
            response = requests.request("POST", url, headers=headers, data=payload)
            results = response.json().get("news", [])
            
            if not results:
                logger.info("SignalAnalyst: No noise found on Serper. Stopping early.")
                return []
                
        except Exception as e:
            logger.error(f"Serper Scan Failed: {e}")
            return []

        # 2. RESEARCH (Tavily)
        # If we got here, we have potential/noise.
        # We pass the BEST URL to Tavily to extract content, OR we ask Tavily to search specifically if Serper was vague.
        # Strategy: Use Tavily search directly on the most interesting Headline to get context.
        
        valid_signals = []
        
        for item in results[:2]: # Limit to top 2 to save costs/time
            title = item.get("title")
            link = item.get("link")
            
            # Filter dumb stuff
            if "linkedin.com" in link or "facebook.com" in link:
                continue

            # 3. ANALYZE (LLM)
            # Fetch content via Tavily extract (or search context)
            content_text = self._get_context_from_tavily(link, title)
            
            if not content_text:
                continue
                
            intelligence = self._consult_llm(firm_name, title, content_text)
            
            if intelligence.get("impact_rating", "IGNORE") != "IGNORE":
                valid_signals.append({
                    "title": title,
                    "url": link,
                    "source": item.get("source"),
                    "published_at": item.get("date"),
                    "signal_type": intelligence.get("signal_type", "NEWS"),
                    "relevance_score": intelligence.get("relevance_score", 0),
                    "impact_rating": intelligence.get("impact_rating"),
                    "analysis": intelligence.get("analysis"),
                    "action_suggested": intelligence.get("action_suggested"),
                    "raw_content": content_text[:1000] # Truncate for storage
                })

        return valid_signals

    def _get_context_from_tavily(self, url: str, query: str) -> Optional[str]:
        """
        Uses Tavily to extract clean text from the URL.
        Fallback: Search context if extract fails.
        """
        if not self.tavily:
            return None
            
        try:
            # "extract" is cheaper/faster if we trust the URL
            # But the Python SDK uses 'extract' differently.
            # Simplified: Use search with the URL as query, or use extract features.
            
            # Using the extract feature of Tavily is best for "Reading"
            # Assuming SDK supports extract, otherwise we use search(url)
            response = self.tavily.extract(urls=[url])
            
            results = response.get("results", [])
            if results and results[0].get("raw_content"):
                 return results[0].get("raw_content")
                 
            return None
            
        except Exception as e:
            logger.warning(f"Tavily Extract Failed: {e}")
            return None

    def _consult_llm(self, firm: str, title: str, content: str) -> Dict[str, Any]:
        """
        The Analyst Judge.
        """
        
        system_prompt = f"""
        You are an Intelligence Analyst for a Benefits Brokerage. 
        Your job is to determine if a news item regarding the firm '{firm}' is MATERIAL to a sales conversation.
        
        MATERIAL SIGNALS:
        - Mergers & Acquisitions (Buying or Selling)
        - PE Funding / Investment
        - Regulatory Fines or Lawsuits
        - Major Executive Changes (CEO/CFO)
        - Rapid Growth / Expansion
        
        IGNORE:
        - Routine promotions
        - "Best Places to Work" awards
        - Generic fluff
        - Content not actually about {firm} (False positives)
        
        Return JSON:
        {{
            "signal_type": "M&A" | "FUNDING" | "REGULATORY" | "PERSONNEL" | "NOISE",
            "relevance_score": 0-100,
            "impact_rating": "HIGH" | "MEDIUM" | "LOW" | "IGNORE",
            "analysis": "One sentence explanation of why this matters.",
            "action_suggested": "One sentence on how to use this in a cold email."
        }}
        """
        
        user_text = f"HEADLINE: {title}\n\nCONTENT: {content[:4000]}" # Limit context window
        
        return self.llm.analyze_text(system_prompt, user_text, json_schema={
            "signal_type": "NOISE", "relevance_score": 0, "impact_rating": "IGNORE", "analysis": "", "action_suggested": ""
        })
