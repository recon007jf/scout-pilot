import os
import requests
import json
import logging
from modules.circuit_breaker import track_api_cost

# Configure logging
logger = logging.getLogger(__name__)

def search_google(query, location="United States"):
    """
    Searches Google using Serper API.
    If SERPER_API_KEY is not set, returns MOCK DATA.
    """
    api_key = os.getenv("SERPER_API_KEY")
    
    # --- MOCK MODE (If no key) ---
    if not api_key:
        logger.warning("No SERPER_API_KEY found. Using MOCK DATA.")
        return _get_mock_results(query)

    # --- REAL MODE ---
    url = "https://google.serper.dev/search"
    payload = json.dumps({
        "q": query,
        "location": location,
        "num": 10
    })
    headers = {
        'X-API-KEY': api_key,
        'Content-Type': 'application/json'
    }

    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        response.raise_for_status()
        
        # Track Cost (Approx $0.001 per query)
        track_api_cost(0.001)
        
        return response.json()
    except Exception as e:
        logger.error(f"Serper API Error: {e}")
        return {"error": str(e), "organic": []}

def search_news(query, location="United States"):
    """
    Searches Google News using Serper API.
    """
    api_key = os.getenv("SERPER_API_KEY")
    
    if not api_key:
        logger.warning("No SERPER_API_KEY found. Using MOCK NEWS.")
        return _get_mock_news(query)

    url = "https://google.serper.dev/news"
    payload = json.dumps({
        "q": query,
        "location": location,
        "num": 10
    })
    headers = {
        'X-API-KEY': api_key,
        'Content-Type': 'application/json'
    }

    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        response.raise_for_status()
        track_api_cost(0.001)
        return response.json()
    except Exception as e:
        logger.error(f"Serper API Error: {e}")
        return {"error": str(e), "news": []}

# --- MOCK DATA GENERATORS ---

def _get_mock_results(query):
    return {
        "organic": [
            {
                "title": f"Neil Parton - Area Senior Vice President - Gallagher",
                "link": "https://ajg.com/team/neil-parton",
                "snippet": f"Neil Parton is the Area SVP at Gallagher Glendale, specializing in large self-funded groups."
            },
            {
                "title": f"Jane Doe - Principal - Unknown Firm",
                "link": "https://linkedin.com/in/jane-doe",
                "snippet": "Jane Doe is a Principal Consultant specializing in Self-Funded plans and Stop Loss strategy."
            }
        ]
    }

def _get_mock_news(query):
    return {
        "news": [
            {
                "title": "Cigna announces major merger with Humana",
                "link": "https://news.google.com/mock1",
                "snippet": "The healthcare giants are set to merge in a $50B deal...",
                "date": "2 hours ago",
                "source": "Bloomberg"
            },
            {
                "title": "TechCorp appoints new VP of Engineering",
                "link": "https://news.google.com/mock2",
                "snippet": "Sarah Connor joins to lead the AI division...",
                "date": "5 hours ago",
                "source": "TechCrunch"
            }
        ]
    }
