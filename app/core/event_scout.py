import datetime
from typing import List, Dict, Any
from app.utils.logger import get_logger
from app.config import settings
from supabase import create_client

logger = get_logger("event_scout")

class EventScout:
    """
    The Proactive Context Engine.
    Matches candidates to future industry events based on:
    1. Geography (Local Hook)
    2. Tribe (Affiliation Hook)
    3. Role (Executive Hook)
    """
    
    def __init__(self):
        # Initialize Supabase client for reading the industry_events table
        self.supabase = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)

    def check_events(self, candidate: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Returns a list of 'Hooks' (Events + Why it matters).
        """
        hooks = []
        
        try:
            # 1. Fetch Future Events
            # We only care about events in the future (or very recent past if we want to say 'Hope you enjoyed...')
            # For now, let's just fetch all from our small 2026 table.
            res = self.supabase.table("industry_events").select("*").execute()
            events = res.data
            
            if not events:
                return []

            cand_city = (candidate.get("city") or "").lower()
            cand_state = (candidate.get("state") or "").lower()
            cand_title = (candidate.get("title") or "").lower()
            cand_firm = (candidate.get("firm") or "").lower()
            
            # 2. Iterate and Triangulate
            for event in events:
                match_reason = None
                hook_text = None
                
                event_tags = event.get("match_tags", [])
                
                # --- LOGIC GATE 1: GEO LOOP (The "Local" Hook) ---
                # Exact City Match (Simple for now)
                # TODO: add distance calc later
                if event.get("city", "").lower() == cand_city:
                    match_reason = "LOCAL_HOST"
                    hook_text = f"Since you're based in {event['city']}, are you planning to stop by {event['name']}?"
                
                # --- LOGIC GATE 2: TRIBE LOOP (The "Affiliation" Hook) ---
                # Health Rosetta
                elif "ROSETTA" in event_tags and ("rosetta" in cand_firm or "advisors" in cand_firm):
                    match_reason = "TRIBE_MEMBER"
                    hook_text = f"Will you be joining the other Health Rosetta advisors at {event['name']}?"
                
                # --- LOGIC GATE 3: EXECUTIVE LOOP (The "Peer" Hook) ---
                # CIAB / Executive events
                elif "EXECUTIVE" in event_tags and any(role in cand_title for role in ["ceo", "president", "principal", "founder", "partner"]):
                    match_reason = "PEER_LEADER"
                    hook_text = f"Are you heading to {event['venue']} for {event['name']} this year?"

                # If matched, add to hooks
                if match_reason:
                    hooks.append({
                        "event_name": event["name"],
                        "match_reason": match_reason,
                        "hook_text": hook_text,
                        "event_date": event["start_date"],
                        "venue": event["venue"]
                    })
                    
        except Exception as e:
            logger.error(f"EventScout Check Failed: {e}")
            
        return hooks
