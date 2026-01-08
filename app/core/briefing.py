from supabase import Client
from typing import List, Dict
from app.utils.logger import get_logger

logger = get_logger("briefing")

class BriefingEngine:
    def __init__(self, db: Client):
        self.db = db

    def generate_briefing(self, enable_high_risk: bool = False) -> Dict:
        """
        Strict Ranking:
        1. Warm
        2. Cold-Safe
        3. High-Risk (Only if enabled)
        """
        briefing_targets = []
        tier_counts = {"Warm": 0, "Cold-Safe": 0, "High-Risk": 0}
        
        # 1. Fetch Warm
        warm = self.db.table("psyche_profiles").select("*, dossiers(*)").eq("risk_profile", "Warm").limit(5).execute()
        for row in warm.data:
            briefing_targets.append(row)
            tier_counts["Warm"] += 1
            
        # 2. Fetch Cold-Safe (if space)
        remaining = 10 - len(briefing_targets)
        if remaining > 0:
            cold = self.db.table("psyche_profiles").select("*, dossiers(*)").eq("risk_profile", "Cold-Safe").limit(remaining).execute()
            for row in cold.data:
                briefing_targets.append(row)
                tier_counts["Cold-Safe"] += 1
                
        # 3. Fetch High-Risk (if enabled & space)
        remaining = 10 - len(briefing_targets)
        if enable_high_risk and remaining > 0:
            risky = self.db.table("psyche_profiles").select("*, dossiers(*)").eq("risk_profile", "High-Risk").limit(remaining).execute()
            for row in risky.data:
                briefing_targets.append(row)
                tier_counts["High-Risk"] += 1
                
        return {
            "targets": briefing_targets,
            "meta": {
                "total": len(briefing_targets),
                "counts": tier_counts
            }
        }
