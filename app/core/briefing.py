from supabase import Client
from typing import List, Dict
import datetime
from app.utils.logger import get_logger

logger = get_logger("briefing")

class BriefingEngine:
    def __init__(self, db: Client):
        self.db = db

    def generate_briefing(self, user_email: str = None, enable_high_risk: bool = False) -> Dict:
        """
        New Architecture (Jan 17):
        Reads pre-selected candidates from 'morning_briefing_queue'.
        NO runtime selection. NO runtime generation.
        """
        briefing_targets = []
        tier_counts = {"Warm": 0, "Cold-Safe": 0, "High-Risk": 0}
        
        today_str = datetime.date.today().isoformat()
        
        # 1. Fetch from Queue (The Source of Truth)
        # We join 'candidates' to get the details.
        try:
            res = self.db.table("morning_briefing_queue") \
                .select("*, candidates(*)") \
                .eq("selected_for_date", today_str) \
                .eq("status", "pending") \
                .limit(10) \
                .execute()
                
            queue_items = res.data or []
            
            # 2. Transform to Frontend Contract
            # Frontend expects: full_name, firm, role, risk_profile, etc.
            # We map from 'candidates' -> root keys.
            
            for item in queue_items:
                candidate = item.get("candidates", {})
                
                # Synthesis of Legacy Fields
                # We default to "Warm" because these were pre-selected by the Algo.
                # Future: Map item['priority_score'] to buckets.
                risk_bucket = "Warm" 
                tier_counts[risk_bucket] += 1
                
                target = {
                    "id": candidate.get("id"), # Use Candidate ID as primary ref
                    "full_name": candidate.get("full_name"),
                    "firm": candidate.get("firm"),
                    "role": candidate.get("role"),
                    "email": candidate.get("email"),
                    "linkedin_url": candidate.get("linkedin_url"),
                    "risk_profile": risk_bucket,
                    
                    # Logic Proofs (The "Why")
                    "ranking_reason": item.get("ranking_reason"), 
                    
                    # Draft Content (The Contract)
                    # We pass it as 'dossiers' because legacy UI might look there,
                    # OR we pass it as 'draft_data'. 
                    # Providing both for safety.
                    "draft_body": candidate.get("draft_body") or item.get("draft_preview"),
                    "dossiers": { 
                        "id": "mock_v0", # Prevent crash if UI checks ID
                        "generated_draft": candidate.get("draft_body") or item.get("draft_preview")
                    }
                }
                briefing_targets.append(target)
                
        except Exception as e:
            logger.error(f"Briefing Generation Failed: {e}")
            # Fail Gracefully -> Empty Briefing
            
        return {
            "targets": briefing_targets,
            "meta": {
                "total": len(briefing_targets),
                "counts": tier_counts,
                "source": "morning_briefing_queue"
            }
        }
