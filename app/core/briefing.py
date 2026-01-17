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
                
                # Phase 2: Schema Alignment with Frontend (BriefingTarget)
                # We synthesis missing fields (Sponsor/Persona) with safe defaults to unblock UI.
                
                target = {
                    "targetId": candidate.get("id"), # Frontend expects targetId
                    "broker": {
                         "name": candidate.get("full_name"),
                         "title": candidate.get("role") or "Unknown Role",
                         "firm": candidate.get("firm") or "Unknown Firm",
                         "email": candidate.get("email") or "",
                         "phone": "",
                         "linkedIn": candidate.get("linkedin_url") or "",
                         "avatar": candidate.get("linkedin_image_url") or "", # Use Image as Avatar
                         "imageUrl": candidate.get("linkedin_image_url") # Explicit Field
                    },
                    "sponsor": {
                        "name": candidate.get("firm"), # Sponsor often == Firm for simple matching
                        "industry": "Insurance", # Default
                        "revenue": "Unknown",
                        "employees": 0,
                        "location": "Unknown"
                    },
                    "businessPersona": {
                        "type": "Standard Profile", # Default
                        "description": item.get("ranking_reason") or "Algorithm Match",
                        "decisionStyle": "Analytical",
                        "communicationPreference": "Email"
                    },
                    "dossier": { 
                        "lastContact": "Never",
                        "relationshipScore": 50,
                        "previousInteractions": 0,
                        "keyNotes": []
                    },
                    "signals": [],  # Empty for Phase 1
                    "draft": {
                        "subject": (candidate.get("draft_body") or "Subject: Connect").split("\n")[0],
                        "body": candidate.get("draft_body") or item.get("draft_preview") or "",
                        "generatedAt": item.get("created_at"),
                        "version": 1
                    },
                    "status": "pending_review", # Default State
                    "priority": item.get("priority_score") or 50,
                    "createdAt": item.get("created_at")
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
