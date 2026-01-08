from supabase import Client
from typing import Dict, Any, List
from app.utils.logger import get_logger

logger = get_logger("refinery")

class RefineryEngine:
    def __init__(self, db: Client):
        self.db = db

    def run_refinery_job(self, limit: int = 50) -> Dict[str, Any]:
        """
        Scan Dossiers -> Select Top Candidates -> Push to Queue.
        Criteria:
        1. Active
        2. Warm or High-Risk
        3. Has Email
        4. Not already queued (Unique constraint handles this but we filter to save ops)
        """
        try:
            # 1. Fetch Candidates
            # We fetch a larger batch to filter in memory for JSONB fields
            res = self.db.table("dossiers")\
                .select("id, full_name, work_email, raw_data, psyche_profiles!inner(risk_profile)")\
                .eq("is_active", True)\
                .neq("work_email", "")\
                .limit(500)\
                .execute()
                
            candidates = res.data
            queued_count = 0
            
            for cand in candidates:
                if queued_count >= limit:
                    break
                    
                raw = cand.get("raw_data", {})
                
                # REFINERY LOGIC UPDATE (Jan 7, 2026):
                # 1. Must be IN_TERRITORY
                # 2. Must have High Funding Confidence
                
                firm_state = raw.get("firm_state_class")
                funding_conf = raw.get("Funding_Confidence")
                
                if firm_state != "IN_TERRITORY":
                    continue
                    
                if funding_conf != "High":
                    continue
                
                try:
                    payload = {
                        "dossier_id": cand["id"],
                        "status": "pending",
                        "priority_score": 100 # simplified priority
                    }
                    
                    self.db.table("morning_briefing_queue").insert(payload).execute()
                    queued_count += 1
                except Exception as insert_e:
                    # Likely duplicate
                    continue

            logger.info(f"Refinery Job Complete. Queued: {queued_count}")
            return {"status": "success", "queued": queued_count}
            
        except Exception as e:
            logger.error(f"Refinery Failed: {e}")
            return {"status": "error", "message": str(e)}
