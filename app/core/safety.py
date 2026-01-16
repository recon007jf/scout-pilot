from datetime import datetime
from supabase import Client
from app.utils.logger import get_logger

logger = get_logger("safety")

class SafetyEngine:
    """
    Enforces the 'Global Safety Brake'.
    State Machine: Active <-> Paused
    Deterministic: Checks resume_at vs current_time.
    """
    
    def __init__(self, db: Client):
        self.db = db

    def get_outreach_status(self, current_time: datetime = None, user_email: str = None):
        """
        Returns the current global status.
        Auto-resumes if paused and now > resume_at.
        Includes Outlook connection status if user_email is provided.
        """
        if not current_time:
            current_time = datetime.utcnow() # timezone naive assumption for MVP, UTC

        # Fetch Singleton
        try:
            response = self.db.table("global_outreach_status").select("*").eq("id", 1).single().execute()
            if not response.data:
                # Should have been seeded, but fail safe
                return {"status": "paused", "reason": "System Init Failure"}
            
            state = response.data
            status = state.get("status")
            resume_at_str = state.get("resume_at")
            
            # Auto-Resume Logic
            if status == "paused" and resume_at_str:
                # Parse resume_at
                try:
                    resume_at = datetime.fromisoformat(resume_at_str.replace('Z', '+00:00'))
                    # Naive vs Aware check - for now assume UTC if Z present
                    # If current_time is passed as UTC naive, ensure resume_at is compatible
                    # Ideally use aware everywhere, but MVP shortcut:
                    if resume_at.tzinfo and not current_time.tzinfo:
                         # make resume_at naive UTC
                         resume_at = resume_at.replace(tzinfo=None)
                    
                    if current_time >= resume_at:
                        logger.info("Auto-Resuming Outreach", extra={"trigger": "timer", "resume_at": str(resume_at)})
                        # Perform Write to DB to flip status
                        self.db.table("global_outreach_status").update({
                            "status": "active",
                            "resume_at": None,
                            "paused_at": None,
                            "updated_by": "system_auto_resume"
                        }).eq("id", 1).execute()
                        return {"status": "active", "auto_resumed": True}
                        
                except Exception as e:
                    logger.error(f"Date Parsing Error in Safety: {e}")
                    # Fail closed
                    return {"status": "paused", "error": str(e)}

            # Check Outlook Status if user_email provided
            outlook_connected = False
            if user_email:
                 # Check integration_tokens table
                 # We are in SafetyEngine, self.db is admin_db (Service Role) so we can read this table
                 token_res = self.db.table("integration_tokens").select("id").eq("user_email", user_email).eq("provider", "outlook").execute()
                 if token_res.data:
                     outlook_connected = True

            state["outlook_connected"] = outlook_connected
            return state
            
        except Exception as e:
            logger.critical(f"Safety Check Failed: {e}")
            # Fail Closed
            return {"status": "paused", "error": "DB Connection Failed"}

    def assert_can_send(self) -> bool:
        """
        Hard gate. Raises exception if not active.
        """
        state = self.get_outreach_status()
        if state.get("status") != "active":
            raise PermissionError(f"GLOBAL OUTREACH PAUSED. Reason: {state.get('pause_reason')}")
        return True
