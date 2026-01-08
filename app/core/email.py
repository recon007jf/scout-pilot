from supabase import Client
from typing import Dict, Any, List
from app.utils.logger import get_logger
from app.core.outlook import OutlookAuth, OutlookClient
from app.core.safety import SafetyEngine

logger = get_logger("email")

class EmailEngine:
    def __init__(self, db: Client):
        self.db = db
        self.safety = SafetyEngine(db)

    def send_email(self, user_email: str, candidate_id: str, subject: str, body: str) -> Dict[str, Any]:
        """
        Executes Direct Send via Microsoft Graph.
        Hard Gate: Safety Check.
        """
        # 1. SAFETY INVARIANT
        try:
            self.safety.assert_can_send()
        except PermissionError as e:
            logger.warning(f"Send Blocked by Safety: {e}")
            raise e # Return 403 in Main
            
        try:
            # 2. Authenticate
            res = self.db.table("integration_tokens").select("*").eq("user_email", user_email).eq("provider", "outlook").execute()
            if not res.data:
                return {"status": "failed", "reason": "No Connect Token"}
                
            record = res.data[0]
            auth = OutlookAuth()
            
            # Refresh Token Logic (Always refresh before send to be safe)
            if record.get("refresh_token"):
                 new_tokens = auth.refresh_token(record["refresh_token"])
                 access_token = new_tokens.get("access_token")
                 self.db.table("integration_tokens").update({
                     "access_token": access_token,
                     "refresh_token": new_tokens.get("refresh_token")
                 }).eq("id", record["id"]).execute()
            else:
                 access_token = record.get("access_token")

            client = OutlookClient(access_token)

            # 3. Get Recipient
            d_res = self.db.table("dossiers").select("work_email").eq("id", candidate_id).execute()
            if not d_res.data:
                return {"status": "failed", "reason": "Candidate Not Found"}
            
            target_email = d_res.data[0].get("work_email")
            if not target_email:
                 return {"status": "failed", "reason": "No Email Address"}

            # P0.3 SAFETY LATCH FOR TESTING (Revised P0.5)
            from app.config import settings
            if not settings.ALLOW_REAL_SEND:
                logger.warning(f"SAFETY LATCH: Blocked email to {target_email} Subject: {subject}")
                # Fail Closed: Block the Write.
                raise PermissionError("Send blocked by safety latch (ALLOW_REAL_SEND=False)")

            # 4. SEND (The Trigger)
            client.send_email(
                subject=subject,
                body=body,
                to_emails=[target_email]
            )
            
            # 5. Update State
            # Canonical
            self.db.table("draft_decisions").insert({
                "dossier_id": candidate_id,
                "decision": "contacted",
                "draft_content": "Direct Send: " + subject
            }).execute()
            
            # Queue
            self.db.table("morning_briefing_queue").update({"status": "sent"}).eq("dossier_id", candidate_id).execute()
            
            return {"status": "sent"}

        except Exception as e:
            logger.error(f"Email Send Failed: {e}")
            return {"status": "error", "message": str(e)}
