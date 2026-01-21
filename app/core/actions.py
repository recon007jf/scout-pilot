from supabase import Client
from typing import Dict, Any, Optional
from app.utils.logger import get_logger
from app.core.outlook import OutlookAuth, OutlookClient

logger = get_logger("actions")

class DraftActionsEngine:
    def __init__(self, db: Client):
        self.db = db

    def handle_action(self, dossier_id: str, action: str, user_email: str, draft_content: Optional[str] = None, draft_subject: Optional[str] = "Introduction") -> Dict[str, Any]:
        """
        Handles Approve/Dismiss/Pause actions.
        - Approve: Logs decision, Creates Outlook Draft (if connected).
        - Dismiss: Logs decision, Updates Dossier -> Inactive.
        - Pause: Logs decision.
        """
        try:
            # 1. Log Decision
            decision_payload = {
                "dossier_id": dossier_id,
                "decision": action,
                "draft_content": draft_content
            }
            self.db.table("draft_decisions").insert(decision_payload).execute()
            
            # 1b. Update Candidate Draft Content (Crucial for Edit -> Approve flow)
            if draft_content:
                 self.db.table("candidates").update({
                     "draft_subject": draft_subject,
                     "draft_body": draft_content
                 }).eq("id", dossier_id).execute()
            
            # 2. Handle Side Effects
            if action == "dismiss":
                self.db.table("dossiers").update({"is_active": False}).eq("id", dossier_id).execute()
                return {"status": "dismissed", "dossier_id": dossier_id}
                
            elif action == "approved":
                # Create Real Outlook Draft
                # Need access token
                try:
                    res = self.db.table("integration_tokens").select("*").eq("user_email", user_email).eq("provider", "outlook").execute()
                    if res.data:
                        record = res.data[0]
                        
                        # Refresh Token logic (Simplified, ideally shared with main.py logic)
                        auth = OutlookAuth()
                        if record.get("refresh_token"):
                             new_tokens = auth.refresh_token(record["refresh_token"])
                             access_token = new_tokens.get("access_token")
                             # Update DB
                             self.db.table("integration_tokens").update({
                                 "access_token": access_token, 
                                 "refresh_token": new_tokens.get("refresh_token")
                             }).eq("id", record["id"]).execute()
                        else:
                             access_token = record.get("access_token")

                        client = OutlookClient(access_token)
                        
                        # Get Dossier Email
                        d_res = self.db.table("dossiers").select("work_email").eq("id", dossier_id).execute()
                        to_email = ""
                        if d_res.data:
                            to_email = d_res.data[0].get("work_email", "")

                        if to_email:
                            draft = client.create_draft(
                                subject=draft_subject,
                                body=draft_content or "",
                                to_emails=[to_email]
                            )
                            return {"status": "approved", "outlook_draft_id": draft.get("id")}
                        else:
                            return {"status": "approved", "warning": "No email found for dossier"}
                    else:
                        return {"status": "approved", "warning": "Outlook not connected"}
                
                except Exception as e:
                    logger.error(f"Outlook Draft Creation Failed: {e}")
                    return {"status": "approved", "warning": f"Outlook Error: {str(e)}"}

            return {"status": action}
            
        except Exception as e:
            logger.error(f"Draft Action Error: {e}")
            return {"error": str(e)}
