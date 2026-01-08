from typing import Dict, List, Optional
import msal
import requests
from app.config import settings
from app.utils.logger import get_logger

logger = get_logger("outlook")

class OutlookAuth:
    """Handles OAuth2 Flow with Microsoft Graph."""
    
    SCOPES = ["User.Read", "Mail.ReadWrite", "Mail.Send"]
    # Force common if not explicitly set to specific tenant ID, or rely on config but warn.
    # Architecture Pivot: Prefer 'common' for multi-tenant SaaS.
    AUTHORITY = f"https://login.microsoftonline.com/{settings.AZURE_TENANT_ID}"

    def __init__(self):
        self.app = msal.ConfidentialClientApplication(
            settings.AZURE_CLIENT_ID,
            authority=self.AUTHORITY,
            client_credential=settings.AZURE_CLIENT_SECRET
        )

    def get_auth_url(self) -> str:
        return self.app.get_authorization_request_url(
            self.SCOPES,
            redirect_uri=settings.AZURE_REDIRECT_URI
        )

    def acquire_token_by_code(self, code: str) -> Dict:
        result = self.app.acquire_token_by_authorization_code(
            code,
            scopes=self.SCOPES,
            redirect_uri=settings.AZURE_REDIRECT_URI
        )
        if "error" in result:
            logger.error(f"Auth Error: {result.get('error_description')}")
            raise Exception(result.get("error_description"))
        return result

    def refresh_token(self, refresh_token: str) -> Dict:
        result = self.app.acquire_token_by_refresh_token(
            refresh_token,
            scopes=self.SCOPES
        )
        if "error" in result:
             logger.error(f"Refresh Error: {result.get('error_description')}")
             raise Exception("Failed to refresh token")
        return result


class OutlookClient:
    """Wrapper for Microsoft Graph API."""
    
    BASE_URL = "https://graph.microsoft.com/v1.0"

    def __init__(self, access_token: str):
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

    def get_me(self) -> Dict:
        resp = requests.get(f"{self.BASE_URL}/me", headers=self.headers)
        resp.raise_for_status()
        return resp.json()

    def create_draft(self, subject: str, body: str, to_emails: List[str]) -> Dict:
        """Creates a draft email (does not send)."""
        payload = {
            "subject": subject,
            "importance": "Low",
            "body": {
                "contentType": "HTML",
                "content": body
            },
            "toRecipients": [
                {"emailAddress": {"address": email}} for email in to_emails
            ]
        }
        
        resp = requests.post(f"{self.BASE_URL}/me/messages", headers=self.headers, json=payload)
        resp.raise_for_status()
        return resp.json()

    def send_email(self, subject: str, body: str, to_emails: List[str]) -> None:
        """Sends an email immediately (Direct Send)."""
        payload = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": "HTML",
                    "content": body
                },
                "toRecipients": [
                    {"emailAddress": {"address": email}} for email in to_emails
                ]
            },
            "saveToSentItems": "true"
        }
        
        resp = requests.post(f"{self.BASE_URL}/me/sendMail", headers=self.headers, json=payload)
        resp.raise_for_status()

