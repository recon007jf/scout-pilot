from pydantic_settings import BaseSettings
from pydantic import ValidationError
import os
import sys
from app.utils.logger import get_logger

logger = get_logger("config")

class Settings(BaseSettings):
    # Supabase
    SUPABASE_URL: str
    SUPABASE_KEY: str
    SUPABASE_SERVICE_ROLE_KEY: str | None = None  # Optional

    # AI
    GEMINI_API_KEY: str
    
    # Azure / Outlook
    AZURE_CLIENT_ID: str = ""
    AZURE_CLIENT_SECRET: str = ""
    # "common" = Multi-Tenant. UUID = Single Tenant.
    # Pivot Instruction: Use "common" to avoid AADSTS50020 errors unless strictly internal.
    AZURE_TENANT_ID: str = "common"
    AZURE_REDIRECT_URI: str = "https://scout-backend-prod-283427197752.us-central1.run.app/api/outlook/callback"
    
    # Internal Probe Security
    SCOUT_INTERNAL_PROBE_KEY: str = ""
    
    # External APIs
    SERPER_API_KEY: str = ""

    # Safety Latch (P0.3)
    # If False, EmailEngine returns "Simulated success" and calls to Graph are blocked.
    ALLOW_REAL_SEND: bool = False

    # Environment
    ENV: str = "production" # development, staging, production
    LOG_LEVEL: str = "INFO"

    class Config:
        # Robustly find .env in backend/ or current dir
        env_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
        env_file_encoding = 'utf-8'
        extra = "ignore" # Ignore extra env vars

try:
    settings = Settings()
except ValidationError as e:
    logger.critical(f"Configuration Validation Failed: {e}", extra={"component": "config"})
    sys.exit(1)
