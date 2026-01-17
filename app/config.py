from pydantic_settings import BaseSettings
from pydantic import ValidationError, Field
import os
import sys
from app.utils.logger import get_logger

logger = get_logger("config")


try:
    from dotenv import load_dotenv
    # Explicitly load .env from scratch/backend/.env
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    load_dotenv(env_path)
    logger.info(f"Loaded .env from {env_path}")
except Exception as e:
    logger.critical(f"Env Load Failed: {e}")

class Settings(BaseSettings):
    # Supabase
    SUPABASE_URL: str
    SUPABASE_KEY: str
    SUPABASE_SERVICE_ROLE_KEY: str | None = None  # Optional

    # AI
    GEMINI_API_KEY: str
    GEMINI_MODEL_NAME: str = "gemini-2.0-flash" # Standardized Model (Jan 2026)
    
    # Azure / Outlook
    AZURE_CLIENT_ID: str = os.getenv("AZURE_CLIENT_ID") or os.getenv("OUTLOOK_CLIENT_ID") or ""
    AZURE_CLIENT_SECRET: str = os.getenv("AZURE_CLIENT_SECRET") or os.getenv("OUTLOOK_CLIENT_SECRET") or ""
    # "common" = Multi-Tenant. UUID = Single Tenant.
    # Pivot Instruction: Use "common" to avoid AADSTS50020 errors unless strictly internal.
    AZURE_TENANT_ID: str = "common"
    AZURE_REDIRECT_URI: str = os.getenv("AZURE_REDIRECT_URI") or os.getenv("OUTLOOK_REDIRECT_URI") or "https://scout-backend-prod-283427197752.us-central1.run.app/api/outlook/callback"
    
    # Internal Probe Security
    SCOUT_INTERNAL_PROBE_KEY: str = ""
    SCOUT_INTERNAL_SECRET: str = "" # Required for generate-draft endpoint
    SCOUT_INTERNAL_SECRET_REQUIRED: bool = False # Phase 1: Compat Mode (False). Set True for Phase 2.
    
    # CORS
    ALLOWED_ORIGINS: str = os.getenv("ALLOWED_ORIGINS") or "https://scout-ui.vercel.app,http://localhost:3000,https://v0-scout-ui.vercel.app"
    
    # Clerk Auth
    # Map NEXT_PUBLIC_... to this field
    CLERK_PUBLISHABLE_KEY: str = Field("", validation_alias="NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY")
    CLERK_SECRET_KEY: str = ""
    # In production, we might want to verify against specific issuer
    # CLERK_ISSUER: str = "https://apt-wolf-61.clerk.accounts.dev"
    
    # External APIs
    SERPER_API_KEY: str = ""
    GOOGLE_MAPS_SERVER_KEY: str = "" # Formerly "GOOGLE_MAPS_SERVER_KEY", map to existing env var

    # Safety Latch (P0.3)
    # If False, EmailEngine returns "Simulated success" and calls to Graph are blocked.
    ALLOW_REAL_SEND: bool = False

    # Identity Bridge (Mission Critical for Phase 1 Integration)
    SCOUT_IDENTITY_MODE: str = "secure" # "secure" (default) or "default_user" (bridge)
    DEFAULT_USER_EMAIL: str = "" # Required if mode is default_user
    
    # Environment
    ENV: str = "production" # development, staging, production
    LOG_LEVEL: str = "INFO"

    class Config:
        # Robustly find .env in backend/ or current dir
        env_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
        env_file_encoding = 'utf-8'
        extra = "ignore" # Ignore extra env vars

settings = Settings()

