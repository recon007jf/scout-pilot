from enum import Enum

class VerificationStatus(Enum):
    VERIFIED = "VERIFIED"
    SUSPECT = "SUSPECT"
    RAW = "RAW"
    NOT_VERIFIED = "NOT_VERIFIED"  # Replaces QUARANTINED

class DataSource(Enum):
    IMPORTED = "IMPORTED"
    GUESSED = "GUESSED"
    FOUND = "FOUND"

class EmailStatus(Enum):
    VERIFIED_PUBLIC = "VERIFIED_PUBLIC"
    UNVERIFIED_INPUT = "UNVERIFIED_INPUT"
    BOUNCE_RISK = "BOUNCE_RISK"

# Column Headers
COL_VERIFICATION_STATUS = "Verification Status"
COL_CONFIDENCE_SCORE = "Confidence Score"
COL_VALIDATION_LOG = "Validation Log"
COL_EMAIL_STATUS = "Email Status"
COL_DATA_SOURCE = "Data Source"
COL_CLIENT_NAME = "Client Name"
COL_FUNDING_TYPE = "Funding Type"

# List of all forensic columns to ensure they exist
FORENSIC_COLUMNS = [
    COL_VERIFICATION_STATUS,
    COL_CONFIDENCE_SCORE,
    COL_VALIDATION_LOG,
    COL_EMAIL_STATUS,
    COL_DATA_SOURCE,
    COL_CLIENT_NAME,
    COL_FUNDING_TYPE
]
