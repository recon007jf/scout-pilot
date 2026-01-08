import hashlib
from typing import Tuple, Dict

def normalize_string(s: str) -> str:
    """Lowercase and strip whitespace."""
    if not s:
        return ""
    return str(s).strip().lower()

def compute_hash_key(first: str, last: str, firm: str) -> str:
    """
    Fallback identity: Hash(First+Last+Firm)
    Used when no Email or LinkedIn is present.
    """
    raw = f"{normalize_string(first)}:{normalize_string(last)}:{normalize_string(firm)}"
    return hashlib.sha256(raw.encode()).hexdigest()

def resolve_identity(row: Dict[str, str]) -> Tuple[str, str]:
    """
    Resolves the 'Diamond Standard' Identity Key.
    
    Precedence:
    1. Work Email (Highest)
    2. LinkedIn URL
    3. Hash(First+Last+Firm)
    
    Returns:
        (identity_key, identity_type)
    """
    # 1. Email Check
    email = normalize_string(row.get("work_email", "")) or normalize_string(row.get("Work Email", ""))
    if email and "@" in email:
        return email, "email"
        
    # 2. LinkedIn Check
    linkedin = normalize_string(row.get("linkedin_url", "")) or normalize_string(row.get("LinkedIn URL", ""))
    if linkedin and "linkedin.com" in linkedin:
        # Simple normalization: Strip query params, protocol
        clean_url = linkedin.split("?")[0].replace("https://", "").replace("http://", "").replace("www.", "").strip("/")
        return clean_url, "linkedin"
        
    # 3. Hash Fallback
    first = row.get("full_name", "").split(" ")[0] if row.get("full_name") else row.get("Full Name", "").split(" ")[0]
    last = row.get("full_name", "").split(" ")[-1] if row.get("full_name") else row.get("Full Name", "").split(" ")[-1]
    firm = row.get("firm", "") or row.get("Firm", "")
    
    return compute_hash_key(first, last, firm), "hash"
