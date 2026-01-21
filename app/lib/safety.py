
# lib/safety.py
def sanitize_external_string_for_db(raw_text: str, max_len: int = 500) -> str | None:
    """
    PLATFORM CONSTRAINT: supabase-py cannot handle large payloads.
    All external strings (URLs, HTML, Tokens) MUST be sanitized here.
    """
    if not raw_text:
        return None
    
    # 1. Strip Query Params (The usual culprit)
    # EXCEPT for LinkedIn Images (licdn.com) which REQUIRE tokens (?e=..., ?t=...)
    if "licdn.com" in raw_text:
        clean_text = raw_text
    else:
        clean_text = raw_text.split('?')[0]
    
    # 2. Hard Truncate (The safety net)
    if len(clean_text) > max_len:
        return clean_text[:max_len]
        
    return clean_text
