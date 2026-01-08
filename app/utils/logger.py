import logging
import json
import sys
from datetime import datetime
import os

class JsonFormatter(logging.Formatter):
    """
    Formatter that outputs JSON strings after parsing the LogRecord.
    """
    def format(self, record):
        log_record = {
            "severity": record.levelname,
            "message": record.getMessage(),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": getattr(record, "component", "backend"),
            "function": record.funcName,
            "line": record.lineno,
            "correlation_id": getattr(record, "correlation_id", None)
        }
        
        # Add any extra attributes in record.__dict__ that are not default LogRecord attributes
        # (This allows skipping standard attrs if needed, but for now simple structure is cleaner)
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_record)

def get_logger(name: str):
    logger = logging.getLogger(name)
    
    # Clean up existing handlers to avoid duplicates during reloads
    if logger.hasHandlers():
        logger.handlers.clear()
        
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    
    # Set level based on env or default to INFO
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logger.setLevel(log_level)
    
    return logger
