import os
import json
from datetime import datetime, date
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

COST_FILE = "cost_tracker.json"
DAILY_SOFT_CAP = 15.0
MONTHLY_HARD_CAP = 200.0

class CircuitBreaker:
    def __init__(self):
        self.cost_file = COST_FILE
        self._load_costs()

    def _load_costs(self):
        if os.path.exists(self.cost_file):
            with open(self.cost_file, 'r') as f:
                self.costs = json.load(f)
        else:
            self.costs = {"daily": {}, "monthly": {}}

    def _save_costs(self):
        with open(self.cost_file, 'w') as f:
            json.dump(self.costs, f)

    def get_daily_spend(self):
        today = str(date.today())
        return self.costs["daily"].get(today, 0.0)

    def get_monthly_spend(self):
        month = date.today().strftime("%Y-%m")
        return self.costs["monthly"].get(month, 0.0)

    def track_cost(self, amount):
        today = str(date.today())
        month = date.today().strftime("%Y-%m")

        self.costs["daily"][today] = self.costs["daily"].get(today, 0.0) + amount
        self.costs["monthly"][month] = self.costs["monthly"].get(month, 0.0) + amount
        self._save_costs()

    def check_limits(self):
        daily_spend = self.get_daily_spend()
        monthly_spend = self.get_monthly_spend()

        if monthly_spend > MONTHLY_HARD_CAP:
            logger.critical(f"MONTHLY HARD CAP EXCEEDED: ${monthly_spend} > ${MONTHLY_HARD_CAP}. STOPPING.")
            return False, "MONTHLY_CAP_EXCEEDED"
        
        if daily_spend > DAILY_SOFT_CAP:
            logger.warning(f"DAILY SOFT CAP EXCEEDED: ${daily_spend} > ${DAILY_SOFT_CAP}. Sending Alert.")
            # In a real app, this would trigger an email alert.
            pass

        return True, "OK"

# Singleton instance
circuit_breaker = CircuitBreaker()

def check_circuit_breaker():
    """
    Module 5: CIRCUIT BREAKERS (Financial Safety)
    Returns True if safe to proceed, False otherwise.
    """
    is_safe, status = circuit_breaker.check_limits()
    return is_safe

def track_api_cost(cost):
    """
    Tracks the cost of an API call.
    """
    circuit_breaker.track_cost(cost)
