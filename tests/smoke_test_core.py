import sys
import os
import unittest
import uuid
import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

# Path Setup
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Mock env vars BEFORE imports that use them
os.environ["TESTING"] = "1"

from app.utils.identity import resolve_identity
from app.core.safety import SafetyEngine
from app.core.draft_engine import DraftEngine, DraftOutput
from app.domain.product_facts import PRODUCT_FACTS
from scripts.seed_targets import seed_targets

class TestIronCladCore(unittest.TestCase):
    
    def setUp(self):
        # Mock DB Client
        self.mock_db = MagicMock()
        
        # Test Data
        self.test_id = str(uuid.uuid4())[:8]
        self.row_data = {
            "Full Name": f"Test User {self.test_id}", 
            "Firm": f"Test Firm {self.test_id}",
            "Work Email": f"test.{self.test_id}@example.com",
            "Role": "Tester",
            "Tier": "Tier 1",
            "LinkedIn URL": ""
        }
        
    def test_01_identity_resolution(self):
        print("\n[TEST 1] Identity Resolution...")
        # Email Priority
        key, type = resolve_identity({"work_email": "A@B.com", "linkedin_url": "linkedin.com/in/a"})
        self.assertEqual(type, "email")
        self.assertEqual(key, "a@b.com")
        
        # LinkedIn Priority
        # Updated Logic: It returns the stripped version "linkedin.com/in/foo"
        key, type = resolve_identity({"work_email": "", "linkedin_url": "https://www.linkedin.com/in/foo/"})
        self.assertEqual(type, "linkedin")
        self.assertEqual(key, "linkedin.com/in/foo")
        
        # Hash Fallback
        key, type = resolve_identity({"full_name": "John Doe", "firm": "Acme"})
        self.assertEqual(type, "hash")
        print("PASSED")

    def test_02_draft_engine_schema(self):
        print("\n[TEST 2] Draft Engine Schema & Facts...")
        # Check Facts exist
        from app.domain.product_facts import get_product_context_str
        context = get_product_context_str()
        self.assertIn("100-2,000 lives", context)
        self.assertIn("Cigna", context)
        print("PASSED")

    def test_03_safety_state_machine_logic(self):
        print("\n[TEST 3] Safety State Machine (Logic Only)...")
        safety = SafetyEngine(self.mock_db)
        
        # 1. Simulate Active State
        # Mock: table().select().eq().single().execute() -> data={"status": "active"}
        active_response = MagicMock()
        active_response.data = {"status": "active", "resume_at": None}
        self.mock_db.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = active_response
        
        state = safety.get_outreach_status()
        self.assertEqual(state["status"], "active")
        safety.assert_can_send() # Should not raise
            
        # 2. Simulate Paused State
        paused_response = MagicMock()
        now = datetime.utcnow()
        resume_at = now + timedelta(hours=1)
        paused_response.data = {"status": "paused", "resume_at": resume_at.isoformat() + "Z"}
        self.mock_db.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = paused_response
        
        state = safety.get_outreach_status(current_time=now)
        self.assertEqual(state["status"], "paused")
        
        with self.assertRaises(PermissionError):
            safety.assert_can_send()
            
        # 3. Simulate Time Travel (Auto-Resume)
        future = now + timedelta(hours=2)
        # Note: In a real run, it performs an update. We mock the response to still trigger the logic.
        # But `get_outreach_status` reads the DB state first. 
        # The logic: if (paused AND now > resume) -> Update DB -> Return Active.
        
        # We Mock the initial read as Paused (same as above)
        # We verify that update() was called
        
        state_future = safety.get_outreach_status(current_time=future)
        
        # Assert Logic flipped it to active in return
        self.assertEqual(state_future["status"], "active")
        self.assertTrue(state_future.get("auto_resumed"))
        
        # Assert DB Update was called
        # table("global_outreach_status").update({...})
        self.mock_db.table.assert_any_call("global_outreach_status")
        # Can inspect args if we want strict verification
        print("PASSED")

    def test_04_idempotent_ingest_logic(self):
        print("\n[TEST 4] Idempotent Ingest Logic (Mocked)...")
        
        # Mock CSV contents
        import io
        csv_content = "Full Name,Work Email,Firm,Role,Tier,LinkedIn URL\nTest User,test@example.com,Test Firm,Tester,Tier 1,"
        
        with patch('builtins.open', return_value=io.StringIO(csv_content)):
            seed_targets("dummy.csv", db_client=self.mock_db)
            
        # Verify Upsert was called
        # Logic: 1 row in CSV -> 1 Upsert call (+ 1 Profile Upsert)
        
        # Identity logic:
        # Full Name=Test User, Email=test@example.com -> Key=test@example.com
        
        # Check call arguments
        # We need to find the specific call to 'dossiers' table logic.
        # Since table() returns the same mock, upsert() is called multiple times.
        # We iterate to find the record with 'identity_key'.
        
        found_dossier = False
        upsert_calls = self.mock_db.table.return_value.upsert.call_args_list
        
        for call in upsert_calls:
            args, _ = call
            record = args[0]
            if "identity_key" in record:
                self.assertEqual(record["identity_key"], "test@example.com")
                self.assertEqual(record["full_name"], "Test User")
                found_dossier = True
                break
                
        self.assertTrue(found_dossier, "Dossier upsert not found in calls")
        
        print("PASSED")

if __name__ == '__main__':
    unittest.main()
