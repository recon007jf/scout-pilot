from app.core.liveness import EmploymentLivenessCheck

def test_liveness():
    checker = EmploymentLivenessCheck()
    
    # 1. Real Case (Should Pass)
    # Assuming Scott Wood is still at Alera Group
    c1 = {"full_name": "Scott Wood", "firm": "Alera Group"}
    print(f"Testing {c1['full_name']} @ {c1['firm']}...")
    res1 = checker.check_status(c1)
    print(f"  Departure? {res1['is_departure']} ({res1['risk_reason']})\n")
    
    # 2. Known "Former" (Simulated Check)
    # We need a real person who left.
    # Let's try "John Doe" at "Enron" (unlikely to work perfectly, but let's try a real recent departure if we knew one).
    # Since we can't easily validly test a 'Former' without a real example, 
    # we will rely on the unit test passing the HAPPY path for now, 
    # and manually verification of the REGEX logic if needed.
    
    # Actually, let's try searching for someone we know left.
    # "Sheryl Sandberg Meta" -> Should say Former.
    c2 = {"full_name": "Sheryl Sandberg", "firm": "Meta"} 
    print(f"Testing {c2['full_name']} @ {c2['firm']} (Expect Departure)...")
    res2 = checker.check_status(c2)
    print(f"  Departure? {res2['is_departure']} ({res2['risk_reason']})\n")

if __name__ == "__main__":
    test_liveness()
