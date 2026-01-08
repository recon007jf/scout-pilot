
import os

def check_and_fix():
    env_path = ".env"
    
    # Check if exists
    if not os.path.exists(env_path):
        print("❌ .env file not found!")
        return

    # Read current content
    with open(env_path, "r") as f:
        content = f.read()
    
    if "GOOGLE_MAPS_SERVER_KEY" in content:
        print("✅ GOOGLE_MAPS_SERVER_KEY is present in .env.")
        # Extract verify (simple parse)
        for line in content.splitlines():
            if line.startswith("GOOGLE_MAPS_SERVER_KEY"):
                print(f"   Value: {line.split('=')[1][:5]}...")
    else:
        print("⚠️  GOOGLE_MAPS_SERVER_KEY missing.")
        key = input("Paste your Server API Key here: ").strip()
        if key:
            with open(env_path, "a") as f:
                f.write(f"\nGOOGLE_MAPS_SERVER_KEY={key}\n")
            print("✅ Key appended to .env")

if __name__ == "__main__":
    check_and_fix()
