import json
import os

# Paths
CRED_PATH = "credentials.json"
ENV_PATH = "../scout-production/.env.local"

def fix_env():
    # 1. Read Credentials
    if not os.path.exists(CRED_PATH):
        print("❌ credentials.json not found")
        return

    with open(CRED_PATH, "r") as f:
        creds = json.load(f)
    
    minified_creds = json.dumps(creds)
    
    # 2. Read .env.local
    if not os.path.exists(ENV_PATH):
        print("❌ .env.local not found")
        return

    with open(ENV_PATH, "r") as f:
        env_content = f.read()
    
    if "GOOGLE_SERVICE_ACCOUNT_KEY" in env_content:
        print("⚠️ GOOGLE_SERVICE_ACCOUNT_KEY already exists in .env.local. Skipping append.")
        return

    # 3. Append
    new_content = env_content + f"\n\n# Added by Antigravity (Debug Fix)\nGOOGLE_SERVICE_ACCOUNT_KEY='{minified_creds}'\n"
    
    with open(ENV_PATH, "w") as f:
        f.write(new_content)
    
    print("✅ Successfully injected GOOGLE_SERVICE_ACCOUNT_KEY into .env.local")

if __name__ == "__main__":
    fix_env()
