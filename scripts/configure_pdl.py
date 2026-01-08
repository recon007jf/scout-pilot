
import os

key = "acd062bb1ba28806082e60ccec138967adcfa33320b88d778fdb9136faf22d6f"
env_path = ".env"

def configure_key():
    # Read existing
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            lines = f.readlines()
    else:
        lines = []

    # Remove existing PDL_API_KEY if any
    lines = [l for l in lines if not l.startswith("PDL_API_KEY")]
    
    # Append new
    if lines and not lines[-1].endswith("\n"):
        lines[-1] += "\n"
    lines.append(f"PDL_API_KEY={key}\n")
    
    with open(env_path, "w") as f:
        f.writelines(lines)
    
    print("✅ PDL_API_KEY configured in .env")

if __name__ == "__main__":
    configure_key()
