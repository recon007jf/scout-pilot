import os
import re
import sys

PROJECT_ROOT = os.getcwd()

# Regex to catch likely file strings: ends with extension, inside quotes
# We focus on data files: csv, json, parquet, txt
PATH_PATTERN = re.compile(r'[\'"]([^\'"]+\.(?:csv|json|parquet|txt))[\'"]')

IGNORE_DIRS = {
    '.git', '.venv', 'venv', '__pycache__', 'node_modules', '.idea', '.vscode',
    'artifacts', 'suppression', 'archive', 'backups', 'logs', '.cache', 'outputs', 'dist', 'build'
}

def audit_paths():
    print(f"🔎 STARTING PATH AUDIT from {PROJECT_ROOT}...\n")
    
    broken_refs = []
    checked_count = 0
    
    for root, dirs, files in os.walk(PROJECT_ROOT):
        dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]
        
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                rel_script_path = os.path.relpath(file_path, PROJECT_ROOT)
                
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                        
                    matches = PATH_PATTERN.findall(content)
                    
                    for ref in matches:
                        # We have a reference 'ref'. Does it exist?
                        # 1. Check Absolute
                        if os.path.isabs(ref):
                            if os.path.exists(ref): continue
                        
                        # 2. Check Relative to PROJECT_ROOT (Common CWD assumption)
                        if os.path.exists(os.path.join(PROJECT_ROOT, ref)): continue
                        
                        # 3. Check Relative to Script Directory
                        if os.path.exists(os.path.join(root, ref)): continue
                        
                        # 4. Check if it's just a filename in 'backend/data/input' (Migration logic fallback?)
                        # No, we want to find BROKEN ones.
                        
                        # If we get here, it's missing.
                        broken_refs.append({
                            'script': rel_script_path,
                            'ref': ref
                        })
                        
                except Exception as e:
                    print(f"Error checking {rel_script_path}: {e}")

    print(f"--- BROKEN PATH REPORT ---")
    if not broken_refs:
        print("✅ No missing hardcoded file references found!")
    else:
        print(f"❌ Found {len(broken_refs)} potentially broken references:\n")
        # Group by script
        grouped = {}
        for item in broken_refs:
            k = item['script']
            if k not in grouped: grouped[k] = []
            grouped[k].append(item['ref'])
            
        for script, refs in grouped.items():
            print(f"📄 {script}:")
            for r in refs:
                print(f"   ✖️  '{r}' NOT FOUND")
            print("")

if __name__ == "__main__":
    audit_paths()
