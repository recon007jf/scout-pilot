import os
import shutil
import time
import re
import sys

# CONFIG
PROJECT_ROOT = os.getcwd()
INPUT_DIR = os.path.join(PROJECT_ROOT, "backend", "data", "input")
CACHE_DIR = os.path.join(PROJECT_ROOT, ".cache")

def backup_file(filepath):
    timestamp = int(time.time())
    backup_path = f"{filepath}.pre_pathfix_{timestamp}"
    shutil.copy2(filepath, backup_path)
    return backup_path

def detect_indentation(content, match_end_index):
    """Detects indentation of the line following the match."""
    # Look for the next non-empty line
    next_lines = content[match_end_index:].splitlines()
    for line in next_lines:
        if line.strip(): # Found code
            # Measure leading whitespace
            return line[:len(line) - len(line.lstrip())]
    return "    " # Default to 4 spaces if block is empty/weird

def safe_inject_lock(content):
    """Injects lock inside if __name__ == '__main__': block with correct indentation."""
    pattern = r"if\s+__name__\s*==\s*['\"]__main__['\"]\s*:"
    match = re.search(pattern, content)
    
    if match:
        indent = detect_indentation(content, match.end())
        lock_code = (
            f"\n{indent}import os, sys\n"
            f"{indent}if not os.getenv('ALLOW_LEGACY_RUNS'):\n"
            f"{indent}    print('SAFETY LOCK: Set ALLOW_LEGACY_RUNS=1 to execute this legacy script.')\n"
            f"{indent}    sys.exit(0)\n"
        )
        insertion_point = match.end()
        return content[:insertion_point] + lock_code + content[insertion_point:]
    return None

def audit_script_usage(filename, content):
    """Heuristic check for CSV usage."""
    print(f"\n--- AUDIT REPORT: {filename} ---")
    suspect_file = "Scout_Broker_Batch_50.csv"
    
    if suspect_file in content:
        print(f"  [ALERT] Found reference to '{suspect_file}'")
        # Find the line
        lines = content.splitlines()
        for i, line in enumerate(lines):
            if suspect_file in line:
                print(f"  Line {i+1}: {line.strip()}")
                
                # Simple Heuristics
                if "read_csv" in line:
                    print("    -> USAGE: Likely INPUT (read_csv)")
                elif "to_csv" in line:
                    print("    -> USAGE: Likely OUTPUT (to_csv)")
                elif "exclude" in line or "suppression" in line:
                    print("    -> USAGE: Likely EXCLUSION LIST")
                elif "open(" in line and "'w'" in line:
                     print("    -> USAGE: Likely OUTPUT (file open 'w')")
                else:
                    print("    -> USAGE: Unknown context")
    else:
        print(f"  [CLEAN] No reference to '{suspect_file}' found.")

def apply_patch(filename, replacements, inject_lock=True):
    path = os.path.join(PROJECT_ROOT, filename)
    if not os.path.exists(path):
        print(f"SKIPPING: {filename} (Not found)")
        return

    print(f"\n--- Processing {filename} ---")
    with open(path, 'r') as f:
        content = f.read()

    new_content = content
    changes = []
    abort_write = False

    # 1. Apply Path Replacements
    for old_str, target_file, target_dir in replacements:
        if old_str in new_content:
            # A. Count Occurrences
            count = new_content.count(old_str)
            if count > 1:
                print(f"  [CRITICAL] String '{old_str}' found {count} times.")
                print("  [ACTION] ABORTING WRITE. Manual review required to avoid collateral damage.")
                abort_write = True
                break

            # B. Verify Target
            if target_dir == INPUT_DIR and not os.path.exists(os.path.join(INPUT_DIR, target_file)):
                print(f"  [WARN] Target missing: {target_file}. Skipping replacement.")
                continue
            
            if target_dir == CACHE_DIR and not os.path.exists(CACHE_DIR):
                os.makedirs(CACHE_DIR)

            # C. Replace
            new_path = os.path.join(target_dir, target_file)
            rel_path = os.path.relpath(new_path, PROJECT_ROOT)
            
            # Use raw string replacement (safe because count == 1)
            new_content = new_content.replace(old_str, rel_path)
            changes.append(f"Fixed path: '{old_str}' -> '{rel_path}'")
    
    if abort_write:
        return

    # 2. Inject Lock
    if inject_lock and "ALLOW_LEGACY_RUNS" not in new_content:
        locked_content = safe_inject_lock(new_content)
        if locked_content:
            new_content = locked_content
            changes.append("Injected safety lock into main block")
        else:
            print("  [WARN] Could not find main block. Safety lock skipped.")

    # 3. Write Changes
    if changes and new_content != content:
        bkp = backup_file(path)
        print(f"  Backup created: {os.path.basename(bkp)}")
        with open(path, 'w') as f:
            f.write(new_content)
        
        print("  CHANGES APPLIED:")
        for c in changes:
            print(f"  - {c}")
    else:
        print("  No changes made (or content identical).")

def execute_repairs():
    # 1. Sniper: Fixes + Lock
    sniper_replacements = [
        ("Scout_Data_Artifacts/Leads_With_Human_Contacts.csv", "Leads_With_Human_Contacts.csv", INPUT_DIR),
        ("serper_results.json", "serper_results.json", CACHE_DIR)
    ]
    apply_patch("backend/scripts/execute_hybrid_sniper.py", sniper_replacements, inject_lock=True)

    # 2. Broker Hunter: Audit + Lock Only
    bh_path = os.path.join(PROJECT_ROOT, "backend/scripts/execute_broker_hunter.py")
    if os.path.exists(bh_path):
        with open(bh_path, 'r') as f:
            audit_script_usage("backend/scripts/execute_broker_hunter.py", f.read())
        
        apply_patch("backend/scripts/execute_broker_hunter.py", [], inject_lock=True)
    else:
        print(f"ERROR: backend/scripts/execute_broker_hunter.py not found.")

if __name__ == "__main__":
    execute_repairs()
