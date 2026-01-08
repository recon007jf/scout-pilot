import os
import shutil
import hashlib
import csv
import re
import time
from datetime import datetime
from collections import defaultdict

# --- CONFIGURATION ---
PROJECT_ROOT = os.getcwd()
# Canonical path per directive
TARGET_DIR = os.path.join(PROJECT_ROOT, "backend", "data", "input")
MANIFEST_FILE = os.path.join(TARGET_DIR, "csv_manifest.csv")

# EXPANDED IGNORE LIST (Prevents ingesting outputs/artifacts)
IGNORE_DIRS = {
    '.git', '.venv', 'venv', '__pycache__', 'node_modules', '.idea', '.vscode',
    'artifacts', 'suppression', 'archive', 'backups', 'logs', '.cache', 'outputs', 'dist', 'build'
}

def validate_root():
    """Ensure we are running from the correct project root."""
    if not os.path.exists(os.path.join(PROJECT_ROOT, "backend")):
        print(f"CRITICAL ERROR: 'backend/' directory not found in {PROJECT_ROOT}.")
        print("You must run this script from the project root.")
        exit(1)

def get_file_hash(filepath):
    """Calculate SHA256 hash of a file safely."""
    hasher = hashlib.sha256()
    try:
        with open(filepath, 'rb') as f:
            while chunk := f.read(8192):
                hasher.update(chunk)
        return hasher.hexdigest()
    except Exception as e:
        return None

def scan_legacy_references():
    """Scans .py files for hardcoded CSV references and groups them."""
    print("\n--- DEPENDENCY CHECK: Migration Checklist ---")
    csv_pattern = re.compile(r'[\'"]([^\'"]+\.csv)[\'"]')
    script_counts = defaultdict(int)
    references = []

    for root, dirs, files in os.walk(PROJECT_ROOT):
        dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]
        for file in files:
            if file.endswith(".py"):
                path = os.path.join(root, file)
                try:
                    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                        matches = csv_pattern.findall(content)
                        if matches:
                            rel_path = os.path.relpath(path, PROJECT_ROOT)
                            script_counts[rel_path] += len(matches)
                            for m in matches:
                                references.append(f"  - {rel_path}: '{m}'")
                except Exception:
                    pass
    
    # Print Summary Table
    if script_counts:
        print(f"{'Script Name':<60} | {'CSV Refs':<10}")
        print("-" * 75)
        for script, count in sorted(script_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"{script:<60} | {count:<10}")
        print("-" * 75)
        print(f"Total potential legacy references: {len(references)}\n")
    else:
        print("  No hardcoded .csv references found.")

def consolidate_csvs_safe():
    validate_root()
    start_time = time.time()
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print(f"SCRIPT STARTED at {datetime.now().isoformat()}")
    print(f"PROJECT_ROOT: {PROJECT_ROOT}")
    print(f"TARGET_DIR:   {TARGET_DIR}")
    print(f"IGNORING:     {sorted(list(IGNORE_DIRS))}")
    print("\nWARNING: Ensure no RAW INPUTS are stored in the ignored folders above.")
    print("If they are, stop now and move them manually.\n")
    
    # 1. Prepare Target Directory
    if not os.path.exists(TARGET_DIR):
        os.makedirs(TARGET_DIR)
        print(f"Created canonical target: {TARGET_DIR}")
    
    # 2. Run Dependency Scan
    scan_legacy_references()

    # 3. Pre-load existing hashes
    seen_hashes = {} # Map hash -> filename for better audit
    print("Pre-scanning target directory for existing content...")
    if os.path.exists(TARGET_DIR):
        for f in os.listdir(TARGET_DIR):
            if f.lower().endswith('.csv') and f != 'csv_manifest.csv':
                path = os.path.join(TARGET_DIR, f)
                h = get_file_hash(path)
                if h:
                    seen_hashes[h] = f
    
    print(f"Index loaded. {len(seen_hashes)} unique CSVs already exist in target.")

    actions = []
    stats = {
        "found": 0, "copied": 0, "copied_renamed": 0,
        "skipped_content": 0, "skipped_exact": 0, "errors": 0
    }

    print(f"Starting Scan (Run ID: {run_id})...")
    
    # 4. Walk and Process
    for root, dirs, files in os.walk(PROJECT_ROOT):
        # PRUNE: Remove ignore dirs AND target dir from traversal
        dirs[:] = [
            d for d in dirs 
            if d not in IGNORE_DIRS 
            and os.path.abspath(os.path.join(root, d)) != os.path.abspath(TARGET_DIR)
        ]
        
        # Visibility: Print top-level scan
        if root == PROJECT_ROOT:
            print(f"Scanning top-level folders: {dirs}")

        for file in files:
            if file.lower().endswith(".csv") and file != "csv_manifest.csv":
                stats["found"] += 1
                source_path = os.path.join(root, file)
                
                source_hash = get_file_hash(source_path)
                
                # ERROR HANDLING
                if not source_hash:
                    stats["errors"] += 1
                    actions.append({
                        "run_id": run_id,
                        "timestamp": datetime.now().isoformat(),
                        "source_path": source_path,
                        "target_path": "N/A",
                        "file_hash": "ERROR",
                        "action_taken": "ERROR_READ_FAILED"
                    })
                    print(f"[ERROR] Could not read/hash: {file}")
                    continue

                target_filename = file
                target_path = os.path.join(TARGET_DIR, target_filename)
                action_taken = ""
                final_target_path = ""

                # LOGIC BRANCHES
                if source_hash in seen_hashes:
                    # Point to the file that actually has this content
                    existing_file = seen_hashes[source_hash]
                    final_target_path = os.path.join(TARGET_DIR, existing_file)
                    
                    if os.path.exists(target_path):
                        action_taken = "SKIPPED_DUPLICATE_EXACT"
                        stats["skipped_exact"] += 1
                    else:
                        action_taken = "SKIPPED_DUPLICATE_CONTENT"
                        stats["skipped_content"] += 1
                else:
                    if os.path.exists(target_path):
                        name, ext = os.path.splitext(file)
                        new_name = f"{name}_conflict_{run_id}{ext}"
                        final_target_path = os.path.join(TARGET_DIR, new_name)
                        shutil.copy2(source_path, final_target_path)
                        action_taken = "COPIED_RENAMED"
                        stats["copied_renamed"] += 1
                        seen_hashes[source_hash] = new_name
                    else:
                        final_target_path = target_path
                        shutil.copy2(source_path, final_target_path)
                        action_taken = "COPIED"
                        stats["copied"] += 1
                        seen_hashes[source_hash] = target_filename

                # Log
                actions.append({
                    "run_id": run_id,
                    "timestamp": datetime.now().isoformat(),
                    "source_path": source_path,
                    "target_path": final_target_path,
                    "file_hash": source_hash,
                    "action_taken": action_taken
                })
                
                if "COPIED" in action_taken:
                    print(f"[{action_taken}] {file}")

    # 5. Write Manifest
    file_exists = os.path.isfile(MANIFEST_FILE)
    fieldnames = ["run_id", "timestamp", "source_path", "target_path", "file_hash", "action_taken"]
    
    try:
        with open(MANIFEST_FILE, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(actions)
        print(f"Manifest updated at: {MANIFEST_FILE}")
    except Exception as e:
        print(f"CRITICAL ERROR WRITING MANIFEST: {e}")

    duration = time.time() - start_time
    # 6. Final Report
    print("-" * 30)
    print(f"CONSOLIDATION COMPLETE (Duration: {duration:.2f}s)")
    print(f"Total CSVs Found:       {stats['found']}")
    print(f"Copied (New):           {stats['copied']}")
    print(f"Copied (Renamed):       {stats['copied_renamed']}")
    print(f"Skipped (Dup Content):  {stats['skipped_content'] + stats['skipped_exact']}")
    print(f"Errors:                 {stats['errors']}")
    print("-" * 30)

if __name__ == "__main__":
    consolidate_csvs_safe()
