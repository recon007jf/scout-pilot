import csv
import sys
import os

# Target File
CSV_PATH = "/Users/josephlf/.gemini/antigravity/scratch/artifacts/Master_Hunting_List_Production_v3_SYSTEM_ENRICHED_FUNDING_PATCHED.csv"

def analyze_csv():
    if not os.path.exists(CSV_PATH):
        print(f"❌ Error: File not found: {CSV_PATH}")
        return

    print(f"# CSV Schema: {os.path.basename(CSV_PATH)}")
    print("\n---\n")

    with open(CSV_PATH, 'r', encoding='utf-8-sig') as f: # BOM aware
        reader = csv.reader(f)
        try:
            headers = next(reader)
        except StopIteration:
            print("❌ Error: Empty file")
            return
        
        # Read all rows to analyze
        rows = list(reader)
        total_rows = len(rows)
        
        # Analysis Storage
        columns = []
        
        # Analyze each column
        for idx, col_name in enumerate(headers):
            values = [row[idx] for row in rows if len(row) > idx]
            non_empty_values = [v for v in values if v.strip() != ""]
            
            # Type Inference
            inferred_type = "empty"
            if non_empty_values:
                # Check for number
                is_number = True
                is_int = True
                for v in non_empty_values:
                    # Remove common currency symbols for checking
                    clean = v.replace('$', '').replace(',', '').replace('%', '').strip()
                    try:
                        float(clean)
                        if '.' in clean:
                            is_int = False
                    except ValueError:
                        is_number = False
                        is_int = False
                        break
                
                if is_number:
                    inferred_type = "number (integer)" if is_int else "number (float)"
                else:
                    # Check for boolean
                    lower_values = set(v.lower() for v in non_empty_values)
                    if lower_values.issubset({'true', 'false', '0', '1', 'yes', 'no'}):
                        inferred_type = "boolean"
                    else:
                        inferred_type = "string"
            
            # Nullability
            empty_count = total_rows - len(non_empty_values)
            nullability = "Always present"
            if empty_count > 0:
                percent_empty = (empty_count / total_rows) * 100
                if percent_empty == 100:
                    nullability = "Always blank"
                elif percent_empty > 50:
                    nullability = "Frequently blank"
                else:
                    nullability = "Sometimes blank"
            
            # Example
            example = non_empty_values[0] if non_empty_values else "N/A"
            if len(example) > 50:
                example = example[:47] + "..."
                
            # Notes
            notes = []
            if inferred_type == "string" and non_empty_values:
                # Check if comma separated
                if any(',' in v for v in non_empty_values):
                     # Heuristic, might be normal text
                     pass
            
            col_data = {
                "name": col_name,
                "type": inferred_type,
                "example": example,
                "nullability": nullability,
                "notes": ", ".join(notes)
            }
            columns.append(col_data)

        # Output Markdown
        for col in columns:
            print(f"### {col['name']}")
            print(f"- **Data type**: {col['type']}")
            print(f"- **Example value**: `{col['example']}`")
            print(f"- **Nullability**: {col['nullability']}")
            if col['notes']:
                print(f"- **Notes**: {col['notes']}")
            print("")
            
        # Summary
        print("## Summary")
        print(f"- **Total Columns**: {len(headers)}")
        print(f"- **Total Rows**: {total_rows}")
        
        # Duplicates check
        seen = set()
        dupes = [x for x in headers if x in seen or seen.add(x)]
        if dupes:
            print(f"- **Duplicate Columns**: {dupes}")
        else:
            print("- **Duplicate Columns**: None")
            
        # Empty check
        empty_cols = [c['name'] for c in columns if c['nullability'] == "Always blank"]
        if empty_cols:
            print(f"- **Empty Columns**: {empty_cols}")
        else:
            print("- **Empty Columns**: None")

if __name__ == "__main__":
    analyze_csv()
