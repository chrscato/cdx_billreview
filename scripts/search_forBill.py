import os
import sys
import json
from typing import List  # ✅ <-- This line is critical

# Add project root to sys.path so utils/ can be imported
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from utils.s3_utils import list_objects, get_s3_json


SEARCH_PREFIX = "data/hcfa_json/"
SEARCH_TERM = "goodwin"  # Replace with the string/keyword you're searching for

def search_jsons_in_s3(search_term: str, prefix: str = SEARCH_PREFIX) -> List[str]:
    matches = []
    all_keys = list_objects(prefix)
    
    for key in all_keys:
        if not key.endswith('.json'):
            continue
        try:
            data = get_s3_json(key)
            data_str = json.dumps(data)
            if search_term.lower() in data_str.lower():
                print(f"\n✅ Match found in: {key}")
                print(json.dumps(data, indent=2))  # Or truncate if you only want summary
                matches.append(key)
        except Exception as e:
            print(f"❌ Error reading {key}: {e}")
    
    print(f"\n🔍 Search complete. {len(matches)} matches found.")
    return matches

if __name__ == "__main__":
    search_term = input("🔍 Enter the text to search for in hcfa_json files: ").strip()
    if not search_term:
        print("⚠️  No input provided. Exiting.")
    else:
        search_jsons_in_s3(search_term)

