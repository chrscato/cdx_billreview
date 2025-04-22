import os
import sys
import json
import tempfile
from dotenv import load_dotenv
from pathlib import Path

# ✅ Manually add project root to sys.path
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

# ✅ Correct import paths
from preprocess.utils.validatejson import validate_json
from utils.s3_utils import list_objects, download

# Load environment
load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
CHECK_PREFIXES = [
    "data/hcfa_json/valid/",
    "data/hcfa_json/valid/unmapped/",
    "data/hcfa_json/valid/mapped/",
    "data/hcfa_json/valid/mapped/staging/",
]

def check_valid_jsons():
    bad_files = []

    for prefix in CHECK_PREFIXES:
        print(f"\n🔍 Checking files in: s3://{S3_BUCKET}/{prefix}")

        json_keys = [k for k in list_objects(prefix) if k.endswith(".json")]
        print(f"Found {len(json_keys)} JSON file(s).")

        for key in json_keys:
            try:
                local_path = download(key, os.path.join(tempfile.gettempdir(), os.path.basename(key)))

                with open(local_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                is_valid, message = validate_json(data)

                if not is_valid:
                    bad_files.append((key, message))
                    print(f"❌ Invalid JSON: {key}")
                    print(f"   Reason: {message}")

                os.remove(local_path)

            except Exception as e:
                print(f"⚠️ Error checking {key}: {e}")
    
    print("\n✅ Summary Report")
    print(f"Total invalid files found: {len(bad_files)}")
    for key, msg in bad_files:
        print(f" - {key}: {msg}")

if __name__ == "__main__":
    check_valid_jsons()
