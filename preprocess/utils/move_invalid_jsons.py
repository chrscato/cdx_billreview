import os
from dotenv import load_dotenv
from pathlib import Path

# Project setup
project_root = Path(__file__).resolve().parents[2]
os.sys.path.append(str(project_root))

# Load S3 helpers
from utils.s3_utils import move

# Load env variables
load_dotenv()
S3_BUCKET = os.getenv("S3_BUCKET")

# Your list of bad files (from your summary report)
INVALID_KEYS = [
    "data/hcfa_json/valid/mapped/20250416_133158020.json",
    "data/hcfa_json/valid/mapped/staging/20250112_1049.json",
    "data/hcfa_json/valid/mapped/staging/20250224_1096.json",
    "data/hcfa_json/valid/mapped/staging/20250319_114129060.json"
]

# Optional: remove duplicates
INVALID_KEYS = list(set(INVALID_KEYS))

def move_invalid_files():
    print(f"⚙️ Moving {len(INVALID_KEYS)} invalid files to data/hcfa_json/invalid/")

    for key in INVALID_KEYS:
        dest_key = f"data/hcfa_json/invalid/{os.path.basename(key)}"
        try:
            move(key, dest_key)
            print(f"✅ Moved {key} → {dest_key}")
        except Exception as e:
            print(f"❌ Failed to move {key}: {e}")

    print("✅ All moves attempted.")

if __name__ == "__main__":
    move_invalid_files()
