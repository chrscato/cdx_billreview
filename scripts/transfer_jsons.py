import os
import json
import shutil
from datetime import datetime

# 🔁 Update this to your JSON directory
FOLDER_PATH = r"C:\Users\ChristopherCato\Downloads\tempfors3"
MAPPED_FOLDER = os.path.join(FOLDER_PATH, "mapped")
UNMAPPED_FOLDER = os.path.join(FOLDER_PATH, "unmapped")
TODAY_STRING = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Keys to retain in the final output
ALLOWED_KEYS = {"patient_info", "billing_info", "service_lines", "mapping_info"}

# === Setup output folders ===
os.makedirs(MAPPED_FOLDER, exist_ok=True)
os.makedirs(UNMAPPED_FOLDER, exist_ok=True)

for filename in os.listdir(FOLDER_PATH):
    if not filename.endswith(".json"):
        continue

    file_path = os.path.join(FOLDER_PATH, filename)

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            original_data = json.load(f)
    except Exception as e:
        print(f"⚠️ Error reading {filename}: {e}")
        continue

    order_id = original_data.get("Order_ID") or original_data.get("order_id")

    data = {
        key: value for key, value in original_data.items()
        if key in {"patient_info", "billing_info", "service_lines"}
    }

    if order_id:
        data["mapping_info"] = {
            "order_id": order_id,
            "mapping_date": TODAY_STRING
        }
        output_path = os.path.join(MAPPED_FOLDER, filename)
    else:
        output_path = os.path.join(UNMAPPED_FOLDER, filename)

    # Save cleaned result
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    os.remove(file_path)

print("✅ Done! Files cleaned and sorted into 'mapped' and 'unmapped'.")
