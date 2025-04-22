import os
import json
import boto3
import datetime
from dateutil.parser import parse as parse_date
from botocore.exceptions import ClientError

BUCKET = "bill-review-prod"  # Replace this with your actual S3 bucket
PREFIX = "data/hcfa_json/valid/mapped/staging/fails/"
OUTPUT_FILE = "data/dashboard/failed_summary.json"

s3 = boto3.client("s3")

def extract_failure_types(data):
    try:
        reasons = data.get("validation_info", {}).get("failure_reasons", [])
        return [r.split(":")[0].strip() for r in reasons if isinstance(r, str)]
    except Exception:
        return ["UNKNOWN"]

def extract_provider(data):
    try:
        return data.get("filemaker", {}).get("provider", {}).get("DBA Name Billing Name", "UNKNOWN_PROVIDER")
    except Exception:
        return "UNKNOWN_PROVIDER"

def extract_dos(data):
    try:
        return data.get("filemaker", {}).get("line_items", [{}])[0].get("DOS") \
            or data.get("service_lines", [{}])[0].get("date_of_service", "UNKNOWN_DATE")
    except Exception:
        return "UNKNOWN_DATE"

def calculate_age_days(dos_str):
    try:
        dos_date = parse_date(dos_str).date()
        return (datetime.date.today() - dos_date).days
    except Exception:
        return None

def main():
    summary = []
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    for obj in response.get("Contents", []):
        key = obj["Key"]
        if not key.lower().endswith(".json"):
            continue

        try:
            resp = s3.get_object(Bucket=BUCKET, Key=key)
            data = json.loads(resp["Body"].read().decode("utf-8"))
        except (ClientError, json.JSONDecodeError) as e:
            summary.append({
                "filename": os.path.basename(key),
                "failure_types": ["READ_ERROR"],
                "provider": "N/A",
                "dos": "N/A",
                "age_days": None
            })
            continue

        filename = data.get("filename", os.path.basename(key))
        failure_types = extract_failure_types(data)
        provider = extract_provider(data)
        dos = extract_dos(data)
        age_days = calculate_age_days(dos)

        summary.append({
            "filename": filename,
            "failure_types": failure_types,
            "provider": provider,
            "dos": dos,
            "age_days": age_days
        })

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as out:
        json.dump(summary, out, indent=2)

    print(f"✅ Wrote {len(summary)} records to {OUTPUT_FILE}")

if __name__ == "__main__":
    main() 