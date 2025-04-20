# process/utils/validator_logger.py

import os
import json
from typing import List
from dotenv import load_dotenv
from process.utils.models import MatchResult
from process.utils import s3_utils

load_dotenv()

# Paths from .env
INPUT_PREFIX = os.getenv("VALIDATE_INPUT_PREFIX", "").rstrip('/')
SUCCESS_PREFIX = f"{INPUT_PREFIX}/success"
FAILS_PREFIX = f"{INPUT_PREFIX}/fails"


def log_validation_results(
    file_name: str,
    claim_json: dict,
    match_results: List[MatchResult],
    unmatched_cpts: List[str],
    skipped_cpts: List[str],
    unit_issues: List[str],
    rate_failure: bool,
    missing_rates: List[str]
):
    failure_reasons = []

    # 1. Unmatched CPTs
    for cpt in unmatched_cpts:
        failure_reasons.append(f"UNMATCHED_CPT: {cpt}")

    # 2. Units validation issues
    failure_reasons.extend(unit_issues)

    # 3. Rate validation failures
    for cpt in missing_rates:
        failure_reasons.append(f"RATE_MISSING: {cpt}")

    # 4. Format match results
    matched_lines = [{
        "billed_cpt": m.billed.cpt_code,
        "ordered_cpt": m.ordered.cpt_code if m.ordered else None,
        "match_type": m.match_type,
        "details": m.details
    } for m in match_results]

    # 5. Determine final status and destination
    if failure_reasons or rate_failure:
        status = "FAIL"
        dest_key = f"{FAILS_PREFIX}/{file_name}"
    elif any(m.match_type == "CLINICAL_EQUIVALENT" for m in match_results):
        status = "SOFT_PASS"
        dest_key = f"{SUCCESS_PREFIX}/{file_name}"
    else:
        status = "PASS"
        dest_key = f"{SUCCESS_PREFIX}/{file_name}"

    # 6. Update claim JSON with validation info
    claim_json["validation_info"] = {
        "status": status,
        "failure_reasons": failure_reasons,
        "matched_lines": matched_lines,
        "unmatched_cpts": unmatched_cpts,
        "skipped_cpts": skipped_cpts
    }

    # 7. Write updated JSON to appropriate S3 folder
    s3_utils.upload_json_to_s3(claim_json, dest_key)
    s3_utils.delete(f"{INPUT_PREFIX}/{file_name}")

    print(f"✅ {file_name} → {status} → {dest_key}")
