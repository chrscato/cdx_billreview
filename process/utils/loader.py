# process/utils/loader.py

import os
from typing import Tuple, List, Dict, Any
from utils import s3_utils
from dotenv import load_dotenv

load_dotenv()

INPUT_PREFIX = os.getenv("VALIDATE_INPUT_PREFIX")  # Should be: data/hcfa_json/valid/mapped/staging/


def load_claim_from_s3(file_name: str) -> Dict[str, Any]:
    """
    Loads a structured claim JSON file from S3 using the key: VALIDATE_INPUT_PREFIX + file_name
    """
    s3_key = os.path.join(INPUT_PREFIX, file_name)
    return s3_utils.get_s3_json(s3_key)


def extract_procedure_data(data: Dict[str, Any]) -> Tuple[List[Dict], List[Dict], Dict[str, Any]]:
    """
    Extracts billed and ordered procedures and metadata from loaded JSON.
    """
    billed = data.get("service_lines", [])
    ordered = data.get("filemaker", {}).get("line_items", [])

    metadata = {
        "order_id": data.get("mapping_info", {}).get("order_id"),
        "filemaker_number": data.get("mapping_info", {}).get("filemaker_number"),
        "patient_name": data.get("patient_info", {}).get("patient_name"),
        "date_of_service": billed[0].get("date_of_service") if billed else None,
    }

    return billed, ordered, metadata
