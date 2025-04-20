# process/utils/filter_ancillaries.py

import os
import json
from typing import List, Tuple
from dotenv import load_dotenv
from process.utils.models import Procedure

load_dotenv()
DATA_DIR = os.getenv("DATA_DIR", "process/data/")
ANCILLARY_JSON_PATH = os.path.join(DATA_DIR, "ancillary_codes.json")


def load_ancillary_cpts() -> List[str]:
    with open(ANCILLARY_JSON_PATH, 'r') as f:
        data = json.load(f)
    return list(data.get("ancillary_codes", {}).keys())


def filter_ancillaries(procedures: List[Procedure]) -> Tuple[List[Procedure], List[str]]:
    """
    Filters out ancillary CPTs from the provided list of Procedure objects.

    Returns:
        (filtered_procedures, skipped_cpt_codes)
    """
    ancillary_cpts = set(load_ancillary_cpts())
    filtered = []
    skipped = []

    for proc in procedures:
        if proc.cpt_code in ancillary_cpts:
            skipped.append(proc.cpt_code)
        else:
            filtered.append(proc)

    return filtered, skipped
