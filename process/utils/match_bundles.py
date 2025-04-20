# process/utils/match_bundles.py

import os
import json
from typing import List, Tuple
from dotenv import load_dotenv
from process.utils.models import Procedure, MatchResult

load_dotenv()
DATA_DIR = os.getenv("DATA_DIR", "process/data/")
BUNDLES_PATH = os.path.join(DATA_DIR, "procedure_bundles.json")


def load_bundles() -> dict:
    with open(BUNDLES_PATH, 'r') as f:
        return json.load(f)


def match_bundles(billed: List[Procedure]) -> Tuple[List[MatchResult], List[Procedure]]:
    bundles = load_bundles()
    matches = []
    remaining = billed.copy()

    for bundle_name, bundle_def in bundles.items():
        core_cpts = set(bundle_def.get("core_codes", []))
        optional_cpts = set(bundle_def.get("optional_codes", []))

        billed_cpt_map = {p.cpt_code: p for p in remaining}
        billed_cpt_set = set(billed_cpt_map.keys())

        # Check if all core CPTs exist
        if core_cpts.issubset(billed_cpt_set):
            # Build match result
            for cpt in core_cpts.union(optional_cpts):
                if cpt in billed_cpt_map:
                    matches.append(MatchResult(
                        billed=billed_cpt_map[cpt],
                        ordered=None,
                        match_type="BUNDLED",
                        details=f"Part of bundle: {bundle_name}"
                    ))

            # Remove matched CPTs from remaining
            remaining = [p for p in remaining if p.cpt_code not in core_cpts.union(optional_cpts)]
            break  # Only apply first match

    return matches, remaining
