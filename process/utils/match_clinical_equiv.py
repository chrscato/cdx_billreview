# process/utils/match_clinical_equiv.py

import sqlite3
from typing import List, Tuple
from process.utils.models import Procedure, MatchResult
import os
from dotenv import load_dotenv

load_dotenv()
PROC_DB_PATH = os.getenv("PROC_DB_PATH", "filemaker.db")


def get_proc_meta(cpt_code: str) -> Tuple[str, str]:
    """
    Fetch category and subcategory from dim_proc for a given CPT code.
    """
    conn = sqlite3.connect(PROC_DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT category, subcategory FROM dim_proc
        WHERE proc_cd = ?
    """, (cpt_code,))
    row = cursor.fetchone()
    conn.close()

    return row if row else (None, None)


def match_clinical_equiv(billed: List[Procedure], ordered: List[Procedure]) -> Tuple[List[MatchResult], List[Procedure]]:
    """
    Match CPTs with same category + subcategory from dim_proc.
    """
    matches = []
    used_ordered = set()

    for b_proc in billed:
        b_cat, b_sub = get_proc_meta(b_proc.cpt_code)
        if not b_cat or not b_sub:
            continue

        for idx, o_proc in enumerate(ordered):
            if idx in used_ordered:
                continue

            o_cat, o_sub = get_proc_meta(o_proc.cpt_code)
            if (b_cat == o_cat) and (b_sub == o_sub):
                matches.append(MatchResult(
                    billed=b_proc,
                    ordered=o_proc,
                    match_type="CLINICAL_EQUIVALENT",
                    details=f"Category: {b_cat}, Subcategory: {b_sub}"
                ))
                used_ordered.add(idx)
                break

    remaining_billed = [b for b in billed if b not in [m.billed for m in matches]]
    return matches, remaining_billed
