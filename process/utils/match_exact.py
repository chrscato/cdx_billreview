# process/utils/match_exact.py

from typing import List, Tuple
from process.utils.models import Procedure, MatchResult


def match_exact(billed: List[Procedure], ordered: List[Procedure]) -> Tuple[List[MatchResult], List[Procedure], List[Procedure]]:
    """
    Matches CPTs exactly between billed and ordered lists.
    One-to-one, greedy matching — no duplicates.

    Returns:
        (matches, remaining_billed, remaining_ordered)
    """
    matches = []
    used_ordered = set()

    for bill_proc in billed:
        for idx, ord_proc in enumerate(ordered):
            if idx in used_ordered:
                continue
            if bill_proc.cpt_code == ord_proc.cpt_code:
                matches.append(MatchResult(
                    billed=bill_proc,
                    ordered=ord_proc,
                    match_type="EXACT"
                ))
                used_ordered.add(idx)
                break

    # Get leftovers
    remaining_billed = [b for b in billed if b not in [m.billed for m in matches]]
    remaining_ordered = [o for i, o in enumerate(ordered) if i not in used_ordered]

    return matches, remaining_billed, remaining_ordered
