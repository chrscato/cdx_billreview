# process/utils/unit_validator.py

from typing import List, Tuple
from process.utils.models import Procedure


def validate_units(procedures: List[Procedure]) -> Tuple[bool, List[str]]:
    """
    Flags any CPTs with units > 1.

    Returns:
        has_violation (bool): True if any CPT has units > 1
        messages (List[str]): Descriptive failures
    """
    violations = []
    for proc in procedures:
        if proc.units > 1:
            violations.append(f"TOO_MANY_UNITS: {proc.cpt_code} billed with {proc.units} units")

    return (len(violations) > 0, violations)
