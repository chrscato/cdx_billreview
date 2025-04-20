# process/utils/models.py

from dataclasses import dataclass, field
from typing import List, Optional, Dict


@dataclass
class Procedure:
    cpt_code: str
    date_of_service: Optional[str] = None  # Format: YYYY-MM-DD
    units: int = 1
    modifiers: List[str] = field(default_factory=list)
    source: str = "unknown"  # 'billed' or 'ordered'
    raw: Dict = field(default_factory=dict)  # Original line for traceability

    def __post_init__(self):
        self.cpt_code = self.cpt_code.strip().upper()
        self.modifiers = [m.strip().upper() for m in self.modifiers if m]


@dataclass
class MatchResult:
    billed: Procedure
    ordered: Procedure
    match_type: str  # 'EXACT', 'BUNDLED', 'CLINICAL_EQUIVALENT', etc.
    details: Optional[str] = None
