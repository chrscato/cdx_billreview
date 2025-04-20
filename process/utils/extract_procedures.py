# process/utils/extract_procedures.py

from typing import List, Dict
from process.utils.models import Procedure
from datetime import datetime


def extract_from_service_lines(service_lines: List[Dict]) -> List[Procedure]:
    """
    Converts 'service_lines' from the claim JSON into a list of standardized Procedure objects.
    """
    procedures = []
    for line in service_lines:
        procedures.append(Procedure(
            cpt_code=str(line.get("cpt_code", "")).strip(),
            date_of_service=parse_dos(line.get("date_of_service")),
            units=int(line.get("units", 1)),
            source="billed",
            modifiers=line.get("modifiers", []),
            raw=line
        ))
    return procedures


def extract_from_line_items(line_items: List[Dict]) -> List[Procedure]:
    """
    Converts 'filemaker.line_items' into standardized Procedure objects.
    """
    procedures = []
    for line in line_items:
        procedures.append(Procedure(
            cpt_code=str(line.get("CPT", "")).strip(),
            date_of_service=parse_dos(line.get("DOS")),
            units=int(line.get("Units", 1)),
            source="ordered",
            modifiers=[line.get("Modifier")] if line.get("Modifier") and line.get("Modifier") != "None" else [],
            raw=line
        ))
    return procedures


def parse_dos(dos_str: str):
    """
    Parses DOS from a range or single date. Returns only the start date in YYYY-MM-DD format.
    """
    if not dos_str:
        return None
    if "-" in dos_str:
        dos_str = dos_str.split("-")[0].strip()
    try:
        return datetime.strptime(dos_str, "%m/%d/%Y").date().isoformat()
    except ValueError:
        try:
            return datetime.strptime(dos_str, "%Y-%m-%d").date().isoformat()
        except:
            return None
