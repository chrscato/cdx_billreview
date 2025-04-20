# process/utils/rate_check.py

import sqlite3
import os
import json
from dotenv import load_dotenv
from typing import List, Tuple, Dict
from process.utils.models import Procedure
from process.utils.filter_ancillaries import load_ancillary_cpts

load_dotenv()
PROC_DB_PATH = os.getenv("PROC_DB_PATH", "filemaker.db")
DATA_DIR = os.getenv("DATA_DIR", "process/data/")
PPO_TABLE = "ppo"


def clean_tin(tin: str) -> str:
    return ''.join(filter(str.isdigit, tin or ""))


def extract_modifier(modifiers: List[str]) -> str:
    for mod in modifiers:
        if mod in ['26', 'TC']:
            return mod
    return None


def lookup_rate(cpt: str, tin: str, modifier: str) -> float:
    conn = sqlite3.connect(PROC_DB_PATH)
    cursor = conn.cursor()

    query = f"""
        SELECT rate FROM {PPO_TABLE}
        WHERE proc_cd = ? AND TIN = ? AND (modifier = ? OR (? IS NULL AND modifier IS NULL))
        LIMIT 1
    """
    cursor.execute(query, (cpt, tin, modifier, modifier))
    result = cursor.fetchone()
    conn.close()
    return float(result[0]) if result else None


def validate_rates(procedures: List[Procedure], provider_tin: str) -> Tuple[bool, Dict[str, float], List[str]]:
    ancillary_cpts = set(load_ancillary_cpts())
    tin_clean = clean_tin(provider_tin)

    rate_map = {}
    missing = []

    for proc in procedures:
        cpt = proc.cpt_code
        if cpt in ancillary_cpts:
            rate_map[cpt] = 0.00
            continue

        modifier = extract_modifier(proc.modifiers)
        rate = lookup_rate(cpt, tin_clean, modifier)

        if rate is not None:
            rate_map[cpt] = rate
        else:
            missing.append(cpt)

    rate_failure = len(missing) > 0
    return rate_failure, rate_map, missing
