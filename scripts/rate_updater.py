#!/usr/bin/env python3
import os
import json
import sqlite3
import logging
import paramiko
from datetime import datetime
from dotenv import load_dotenv
import tempfile


# Load environment variables
load_dotenv()

# Constants
REMOTE_HOST = '159.223.104.254'
REMOTE_USER = 'root'
REMOTE_PASSWORD = os.getenv('REMOTE_VM_PASSWORD')
REMOTE_DB_PATH = '/srv/bill_review/filemaker.db'
REMOTE_DATA_DIR = '/srv/bill_review/data/rate_updates'
REMOTE_ARCHIVE_DIR = f'{REMOTE_DATA_DIR}/archive'
LOCAL_TEMP_DB = os.path.join(tempfile.gettempdir(), 'filemaker_temp.db')

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('rate_updater.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def connect_sftp():
    """Establish SFTP connection."""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(REMOTE_HOST, username=REMOTE_USER, password=REMOTE_PASSWORD)
    sftp = ssh.open_sftp()
    return ssh, sftp


def fetch_db(sftp):
    """Download the remote database to local temp file."""
    sftp.get(REMOTE_DB_PATH, LOCAL_TEMP_DB)


def push_db(sftp):
    """Push modified database back to remote."""
    sftp.put(LOCAL_TEMP_DB, REMOTE_DB_PATH)


def ensure_table(conn):
    """Create ppo table if not exists."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ppo (
            id TEXT,
            RenderingState TEXT,
            TIN TEXT,
            provider_name TEXT,
            proc_cd TEXT,
            modifier TEXT,
            proc_desc TEXT,
            proc_category TEXT,
            rate REAL
        );
    """)


def insert_ppo_rate(conn, record):
    """Insert a single PPO rate."""
    sql = """
        INSERT INTO ppo (id, RenderingState, TIN, provider_name, proc_cd, modifier, proc_desc, proc_category, rate)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    conn.execute(sql, record)


def archive_file(sftp, remote_path):
    """Move processed file to archive folder on remote."""
    filename = os.path.basename(remote_path)
    sftp.mkdir(REMOTE_ARCHIVE_DIR) if not remote_path_exists(sftp, REMOTE_ARCHIVE_DIR) else None
    archive_path = f"{REMOTE_ARCHIVE_DIR}/{filename}"
    sftp.rename(remote_path, archive_path)
    logger.info(f"Archived: {filename}")


def remote_path_exists(sftp, path):
    try:
        sftp.stat(path)
        return True
    except FileNotFoundError:
        return False
    
def process_ota_file(json_data, conn):
    """Handle create_ota rate type."""
    order_id = json_data.get("order_id")
    rates = json_data.get("rates", [])
    if not order_id or not isinstance(rates, list):
        return False

    for item in rates:
        if "cpt_code" in item and "rate" in item:
            sql = """
            INSERT INTO current_otas (ID_Order_PrimaryKey, CPT, modifier, rate)
            VALUES (?, ?, ?, ?);
            """
            conn.execute(sql, (
                order_id,
                str(item["cpt_code"]),
                item.get("modifier", ""),
                float(item["rate"])
            ))
    logger.info(f"Inserted OTA rates for order: {order_id}")
    return True


def process_category_file(json_data, conn):
    """Handle category-based rate type."""
    tin = json_data.get("tin")
    provider_name = json_data.get("ota_data", {}).get("provider_name", "")
    category_rates = json_data.get("category_rates", {})
    success = False

    for category, rate in category_rates.items():
        cursor = conn.execute("""
            SELECT proc_cd, modifier, proc_desc, category
            FROM dim_proc
            WHERE LOWER(category) = LOWER(?)
        """, (category.lower(),))
        for row in cursor.fetchall():
            proc_cd, modifier, proc_desc, proc_category = row
            record = (
                f"PPO_{tin}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                '',
                tin,
                provider_name,
                proc_cd,
                modifier or '',
                proc_desc or '',
                proc_category or '',
                float(rate)
            )
            insert_ppo_rate(conn, record)
            logger.info(f"Inserted category rate: {record}")
            success = True
    return success

def process_individual_file(json_data, conn):
    """Handle individual rate type and insert into PPO table."""
    tin = json_data.get("tin")
    if not tin:
        logger.warning("Missing TIN in individual rate file")
        return False

    provider_name = json_data.get("ota_data", {}).get("provider_name", "") if isinstance(json_data.get("ota_data"), dict) else ""
    unique_id = f"PPO_{tin}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    success = False

    rates = json_data.get("rates", [])
    if not isinstance(rates, list):
        logger.warning("Invalid or missing 'rates' list in individual rate file")
        return False

    for item in rates:
        if "cpt_code" in item and "rate" in item:
            record = (
                unique_id,
                '',  # RenderingState
                tin,
                provider_name,
                str(item["cpt_code"]),
                item.get("modifier", ""),
                '',  # proc_desc
                '',  # proc_category
                float(item["rate"])
            )
            insert_ppo_rate(conn, record)
            logger.info(f"Inserted: {record}")
            success = True
        else:
            logger.warning(f"Skipping incomplete rate item: {item}")

    return success


def process_file(json_data, conn):
    """Dispatch processing based on rate_type."""
    rate_type = json_data.get("rate_type", "").lower()

    if rate_type == "individual":
        return process_individual_file(json_data, conn)
    elif rate_type == "create_ota":
        return process_ota_file(json_data, conn)
    elif rate_type == "category":
        return process_category_file(json_data, conn)
    else:
        logger.warning(f"Unknown rate_type: {rate_type}")
        return False



def main():
    logger.info("Starting rate update process")

    ssh, sftp = connect_sftp()
    fetch_db(sftp)

    # Connect to local temp DB
    conn = sqlite3.connect(LOCAL_TEMP_DB)
    ensure_table(conn)

    # Process .json files in remote dir
    for fname in sftp.listdir(REMOTE_DATA_DIR):
        if fname.endswith(".json"):
            remote_file_path = f"{REMOTE_DATA_DIR}/{fname}"
            with sftp.file(remote_file_path, 'r') as f:
                try:
                    data = json.load(f)
                    if process_file(data, conn):
                        archive_file(sftp, remote_file_path)
                except Exception as e:
                    logger.error(f"Failed to process {fname}: {e}")

    # Commit and push DB
    conn.commit()
    conn.close()
    push_db(sftp)

    # Cleanup
    sftp.close()
    ssh.close()
    logger.info("Rate update process complete")


if __name__ == "__main__":
    main()
