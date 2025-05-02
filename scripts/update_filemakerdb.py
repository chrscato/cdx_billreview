import csv
import sqlite3
import datetime
import logging
import os
import re

def format_date(date_str):
    if not date_str:
        return None
    date_str = date_str.strip()
    formats = ['%m/%d/%Y', '%m-%d-%Y', '%m/%d/%y', '%m-%d-%y', '%Y/%m/%d', '%Y-%m-%d']
    for fmt in formats:
        try:
            date_obj = datetime.datetime.strptime(date_str, fmt)
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            continue
    logging.warning(f"Could not parse date: {date_str}")
    return None

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("import_log.txt", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def generate_order_id(fmr, patient_info):
    last_name = patient_info.get('Patient_Last_Name', '')
    first_name = patient_info.get('Patient_First_Name', '')
    patient_name = patient_info.get('PatientName', '')
    name_part = f"{last_name}{first_name}".strip() if last_name or first_name else patient_name.strip() if patient_name else "UNKNOWN"
    name_part = re.sub(r'[^a-zA-Z0-9]', '', name_part)[:10].upper()
    return f"ORD-{fmr}-{name_part}"

def clean_and_insert_from_csv(csv_path, db_path):
    if not os.path.exists(csv_path):
        logging.error(f"CSV file not found: {csv_path}")
        return False

    conn = sqlite3.connect(db_path, timeout=300)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON")

    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS import_test (test_id INTEGER PRIMARY KEY, timestamp TEXT)")
        cursor.execute("INSERT INTO import_test (timestamp) VALUES (?)", (datetime.datetime.now().isoformat(),))
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Database write test failed: {e}")
        conn.close()
        return False

    total_rows, skipped_rows, processed_rows = 0, 0, 0
    orders = {}
    line_items = []
    order_id_map = {}
    duplicate_fmrs = set()
    special_names = ["Brett Jefferies"]

    try:
        with open(csv_path, 'r', encoding='utf-8-sig') as csv_file:
            reader = csv.DictReader(csv_file)
            for row_num, row in enumerate(reader, start=2):
                total_rows += 1
                patient_name = row.get('PatientName', '')
                is_special = any(name in patient_name for name in special_names) if patient_name else False

                for field in ['Modifier', 'CPT', 'Charge', 'DOS_from', 'DOS_to']:
                    if row.get(field) and isinstance(row[field], str):
                        row[field] = ''.join(c for c in row[field] if c.isprintable())

                for key in row:
                    if row[key] is None or (isinstance(row[key], str) and row[key].strip() == ''):
                        row[key] = None
                    elif isinstance(row[key], str):
                        row[key] = row[key].strip()

                fmr = row.get('FileMaker_Record_Number')
                original_fmr = fmr
                if fmr is None:
                    skipped_rows += 1
                    continue

                try:
                    fmr = str(fmr).strip().lstrip("'`'").replace('\u200b', '')
                    if 'e' in fmr.lower():
                        fmr = f"{float(fmr):.0f}"
                    if '.' in fmr:
                        fmr = fmr.rstrip('0').rstrip('.')
                    if not fmr.isdigit():
                        fmr = f"{float(fmr):.0f}"
                except:
                    skipped_rows += 1
                    continue

                processed_rows += 1
                patient_info = {
                    'Patient_Last_Name': row.get('Patient_Last_Name'),
                    'Patient_First_Name': row.get('Patient_First_Name'),
                    'PatientName': row.get('PatientName')
                }
                order_id = generate_order_id(fmr, patient_info)
                if fmr in order_id_map:
                    if order_id not in order_id_map[fmr]:
                        order_id_map[fmr].append(order_id)
                        duplicate_fmrs.add(fmr)
                else:
                    order_id_map[fmr] = [order_id]

                orders[order_id] = {
                    'Order_ID': order_id,
                    'FileMaker_Record_Number': fmr,
                    'Patient_Address': row.get('Patient_Address'),
                    'Patient_City': row.get('Patient_City'),
                    'Patient_State': row.get('Patient_State'),
                    'Patient_Zip': row.get('Patient_Zip'),
                    'Patient_DOB': row.get('Patient_DOB'),
                    'Patient_Last_Name': row.get('Patient_Last_Name'),
                    'Patient_First_Name': row.get('Patient_First_Name'),
                    'PatientName': row.get('PatientName'),
                    'PatientPhone': row.get('PatientPhone'),
                    'Referring_Physician': row.get('Referring_Physician'),
                    'Referring_Physician_NPI': row.get('Referring_Physician_NPI'),
                    'Assigning_Company': row.get('Assigning_Company'),
                    'Assigning_Adjuster': row.get('Assigning_Adjuster'),
                    'Claim_Number': row.get('Claim_Number'),
                    'Order_Type': row.get('Order_Type'),
                    'Jurisdiction_State': row.get('Jurisdiction_State'),
                    'created_at': datetime.datetime.now().isoformat(),
                    'updated_at': datetime.datetime.now().isoformat(),
                    'is_active': True,
                    'provider_id': row.get('provider_id'),
                    'provider_name': row.get('provider_name'),
                    'Patient_Injury_Date': None,
                    'Patient_Injury_Description': None,
                    'bundle_type': None
                }

                if any(row.get(field) for field in ['CPT', 'DOS_from', 'Charge', 'Units', 'Modifier', 'DOS_to']):
                    charge = row.get('Charge')
                    try:
                        charge = float(str(charge).replace('$', '').replace(',', '')) if charge else None
                    except:
                        charge = None

                    units = row.get('Units')
                    try:
                        units = int(float(units)) if units and str(units).replace('.', '', 1).isdigit() else None
                    except:
                        units = None

                    dos_value = row.get('DOS_from')
                    formatted_dos = format_date(dos_value) if dos_value else None

                    line_items.append({
                        'Order_ID': order_id,
                        'DOS': formatted_dos,
                        'CPT': row.get('CPT'),
                        'Modifier': row.get('Modifier'),
                        'Units': units,
                        'Description': None,
                        'Charge': charge,
                        'line_number': None,
                        'created_at': datetime.datetime.now().isoformat(),
                        'updated_at': datetime.datetime.now().isoformat(),
                        'is_active': True,
                        'BR_paid': None,
                        'BR_rate': None,
                        'EOBR_doc_no': None,
                        'HCFA_doc_no': None,
                        'BR_date_processed': None
                    })

        logging.info(f"Found {len(duplicate_fmrs)} duplicate FMRs")

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'")
        if not cursor.fetchone():
            logging.error("Orders table missing")
            return False

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='line_items'")
        if not cursor.fetchone():
            logging.error("Line items table missing")
            return False

        logging.info(f"Processing {len(orders)} orders and {len(line_items)} line items")

        # Insert orders (same logic as before, still safe to replace)
        order_items = list(orders.items())
        orders_inserted = 0
        batch_size = 100

        for i in range(0, len(order_items), batch_size):
            batch = order_items[i:i+batch_size]
            conn.execute("BEGIN IMMEDIATE TRANSACTION")
            for order_id, order_data in batch:
                columns = ", ".join(order_data.keys())
                placeholders = ", ".join(["?" for _ in order_data])
                values = list(order_data.values())
                try:
                    cursor.execute(f"INSERT OR REPLACE INTO orders ({columns}) VALUES ({placeholders})", values)
                    orders_inserted += 1
                except sqlite3.Error as e:
                    logging.error(f"Insert failed for order {order_id}: {e}")
            conn.commit()

        # Insert or update line items (new logic)
        line_counts = {}
        for item in line_items:
            oid = item['Order_ID']
            line_counts[oid] = line_counts.get(oid, 0) + 1
            item['line_number'] = line_counts[oid]

        line_items_inserted = 0
        line_items_updated = 0

        for i in range(0, len(line_items), batch_size):
            batch = line_items[i:i+batch_size]
            conn.execute("BEGIN IMMEDIATE TRANSACTION")
            for item in batch:
                cursor.execute("""
                    SELECT id FROM line_items
                    WHERE Order_ID = ? AND CPT = ? AND DOS = ?
                """, (item['Order_ID'], item['CPT'], item['DOS']))
                existing = cursor.fetchone()

                if existing:
                    cursor.execute("""
                        UPDATE line_items
                        SET Modifier = ?, Units = ?, Description = ?, Charge = ?, line_number = ?, 
                            updated_at = ?, is_active = ?
                        WHERE id = ?
                    """, (
                        item['Modifier'], item['Units'], item['Description'], item['Charge'],
                        item['line_number'], item['updated_at'], item['is_active'],
                        existing[0]
                    ))
                    line_items_updated += 1
                else:
                    columns = ", ".join(item.keys())
                    placeholders = ", ".join(["?" for _ in item])
                    values = list(item.values())
                    try:
                        cursor.execute(f"INSERT INTO line_items ({columns}) VALUES ({placeholders})", values)
                        line_items_inserted += 1
                    except sqlite3.Error as e:
                        logging.error(f"Insert failed for line item: {e}")
            conn.commit()

        logging.info(f"Orders inserted: {orders_inserted}")
        logging.info(f"Line items inserted: {line_items_inserted}, updated: {line_items_updated}")
        return True

    except Exception as e:
        logging.error(f"CSV Processing error: {e}")
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 2:
        csv_path = sys.argv[1]
        db_path = sys.argv[2]
    else:
        csv_path = r"C:\Users\ChristopherCato\Downloads\li_export_5.1.csv"
        db_path = r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\bill_review\filemaker.db"
    logging.info(f"Starting import from {csv_path} to {db_path}")
    success = clean_and_insert_from_csv(csv_path, db_path)
    logging.info("Import completed successfully" if success else "Import failed")
