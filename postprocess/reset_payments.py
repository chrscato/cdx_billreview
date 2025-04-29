import sqlite3
from pathlib import Path
from config.settings import DB_PATH

def reset_payment_fields(line_item_ids):
    """
    Reset payment fields to NULL for specified line items
    
    Args:
        line_item_ids (list): List of line item IDs to reset
    """
    if not Path(DB_PATH).exists():
        print(f"Error: Database file not found at {DB_PATH}")
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Update the line_items table
        cursor.execute('''
        UPDATE line_items SET 
            BR_paid = NULL,
            BR_rate = NULL,
            EOBR_doc_no = NULL,
            HCFA_doc_no = NULL,
            BR_date_processed = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id IN ({})
        '''.format(','.join('?' * len(line_item_ids))), line_item_ids)
        
        rows_affected = cursor.rowcount
        conn.commit()
        
        print(f"Reset payment info for {rows_affected} line items")
        
        # Verify the changes
        cursor.execute('''
        SELECT id, Order_ID, CPT, BR_paid, BR_rate, EOBR_doc_no 
        FROM line_items 
        WHERE id IN ({})
        '''.format(','.join('?' * len(line_item_ids))), line_item_ids)
        
        rows = cursor.fetchall()
        print("\nUpdated records:")
        for row in rows:
            print(f"  ID: {row[0]}, Order: {row[1]}, CPT: {row[2]}, Paid: {row[3]}, Rate: {row[4]}, EOBR: {row[5]}")
            
    except Exception as e:
        print(f"Error resetting payment info: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    # List of line item IDs to reset
    line_item_ids = [
        # Add your line item IDs here
        # Example:
20612,
20664,
22253,
22495,
22292,
23112,
23307,
23308,
23306,
23305,
73922,
22627,
22669,
8806,
8807,
22009,
23252,
10432,
18438,
23548,
8864,
75836,
23695,
22139,
76213,
76214,
9508,
76006,
75710,
75840,
75739,
76007,
76257,
76136,
76171,
76281,
76258,
76161,
76160,
75854,
76123,
75972,
75973,
75991,
76132,
76150,
76430,
75778,
75849,
18052,
18051,
76002,
76014,
75919,
75777,
76162,
76341,
76170,
76339,
76155,
76076,
76077,
76294,
76295,
76293,
76362,
76363,
76335,
76274,
75876,
75820,
76425,
76429,
76327,
76522,
76333,
75715,
75846,
76064,
76024,
76177,
76055,
76164,
76074,
76270,
75855,
75856,
76261,
76306,
76206,
76100,
75978,
75980,
76019,
75738,
76193,
76051,
76159,
76508,
76480,
76457,
75834,
76592,
76388,
76568,
76184,
76540,
76541,
76351,
76621,
76571,
76442,
76190,
76477,
76494,
76707,
76488,
76139,
76638,
76122,
76334,
76083,
76793,
76732,
76517,
76795,
76626,
76265,
76769,
76340,
76027,
76563,
76620,
23055,
22756,
22677,
76722,
76723,
76724,
20880,
76785,
21213,
20140,
22207,
76412,
76720,
20490,
23057,
23075,
23074,
23465,
76982,
76589,
76581,
76189,
76441,
76516,
76580,
76967,
76936,
76850,
76629,
22749,
76116,
76402,
76506,
76737,
76738,
77053,
21348,
21208,
18192,
75874,
76574,
76474,
76332,
76421,
23616,
21370,
76974,
76668,
76703,
76789,
76817,
76719,
76369,
76393,
76316,
76090,
22064,
19992,
18581,
22506,
22886,
18096,
20077,
22542,
21488

    ]
    
    if not line_item_ids:
        print("Please add line item IDs to the list in the script")
    else:
        print(f"Resetting payment fields for {len(line_item_ids)} line items...")
        reset_payment_fields(line_item_ids) 