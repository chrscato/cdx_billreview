from datetime import datetime, timedelta
from dateutil.parser import parse
import holidays

def format_date_for_eob(date_str):
    """Format date string for EOB document"""
    if not date_str:
        return ""
    try:
        # Handle date ranges (e.g., "04/02/25 - 04/02/25")
        if " - " in date_str:
            # Just take the first date in the range
            date_str = date_str.split(" - ")[0]
        
        date_obj = parse(date_str)
        return date_obj.strftime("%Y-%m-%d")
    except Exception as e:
        print(f"Error parsing date '{date_str}': {e}")
        # Return a safe default if parsing fails
        return datetime.now().strftime("%Y-%m-%d")

def format_date(date_str):
    """General date formatter"""
    if not date_str:
        return ""
    try:
        date_obj = parse(date_str)
        return date_obj.strftime("%Y-%m-%d")
    except Exception:
        return date_str  # Return original if parsing fails

def format_currency(amount):
    """Format amount as currency"""
    return "${:,.2f}".format(float(amount))

def calculate_due_date(bill_date):
    """Calculate due date based on bill date (45 business days)"""
    us_holidays = holidays.US(years=[2021, 2022, 2023, 2024, 2025])
    due_date = bill_date
    days_added = 0
    while days_added < 45:
        due_date += timedelta(days=1)
        if due_date.weekday() < 5 and due_date not in us_holidays:
            days_added += 1
    return due_date