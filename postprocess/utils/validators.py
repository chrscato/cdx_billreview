def validate_record(record):
    """
    Validate that a record meets all requirements for processing (adapted record structure)
    Checks:
    1. Record has line_items with validated rates
    2. Record has required date_of_service
    3. Record has required patient info
    4. Record has required provider info
    """
    # Get order ID for better debugging
    order_id = record.get("order_id", "Unknown")

    data = record.get("data", {})
    line_items = data.get("line_items", [])
    if not line_items:
        print(f"  [DEBUG] No line_items found [Order ID: {order_id}]")
        return False
    for line in line_items:
        if line.get("validated_rate") is None:
            print(f"  [DEBUG] Missing validated_rate in line item: {line} [Order ID: {order_id}]")
            return False

    # Check for date_of_service existence
    has_date = any(line.get("date_of_service") for line in line_items)
    if not has_date:
        print(f"  [DEBUG] Missing date_of_service in line_items [Order ID: {order_id}]")
        return False

    # Check for patient info
    patient_info = data.get("patient_info", {})
    if not patient_info.get("PatientName"):
        print(f"  [DEBUG] Missing 'PatientName' in patient_info: {patient_info} [Order ID: {order_id}]")
        return False

    # Check for provider info
    provider_info = data.get("provider_info", {})
    if not provider_info.get("Billing_Name"):
        print(f"  [DEBUG] Missing 'Billing_Name' in provider_info: {provider_info} [Order ID: {order_id}]")
        return False

    return True