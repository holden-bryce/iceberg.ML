from fuzzywuzzy import process
import re
from datetime import datetime

def clean_extracted_data(extracted_data):
    """
    Cleans and normalizes extracted key-value pairs for downstream processing.

    Parameters:
    - extracted_data (dict): The raw extracted data with keys and values.

    Returns:
    - dict: Cleaned and normalized data with standardized keys.
    """
    key_mapping = {
        'PO Number': 'po_number',
        'P.O. Number': 'po_number',
        'Purchase Order Number': 'po_number',
        'Total Amount': 'Total',
        'Invoice Total': 'Total',
        'Amount': 'Total',
        'Date': 'Date',
        'Vendor Name': 'vendor_name',
        'Company Name': 'company_name',
        'Invoice Number': 'invoice_number',
    }

    cleaned_data = {}

    for raw_key, value in extracted_data.items():
        # Match the raw key to standardized key using FuzzyWuzzy
        best_match, score = process.extractOne(raw_key.strip(), key_mapping.keys())
        print(f"Raw Key: {raw_key}, Best Match: {best_match}, Score: {score}")

        if score >= 80:
            normalized_key = key_mapping[best_match]
        else:
            normalized_key = raw_key.strip()  # Use raw key if no good match found

        # Clean the value based on its key type
        if isinstance(value, str):
            value = value.strip()
            value = re.sub(r'[^\d\w\s.-]', '', value)  # Remove unwanted characters

        if normalized_key == 'Total':
            value = convert_to_float(value)
        elif normalized_key in ['po_number', 'invoice_number']:
            value = ''.join(filter(str.isdigit, value))  # Extract only digits

        print(f"Normalized Key: {normalized_key}, Cleaned Value: {value}")
        cleaned_data[normalized_key] = value

    # Convert recognized dates to ISO format
    if 'Date' in cleaned_data:
        cleaned_data['Date'] = convert_to_iso_date(cleaned_data['Date'])

    return cleaned_data


def convert_to_float(value):
    """
    Converts a value to a float. Strips trailing text like "USD" before conversion.
    Returns None if conversion fails.
    """
    try:
        if isinstance(value, str):
            # Remove non-numeric characters except for "." and ","
            value = re.sub(r'[^\d.,]', '', value).replace(',', '')
        return float(value)
    except (ValueError, TypeError):
        print(f"Warning: Unable to convert value '{value}' to float.")
        return None

def convert_to_iso_date(date_str):
    """
    Converts a date string into ISO 8601 format (YYYY-MM-DD).
    Supports multiple date formats.
    """
    formats = [
        "%B %d, %Y",  # Full month name, e.g., "January 15, 2024"
        "%b %d, %Y",  # Abbreviated month name, e.g., "Jan 15, 2024"
        "%Y-%m-%d"    # ISO format as input
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date().isoformat()
        except ValueError:
            continue
    print(f"Warning: Unable to parse date '{date_str}'. Returning original.")
    return date_str  # Return original if unable to parse


# Example Usage
if __name__ == "__main__":
    # Example extracted data
    raw_data = {
        'P.O. Number': ' 45678 ',
        'Total Amount': '$12,500.00',
        'Supplier Name': ' BRYCEBIZ AI ARMY ',
        'Date': 'November 25, 2024',
        'Invoice No': ' 98765 '
    }

    cleaned = clean_extracted_data(raw_data)
    print("Cleaned Data:", cleaned)
