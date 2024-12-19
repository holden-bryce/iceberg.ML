import boto3
from decimal import Decimal
from datetime import datetime
import re
from fuzzywuzzy import fuzz, process
from clean import clean_extracted_data 
import json  # Import the cleaning function

# AWS client initialization
s3_client = boto3.client('s3')
textract_client = boto3.client('textract')
dynamodb = boto3.resource('dynamodb')

# Configuration
PO_TABLE_NAME = 'po_table'  # Replace with your DynamoDB table name

# Define expected keys with synonyms
EXPECTED_KEYS = {
    "po_number": ["po_number", "PO Number", "PO#", "Purchase Order Number"],
    "Total": ["Total", "order_total", "Amount", "Total Price"],
    "Date": ["Date", "Order Date", "order_date"],
    "customer_id": ["customer_id", "Customer ID", "client_id"]
}


def extract_customer_id(file_key):
    """
    Extracts the customer ID from the first digits of the file name.
    Assumes the file name starts with the customer ID (e.g., "12345_filename.pdf").
    """
    match = re.match(r'^(\d+)', file_key)
    if match:
        return match.group(1)
    else:
        raise ValueError(f"File key '{file_key}' does not start with a customer ID.")


def clean_amount(amount_str):
    """
    Cleans amount string and converts to Decimal.
    """
    try:
        if not amount_str:
            return Decimal('0')
        # Remove any non-numeric characters except decimal point and negative sign
        cleaned = re.sub(r'[^\d.-]', '', str(amount_str))
        return Decimal(cleaned)
    except Exception as e:
        print(f"Error cleaning amount: {str(e)}")
        return Decimal('0')


def process_po_to_storage(extracted_data, bucket_name, file_key):
    try:
        # Extract customer ID from filename
        customer_id = extract_customer_id(file_key)
        print(f"Extracted Customer ID: {customer_id}")
        
        # Initialize raw text
        raw_text = ""
        
        # Handle both Nanonets response and Textract fallback
        if isinstance(extracted_data, dict):
            if 'result' in extracted_data and extracted_data['result']:
                raw_text = extracted_data['result'][0]['prediction'][0].get('ocr_text', '')
        else:
            raw_text = str(extracted_data)

        print(f"Raw text to process: {raw_text}")

        # Updated regex patterns for Quaker Houghton PO format
        extracted_fields = {
            'po_number': safe_extract(raw_text, r'Order\s*Number\s*(\d+(?:\s*OP)?)|Customer\s*PO\s*Number\s*(\d+)'),
            'Date': safe_extract(raw_text, r'Ordered\s*(\d{4}/\d{2}/\d{2})'),
            'Total': safe_extract(raw_text, r'Total\s*Order\s*excl\.\s*VAT\s*(\d{1,3}(?:,\d{3})*\.\d{2})'),
            'vendor_name': 'LABTECH CORP',
            'company_name': 'Quaker Houghton',
            'shipping_address': safe_extract(raw_text, r'Ship\s*To\s*Address.*?(?=UNITED\s*STATES)(.*?)(?=Ship\s*Via|$)', re.DOTALL),
            'payment_terms': safe_extract(raw_text, r'Payment\s*Terms\s*(.*?)(?=\n|$)'),
            'order_date': safe_extract(raw_text, r'Ordered\s*(\d{4}/\d{2}/\d{2})')
        }

        print(f"Extracted fields: {extracted_fields}")

        # Clean and prepare data for DynamoDB
        db_item = {
            'po_number': str(extracted_fields.get('po_number', '').replace(' OP', '') or 'Unknown'),
            'Date': extracted_fields.get('Date') or extracted_fields.get('order_date') or 'Unknown',
            'Total': clean_amount(extracted_fields.get('Total', '0')),
            'vendor_name': extracted_fields.get('vendor_name'),
            'company_name': extracted_fields.get('company_name'),
            'customer_id': int(customer_id),  # Store as integer
            'PDFLink': f's3://{bucket_name}/{file_key}',
            'UploadDate': datetime.now().isoformat(),
            'Shipping To': extracted_fields.get('shipping_address', '').strip() or 'Unknown',
            'payment_terms': extracted_fields.get('payment_terms') or 'Unknown',
            'Description': '',  # Added to match table schema
            'Freight Terms': '',  # Added to match table schema
            'Item': '',  # Added to match table schema
            'Quantity': '',  # Added to match table schema
            'Status': '',  # Added to match table schema
            'Unit Price': '',  # Added to match table schema
            'Vendor': extracted_fields.get('vendor_name', 'LABTECH CORP')
        }

        print(f"Prepared DynamoDB item: {db_item}")

        # Store in DynamoDB
        table = dynamodb.Table(PO_TABLE_NAME)
        table.put_item(Item=db_item)
        
        return db_item  # Return the complete item

    except Exception as e:
        print(f"Error processing PO: {str(e)}")
        raise

def clean_json_data(raw_data, bucket_name, file_key):
    """
    Cleans raw JSON data and maps it to expected fields using fuzzy matching.
    """
    cleaned_data = {}
    for target_key, expected_keys in EXPECTED_KEYS.items():
        # Check all possible synonyms for the expected key
        matched_key, score = None, 0
        for expected_key in expected_keys:
            match, match_score = process.extractOne(expected_key, raw_data.keys(), scorer=fuzz.token_sort_ratio)
            if match_score > score:  # Keep the highest scoring match
                matched_key, score = match, match_score
        
        if score >= 80:  # Adjust the score threshold as needed
            print(f"Mapped '{matched_key}' to '{target_key}' with score {score}")
            cleaned_data[target_key] = raw_data[matched_key]
        else:
            print(f"Could not map key for '{target_key}' confidently. Skipping.")

    # Add metadata fields for DynamoDB
    cleaned_data["PDFLink"] = f"s3://{bucket_name}/{file_key}"
    cleaned_data["UploadDate"] = datetime.utcnow().isoformat()

    print(f"Cleaned Data from JSON: {cleaned_data}")
    return cleaned_data


def extract_key_value_pairs(textract_response):
    """
    Extract key-value pairs from Textract response and return a dictionary.
    """
    key_map, value_map, block_map = {}, {}, {}

    for block in textract_response['Blocks']:
        block_id = block.get('Id')
        if not block_id:
            continue

        block_map[block_id] = block
        if block['BlockType'] == 'KEY_VALUE_SET':
            if 'KEY' in block['EntityTypes']:
                key_map[block_id] = block
            elif 'VALUE' in block['EntityTypes']:
                value_map[block_id] = block

    def get_text(block):
        """
        Extract text from a block.
        """
        if 'Text' in block:
            return block['Text']
        if 'Relationships' in block:
            text = []
            for relation in block['Relationships']:
                if relation['Type'] == 'CHILD':
                    for child_id in relation['Ids']:
                        child_block = block_map.get(child_id)
                        if child_block and child_block['BlockType'] == 'WORD':
                            text.append(child_block.get('Text', ''))
            return ' '.join(text)
        return ""

    key_value_pairs = {}
    for key_id, key_block in key_map.items():
        key_text = get_text(key_block)
        value_block = None
        for relation in key_block.get('Relationships', []):
            if relation['Type'] == 'VALUE':
                for value_id in relation['Ids']:
                    value_block = value_map.get(value_id)
                    if value_block:
                        break
        if value_block:
            value_text = get_text(value_block)
            key_value_pairs[key_text] = value_text

    return key_value_pairs
    
def safe_extract(text, pattern, flags=0):
    """
    Safely extracts text using regex pattern with error handling.
    """
    try:
        match = re.search(pattern, text, flags)
        if match:
            # If there are groups, return the first non-None group
            groups = match.groups()
            if groups:
                return next((g for g in groups if g is not None), '')
            return match.group(0)
        return ''
    except Exception as e:
        print(f"Error in safe_extract: {str(e)}")
        return ''

