import boto3
from decimal import Decimal
from datetime import datetime
import re
from fuzzywuzzy import fuzz
from clean import clean_extracted_data  # Assuming this is implemented elsewhere

# AWS client initialization
s3_client = boto3.client('s3')
textract_client = boto3.client('textract')
dynamodb = boto3.resource('dynamodb')

# Configuration
PO_TABLE_NAME = 'po_table'  # Replace with your PO DynamoDB table name
COMPLETED_TABLE_NAME = 'Completed_Items'  # Replace with your completed table name


def extract_customer_id(file_key):
    """
    Extracts the customer ID from the file name (assumes it starts with digits).
    """
    match = re.match(r'^(\d+)', file_key)
    if match:
        return match.group(1)
    else:
        raise ValueError(f"File key '{file_key}' does not start with a customer ID.")


def process_invoice_to_storage(bucket_name, file_key):
    """
    Extracts invoice data, matches with the PO table, and sends Total to the completed table.
    """
    try:
        # Extract customer ID from file name
        customer_id = extract_customer_id(file_key)
        print(f"Extracted Customer ID: {customer_id}")

        # Download the file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        pdf_content = response['Body'].read()
        print(f"PDF Content Size: {len(pdf_content)} bytes")

        # Extract text using Textract instead of Nanonets
        textract_response = textract_client.detect_document_text(
            Document={'Bytes': pdf_content}
        )
        
        # Extract text from Textract response
        extracted_text = ""
        for item in textract_response['Blocks']:
            if item['BlockType'] == 'LINE':
                extracted_text += item['Text'] + "\n"

        # Format as key-value pairs
        extracted_pairs = {'raw_text': extracted_text}
        print(f"Raw Extracted Pairs: {extracted_pairs}")

        # Clean and standardize extracted data
        cleaned_data = clean_extracted_data(extracted_pairs)
        print(f"Cleaned Invoice Data: {cleaned_data}")

        # Validate required fields
        if not cleaned_data.get("invoice_number"):
            raise ValueError("Valid invoice number not found. Skipping this file.")
        if not cleaned_data.get("po_number"):
            raise ValueError("Valid PO number not found. Skipping this file.")
        if not cleaned_data.get("Total"):
            raise ValueError("Valid total amount not found. Skipping this file.")

        # Match invoice with PO in DynamoDB
        po_data = match_po_in_dynamodb(cleaned_data["po_number"])
        if not po_data:
            raise ValueError(f"No matching PO found for PO number: {cleaned_data['po_number']}")

        # Reconcile PO number only
        if not reconcile_invoice_with_po(cleaned_data, po_data):
            raise ValueError(f"Reconciliation failed for invoice and PO number: {cleaned_data['po_number']}")

        # Save the reconciled data to the completed table
        save_to_completed_table(cleaned_data, bucket_name, file_key, customer_id, po_data)

        print(f"Invoice successfully processed and stored for file: {file_key}")
        return cleaned_data

    except Exception as e:
        print(f"Error processing invoice file: {str(e)}")
        return None


def extract_key_value_pairs(textract_response):
    """
    Extract key-value pairs from Textract response and return a dictionary.
    """
    if not textract_response.get('Blocks'):
        print("Textract response does not contain any Blocks.")
        return None

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


def match_po_in_dynamodb(po_number):
    """
    Match PO number in the PO table and return PO data.
    """
    table = dynamodb.Table(PO_TABLE_NAME)
    response = table.get_item(Key={'po_number': po_number})
    return response.get('Item', None)


def reconcile_invoice_with_po(invoice_data, po_data):
    """
    Verify that the PO number exists in the PO table and send the invoice Total.
    """
    # Ensure PO number from the invoice matches the PO data
    if str(invoice_data.get("po_number")) != str(po_data.get("po_number")):
        print(f"PO number mismatch: Invoice PO {invoice_data.get('po_number')} != PO Table {po_data.get('po_number')}")
        return False

    # Log that the PO was matched successfully
    print(f"PO number {invoice_data.get('po_number')} matched successfully.")
    return True


def save_to_completed_table(invoice_data, bucket_name, file_key, customer_id, po_data):
    """
    Save invoice data to the completed table with minimal reconciliation, sending the invoice Total.
    """
    table = dynamodb.Table(COMPLETED_TABLE_NAME)

    # Helper function to ensure all float values are converted to Decimal
    def convert_to_decimal(data):
        if isinstance(data, dict):
            return {k: convert_to_decimal(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [convert_to_decimal(v) for v in data]
        elif isinstance(data, float):
            return Decimal(str(data))
        else:
            return data

    # Prepare the completed item
    db_item = {
        "invoice_number": str(invoice_data.get("invoice_number")),
        "po_number": str(invoice_data.get("po_number")),
        "Total": Decimal(str(invoice_data.get("Total"))),  # Use the invoice's Total directly
        "vendor_name": po_data.get("vendor_name", "Unknown"),
        "UploadDate": datetime.utcnow().isoformat(),
        "CompletionDate": datetime.utcnow().isoformat(),
        "customer_id": Decimal(customer_id),
        "Invoice_PDF_Link": f"s3://{bucket_name}/{file_key}",  # Invoice PDF link
        "PO_PDF_Link": po_data.get("PDFLink", "Unknown"),  # PO PDF link
        "PO_Data": convert_to_decimal(po_data),  # Entire PO data
        "Invoice_Data": convert_to_decimal(invoice_data),  # Entire Invoice data
    }

    # Log the final item
    print(f"Prepared Completed Item: {db_item}")

    # Insert into DynamoDB
    try:
        table.put_item(Item=db_item)
        print(f"Successfully inserted completed item into DynamoDB: {db_item}")
    except Exception as e:
        print(f"Failed to insert completed item into DynamoDB: {e}")
        raise e
