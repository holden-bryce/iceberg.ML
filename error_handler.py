import boto3
import json
import uuid
from email import policy
from email.parser import BytesParser
from decimal import Decimal
from datetime import datetime
from fuzzywuzzy import fuzz, process as fuzzy_process
import re

# Import dependencies for processing
from po_to_storage import process_po_to_storage
from invoice_to_storage import process_invoice_to_storage
from labtechapi import process_with_labtechapi
from brycebizapi import process_with_brycebiz
from clean import clean_extracted_data
from holden_po_processor import process_holden_po_email

from datetime import datetime
from error_handler import IcebergErrorHandler

# Initialize error handler at the top with other initializations
error_handler = IcebergErrorHandler()

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
textract_client = boto3.client('textract')

# Define buckets
RAW_EMAIL_BUCKET = 'icebergrawmail'
PO_BUCKET = 'icebergpos'
INVOICE_BUCKET = 'iceberginvoices'
HOLDEN_JSON_BUCKET = 'jsonfiles4holden'

# Define DynamoDB tables
PO_TABLE = dynamodb.Table('po_table')
COMPLETED_TABLE = dynamodb.Table('Completed_Items')

# Define key aliases for fuzzy matching
KEY_ALIASES = {
    "po_number": ["PO#", "Purchase Order", "PO Number"],
    "order_total": ["Order Total", "Total Amount", "Amount"],
    "order_date": ["Order Date", "Date of Order", "Purchase Date"],
}

# Fuzzy match threshold
FUZZY_MATCH_THRESHOLD = 80

# Receiver email to customer ID mapping
RECEIVER_TO_CUSTOMER_ID = {
    "labtech@flowerwork.co": "444",
    "holden@flowerwork.co": "69420",
    # Add more mappings as needed
}

# API processors for each internal customer ID
API_PROCESSORS = {
    "444": process_with_labtechapi,
    "69420": process_with_brycebiz,
    # Add other processors as needed
}

def lambda_handler(event, context):
    """
    Main entry point for handling S3 events.
    """
    try:
        for record in event['Records']:
            bucket_name = record['s3']['bucket']['name']
            file_key = record['s3']['object']['key']

            print(f"Processing file from bucket: {bucket_name}, key: {file_key}")

            if bucket_name == RAW_EMAIL_BUCKET:
                process_email(bucket_name, file_key)
            elif bucket_name == PO_BUCKET:
                process_po(bucket_name, file_key)
            elif bucket_name == INVOICE_BUCKET:
                process_invoice(bucket_name, file_key)
            else:
                print(f"Unrecognized bucket: {bucket_name}. Skipping.")

        return {"statusCode": 200, "body": "Event processed successfully"}
    except Exception as e:
        print(f"Error handling event: {e}")
        return {"statusCode": 500, "body": f"Error: {str(e)}"}

def process_email(bucket_name, file_key):
    """
    Processes emails, extracts PDFs or data, and routes them.
    """
    try:
        start_time = datetime.now()
        
        email_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        email_content = email_obj['Body'].read()
        msg = BytesParser(policy=policy.default).parsebytes(email_content)
        
        receiver_email = extract_receiver_email(msg['to'])
        customer_id = RECEIVER_TO_CUSTOMER_ID.get(receiver_email, "unknown")
        
        if customer_id == "unknown":
            raise ValueError(f"Receiver email {receiver_email} not recognized")
        
        if customer_id == "69420":  # Holden's customer ID
            result = process_holden_po_email(email_content)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            error_handler.notify_success(
                file_key=file_key,
                document_type='Holden PO Email',
                customer_id=customer_id,
                processing_details={
                    'source_bucket': bucket_name,
                    'processing_time': processing_time,
                }
            )
            
            return result

        for part in msg.iter_attachments():
            if part.get_content_type() == 'application/pdf':
                pdf_content = part.get_payload(decode=True)
                subject = msg['subject'] or ""
                body = msg.get_body(preferencelist=('plain')).get_content() if msg.get_body() else ""

                if "invoice" in subject.lower() or "invoice" in body.lower():
                    upload_pdf_to_bucket(INVOICE_BUCKET, pdf_content, customer_id)
                elif "po" in subject.lower() or "purchase order" in body.lower():
                    upload_pdf_to_bucket(PO_BUCKET, pdf_content, customer_id)
                else:
                    print("No matching keywords found. Skipping PDF.")
            else:
                print("No PDF attachments found. Attempting text processing.")
                body = part.get_payload(decode=True).decode(part.get_content_charset() or "utf-8")
                process_text_content(body, customer_id)

    except Exception as e:
        error_handler.handle_processing_error(
            file_key=file_key,
            bucket_name=bucket_name,
            error=e,
            content=email_content if 'email_content' in locals() else None,
            additional_info={
                'customer_id': customer_id if 'customer_id' in locals() else 'unknown',
                'document_type': 'Email',
                'receiver_email': receiver_email if 'receiver_email' in locals() else None
            }
        )
        raise

def process_text_content(body, customer_id):
    """
    Processes the email body to extract and route PO data using fuzzy matching.
    """
    matched_data = fuzzy_match_keys(body, KEY_ALIASES)

    if not matched_data.get("po_number") or not matched_data.get("order_total") or not matched_data.get("order_date"):
        print("Required fields not found using fuzzy matching. Skipping.")
        return

    po_data = {
        "po_number": matched_data["po_number"],
        "order_total": float(matched_data["order_total"].replace(",", "")),
        "order_date": matched_data["order_date"],
        "customer_id": customer_id,
    }

    po_content = json.dumps(po_data)
    file_name = f"{customer_id}_{uuid.uuid4().hex}.json"
    s3_client.put_object(Bucket=PO_BUCKET, Key=file_name, Body=po_content)
    print(f"Uploaded PO data to {PO_BUCKET}/{file_name}")

def process_po(bucket_name, file_key):
    """
    Processes a PO file using Textract and Hugging Face for enhanced extraction.
    """
    try:
        start_time = datetime.now()  # Start timing
        
        # Extract customer ID from file key
        customer_id = file_key.split('_')[0]
        
        # Get the PDF content
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        pdf_content = response['Body'].read()
        
        # Use Textract to extract text
        textract_response = textract_client.detect_document_text(
            Document={'Bytes': pdf_content}
        )
        
        # Format Textract response
        extracted_text = ""
        for item in textract_response['Blocks']:
            if item['BlockType'] == 'LINE':
                extracted_text += item['Text'] + "\n"
        
        # Use Hugging Face to extract information
        extracted_data = extract_document_info(extracted_text)
        
        # Add raw text to extracted data
        extracted_data['raw_text'] = extracted_text
        
        # Format response to match expected structure
        formatted_data = {
            'result': [{
                'prediction': [
                    {'ocr_text': extracted_text,
                     'extracted_fields': extracted_data}
                ]
            }]
        }
        
        # Process and store the data
        po_data = process_po_to_storage(formatted_data, bucket_name, file_key)
        
        # Calculate processing time
        processing_time = (datetime.now() - start_time).total_seconds()
        
        # On success, notify
        error_handler.notify_success(
            file_key=file_key,
            document_type='PO',
            customer_id=customer_id,
            processing_details={
                'source_bucket': bucket_name,
                'document_number': po_data.get('po_number'),
                'processing_time': processing_time,
            }
        )
        
        return po_data

    except Exception as e:
        # Handle error
        error_handler.handle_processing_error(
            file_key=file_key,
            bucket_name=bucket_name,
            error=e,
            content=pdf_content,
            additional_info={
                'customer_id': customer_id,
                'document_type': 'PO'
            }
        )
        raise

def process_invoice(bucket_name, file_key):
    """
    Processes invoices and matches with POs in DynamoDB.
    """
    try:
        start_time = datetime.now()
        customer_id = file_key.split('_')[0]
        
        invoice_data = process_invoice_to_storage(bucket_name, file_key)
        po_number = invoice_data.get("po_number")

        if not po_number:
            raise ValueError("PO number missing from invoice")

        completed_item = COMPLETED_TABLE.get_item(Key={"po_number": po_number}).get("Item")
        if not completed_item:
            raise ValueError(f"PO {po_number} not found in completed items")

        processor = API_PROCESSORS.get(customer_id)
        if processor:
            response = processor(completed_item)
            
            # Calculate processing time
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # Notify success
            error_handler.notify_success(
                file_key=file_key,
                document_type='Invoice',
                customer_id=customer_id,
                processing_details={
                    'source_bucket': bucket_name,
                    'document_number': invoice_data.get('invoice_number'),
                    'po_number': po_number,
                    'processing_time': processing_time,
                }
            )
            
            return response
        else:
            raise ValueError(f"No API processor found for {customer_id}")
            
    except Exception as e:
        error_handler.handle_processing_error(
            file_key=file_key,
            bucket_name=bucket_name,
            error=e,
            additional_info={
                'customer_id': customer_id,
                'document_type': 'Invoice',
                'po_number': po_number if 'po_number' in locals() else None
            }
        )
        raise

def upload_pdf_to_bucket(target_bucket, pdf_content, customer_id):
    """
    Uploads a PDF file to the specified S3 bucket.
    """
    file_name = f"{customer_id}_{uuid.uuid4().hex}.pdf"
    s3_client.put_object(Bucket=target_bucket, Key=file_name, Body=pdf_content)
    print(f"Uploaded PDF to {target_bucket}/{file_name}")

def extract_receiver_email(to_header):
    """
    Extracts the receiver email address from the 'To' header.
    """
    return to_header.split('<')[-1].replace('>', '').strip()

def fuzzy_match_keys(body, key_aliases):
    """
    Matches keys in text using fuzzy matching.
    """
    matched_data = {}
    for key, aliases in key_aliases.items():
        for alias in aliases:
            matched_line = fuzzy_process.extractOne(alias, body.splitlines(), scorer=fuzz.token_sort_ratio)
            if matched_line and matched_line[1] >= FUZZY_MATCH_THRESHOLD:
                match_value = re.search(r"[\d,\.]+", matched_line[0])
                if match_value:
                    matched_data[key] = match_value.group().strip()
    return matched_data
