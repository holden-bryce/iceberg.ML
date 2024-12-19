import boto3
import json
import uuid
from email import policy
from email.parser import BytesParser
from decimal import Decimal
from datetime import datetime
import re
import torchvision
torchvision.disable_beta_transforms_warning()

# Import dependencies for processing
from po_to_storage import process_po_to_storage
from invoice_to_storage import process_invoice_to_storage
from labtechapi import process_with_labtechapi
from brycebizapi import process_with_brycebiz
from clean import clean_extracted_data
from holden_po_processor import process_holden_po_email
from document_processor import extract_document_info

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

# Receiver email to customer ID mapping
RECEIVER_TO_CUSTOMER_ID = {
    "labtech@flowerwork.co": "444",
    "holden@flowerwork.co": "69420",
}

# API processors for each internal customer ID
API_PROCESSORS = {
    "444": process_with_labtechapi,
    "69420": process_with_brycebiz,
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
        email_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        email_content = email_obj['Body'].read()
        msg = BytesParser(policy=policy.default).parsebytes(email_content)
        print("Email parsed successfully.")

        receiver_email = extract_receiver_email(msg['to'])
        customer_id = RECEIVER_TO_CUSTOMER_ID.get(receiver_email, "unknown")
        
        if customer_id == "69420":  # Holden's customer ID
            result = process_holden_po_email(email_content)
            print(f"Holden PO processing result: {result}")
            return result

        if customer_id == "unknown":
            print(f"Receiver email {receiver_email} not recognized. Skipping.")
            return

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
                extracted_data = extract_document_info(body)
                process_text_content(extracted_data, customer_id)

    except Exception as e:
        print(f"Error processing email: {e}")

def process_po(bucket_name, file_key):
    try:
        print(f"\n=== Starting PO Processing ===")
        print(f"Bucket: {bucket_name}")
        print(f"File Key: {file_key}")
        
        # Get the PDF content
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        pdf_content = response['Body'].read()
        
        # Check if the PDF content is loaded correctly
        if not pdf_content:
            print("PDF content is None. Check the file in S3.")
            return
        
        # Use Textract to extract text and analyze document
        textract_response = textract_client.analyze_document(
            Document={'Bytes': pdf_content},
            FeatureTypes=['TABLES', 'FORMS']
        )
        
        # Extract text and process with Hugging Face
        extracted_text = ""
        for item in textract_response['Blocks']:
            if item['BlockType'] == 'LINE':
                extracted_text += item['Text'] + "\n"
        
        # Process with Hugging Face
        extracted_data = extract_document_info(extracted_text)
        
        # Extract tables
        tables = []
        for block in textract_response['Blocks']:
            if block['BlockType'] == 'TABLE':
                table_data = extract_table_data(block, textract_response['Blocks'])
                tables.append(table_data)
        
        # Create final PO data structure
        po_data = {
            "order_details": extracted_data,
            "tables": tables,
            "metadata": {
                "source_file": file_key,
                "creation_timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "processing_status": "Processed"
            }
        }
        
        # Save to S3
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        json_filename = f"69420_{timestamp}_po.json"
        
        s3_client.put_object(
            Bucket='jsonfiles4holden',
            Key=json_filename,
            Body=json.dumps(po_data, default=str, indent=2),
            ContentType='application/json'
        )
        
        print(f"Saved PO JSON to S3: jsonfiles4holden/{json_filename}")
        
        return po_data
        
    except Exception as e:
        print(f"Error processing PO: {str(e)}")
        raise
def process_invoice(bucket_name, file_key):
    """
    Processes invoices and matches with POs in DynamoDB.
    """
    try:
        # Extract text from invoice using Textract
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        pdf_content = response['Body'].read()
        
        textract_response = textract_client.detect_document_text(
            Document={'Bytes': pdf_content}
        )
        
        extracted_text = ""
        for item in textract_response['Blocks']:
            if item['BlockType'] == 'LINE':
                extracted_text += item['Text'] + "\n"
        
        # Use Hugging Face API to extract invoice information
        extracted_data = extract_document_info(extracted_text)
        
        # Process invoice with existing logic
        invoice_data = process_invoice_to_storage(bucket_name, file_key, extracted_data)
        po_number = invoice_data.get("po_number")

        if not po_number:
            print("PO number missing. Skipping.")
            return

        completed_item = COMPLETED_TABLE.get_item(Key={"po_number": po_number}).get("Item")
        if not completed_item:
            print(f"PO {po_number} not found in completed items. Skipping.")
            return

        internal_customer_id = file_key.split("_")[0]
        processor = API_PROCESSORS.get(internal_customer_id)

        if processor:
            response = processor(completed_item)
            print(f"Processed with API: {response}")
        else:
            print(f"No API processor found for {internal_customer_id}.")
    except Exception as e:
        print(f"Error processing invoice: {e}")

def process_text_content(extracted_data, customer_id):
    """
    Processes text content using the enhanced document processing.
    """
    if not extracted_data.get("po_number") or not extracted_data.get("Total") or not extracted_data.get("Date"):
        print("Required fields not found in extracted data. Skipping.")
        return

    po_data = {
        "po_number": extracted_data["po_number"],
        "order_total": float(str(extracted_data["Total"]).replace(",", "")),
        "order_date": extracted_data["Date"],
        "customer_id": customer_id,
        "vendor_name": extracted_data.get("vendor_name", ""),
        "shipping_address": extracted_data.get("shipping_address", "")
    }

    po_content = json.dumps(po_data)
    file_name = f"{customer_id}_{uuid.uuid4().hex}.json"
    s3_client.put_object(Bucket=PO_BUCKET, Key=file_name, Body=po_content)
    print(f"Uploaded enhanced PO data to {PO_BUCKET}/{file_name}")

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