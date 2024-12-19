import json
from datetime import datetime
import boto3
import re
from email import policy
from email.parser import BytesParser
from document_processor import extract_document_info

# Missing textract_client initialization
  # Add this with other clients
# Initialize AWS clients
s3_client = boto3.client('s3')
textract = boto3.client('textract')
textract_client = boto3.client('textract')  # Add this line

def process_holden_po_email(email_content):
    """Process PO emails and create structured JSON"""
    try:
        print("Starting to process PO email")
        
        msg = BytesParser(policy=policy.default).parsebytes(email_content)
        
        for part in msg.iter_attachments():
            if part.get_content_type() == 'application/pdf':
                pdf_content = part.get_payload(decode=True)
                
                print("Calling Textract for complete document analysis...")
                textract_response = textract_client.analyze_document(
                    Document={'Bytes': pdf_content},
                    FeatureTypes=['TABLES', 'FORMS']
                )
                
                # Extract all content types
                extracted_content = {
                    'raw_text': "",
                    'tables': [],
                    'key_value_pairs': {}
                }
                
                # Process each block type
                for block in textract_response['Blocks']:
                    if block['BlockType'] == 'LINE':
                        extracted_content['raw_text'] += block['Text'] + "\n"
                    elif block['BlockType'] == 'TABLE':
                        table_data = extract_table_data(block, textract_response['Blocks'])
                        extracted_content['tables'].append(table_data)
                    elif block['BlockType'] == 'KEY_VALUE_SET':
                        if 'KEY' in block.get('EntityTypes', []):
                            key_text = get_text_from_relationships(block, textract_response['Blocks'])
                            value_block = get_value_block(block, textract_response['Blocks'])
                            if value_block:
                                value_text = get_text_from_relationships(value_block, textract_response['Blocks'])
                                extracted_content['key_value_pairs'][key_text] = value_text

                # Combine all extracted content for Hugging Face
                complete_text = (
                    f"Raw Text:\n{extracted_content['raw_text']}\n\n"
                    f"Key-Value Pairs:\n" + 
                    "\n".join([f"{k}: {v}" for k, v in extracted_content['key_value_pairs'].items()]) +
                    f"\n\nTables:\n" + 
                    "\n".join([str(table) for table in extracted_content['tables']])
                )
                
                print("\nSending complete extracted content to Hugging Face...")
                structured_data = extract_document_info(complete_text)
                
                # Create final PO data structure
                po_data = {
                    "order_details": {
                        "po_number": structured_data.get("po_number"),
                        "order_date": structured_data.get("order_date"),
                        "vendor_info": structured_data.get("vendor_info", {}),
                        "shipping_info": structured_data.get("shipping_info", {}),
                        "payment_terms": structured_data.get("payment_terms"),
                        "total_amount": structured_data.get("total_amount")
                    },
                    "line_items": structured_data.get("line_items", []),
                    "metadata": {
                        "source_email_subject": msg.get('Subject', ''),
                        "creation_timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "processing_status": "Processed"
                    },
                    "raw_extraction": {
                        "textract_content": extracted_content,
                        "structured_data": structured_data
                    }
                }
                
                # Save to S3
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                file_name = f"69420_{timestamp}_po.json"
                json_content = json.dumps(po_data, default=str, indent=2)
                
                print("\n=== Final JSON Content ===")
                print(json_content)
                
                s3_client.put_object(
                    Bucket='jsonfiles4holden',
                    Key=file_name,
                    Body=json_content,
                    ContentType='application/json'
                )
                
                return {"statusCode": 200, "body": "Event processed successfully"}
                
        return {"statusCode": 400, "body": "No PDF attachment found"}
        
    except Exception as e:
        print(f"Error processing email: {str(e)}")
        return {"statusCode": 500, "body": str(e)}

def extract_text_from_attachments(msg):
    """Extract text from email attachments using Textract"""
    for part in msg.walk():
        if part.get_content_type() in ['application/pdf', 'image/jpeg', 'image/png']:
            attachment = part.get_payload(decode=True)
            temp_key = f"temp/{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            # Save attachment temporarily
            s3_client.put_object(
                Bucket='icebergrawmail',
                Key=temp_key,
                Body=attachment
            )
            
            # Extract text using Textract
            response = textract.detect_document_text(
                Document={
                    'S3Object': {
                        'Bucket': 'icebergrawmail',
                        'Name': temp_key
                    }
                }
            )
            
            # Combine all text blocks
            return ' '.join([item['Text'] for item in response['Blocks'] if item['BlockType'] == 'LINE'])
    
    return ""

def clean_subject(subject):
    """Clean email subject"""
    return re.sub(r'(?:DKIM|dkim|bh).*$', '', subject).strip()

# Initialize AWS clients
s3_client = boto3.client('s3')
textract = boto3.client('textract')
def get_text_from_relationships(block, blocks):
    """Extract text from related word blocks"""
    text = ''
    if 'Relationships' in block:
        for relationship in block['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    child_block = next((b for b in blocks if b['Id'] == child_id), None)
                    if child_block and child_block['BlockType'] == 'WORD':
                        text += child_block['Text'] + ' '
    return text.strip()

def get_value_block(key_block, blocks):
    """Get the value block associated with a key block"""
    if 'Relationships' in key_block:
        for relationship in key_block['Relationships']:
            if relationship['Type'] == 'VALUE':
                for value_id in relationship['Ids']:
                    value_block = next((b for b in blocks if b['Id'] == value_id), None)
                    if value_block:
                        return value_block
    return None

def extract_table_data(table_block, blocks):
    """Extract data from a table block"""
    table_cells = {}
    max_row = 0
    max_col = 0
    
    for relationship in table_block.get('Relationships', []):
        if relationship['Type'] == 'CHILD':
            for cell_id in relationship['Ids']:
                cell_block = next((b for b in blocks if b['Id'] == cell_id), None)
                if cell_block and cell_block['BlockType'] == 'CELL':
                    row_index = cell_block['RowIndex']
                    col_index = cell_block['ColumnIndex']
                    max_row = max(max_row, row_index)
                    max_col = max(max_col, col_index)
                    cell_text = get_text_from_relationships(cell_block, blocks)
                    table_cells[(row_index, col_index)] = cell_text
    
    table_data = []
    for row in range(1, max_row + 1):
        row_data = []
        for col in range(1, max_col + 1):
            row_data.append(table_cells.get((row, col), ""))
        table_data.append(row_data)
    
    return table_data