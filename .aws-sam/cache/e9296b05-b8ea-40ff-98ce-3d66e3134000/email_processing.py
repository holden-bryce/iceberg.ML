import boto3
import uuid
from fpdf import FPDF
from io import BytesIO
import re
from datetime import datetime
# Initialize S3 client
s3_client = boto3.client('s3')
PO_BUCKET = "icebergpos"
INVOICE_BUCKET = "iceberginvoices"
def is_invoice_pdf(raw_text):
    """
    Determines if the raw text content likely corresponds to an invoice.
    """
    keywords = ["invoice", "bill to", "amount due", "payment"]
    raw_text_lower = raw_text.lower()
    for keyword in keywords:
        if keyword in raw_text_lower:
            return True
    return False
def extract_attachment_and_upload(raw_email, bucket_name, key_prefix):
    """
    Extracts attachments from a raw email and uploads them to an S3 bucket.
    """
    msg = BytesParser(policy=policy.default).parsebytes(raw_email)
    for part in msg.iter_attachments():
        filename = part.get_filename()
        if not filename:
            filename = f"attachment_{uuid.uuid4().hex}.dat"
        attachment_content = part.get_payload(decode=True)
        s3_key = f"{key_prefix}/{filename}"
        try:
            s3_client.upload_fileobj(BytesIO(attachment_content), bucket_name, s3_key)
            print(f"Uploaded attachment {filename} to s3://{bucket_name}/{s3_key}")
        except Exception as e:
            print(f"Error uploading attachment {filename}: {e}")
def process_email_or_pdf(bucket_name, file_key, customer_id):
    """
    Handles files in the raw email bucket. Converts content to PDFs and routes them.
    """
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read()
        try:
            msg = BytesParser(policy=policy.default).parsebytes(file_content)
            has_attachment = False
            for part in msg.iter_attachments():
                if part.get_content_type() == 'application/pdf':
                    has_attachment = True
                    pdf_content = part.get_payload(decode=True)
                    if is_invoice_pdf(pdf_content.decode('utf-8', errors='ignore')):
                        upload_pdf_to_bucket(INVOICE_BUCKET, pdf_content, customer_id)
                    else:
                        upload_pdf_to_bucket(PO_BUCKET, pdf_content, customer_id)
                    return
            if not has_attachment:
                generate_pdf_from_raw_content(
                    raw_text=file_content.decode('utf-8', errors='ignore'),
                    bucket_name=PO_BUCKET,
                    customer_id=customer_id
                )
        except Exception as e:
            if file_content and file_content.strip():
                raw_text = file_content.decode('utf-8', errors='ignore')
                generate_pdf_from_raw_content(
                    raw_text=raw_text,
                    bucket_name=PO_BUCKET,
                    customer_id=customer_id
                )
    except Exception as e:
        print(f"Error processing file: {e}")
        raise
def generate_pdf_from_raw_content(raw_text, bucket_name, customer_id):
    """
    Generates a PDF from raw email content while maintaining all data extraction.
    """
    try:
        # Extract sections using existing patterns (maintain this part)
        patterns = {
            "Purchase Order Details": r"(?i)(Purchase Order Details.*?)(?=Shipping Details|Summary|LABTECH CORPORATION)",
            "Shipping Details": r"(?i)(Shipping Details.*?)(?=Summary|LABTECH CORPORATION)",
            "Summary": r"(?i)(Summary.*?)(?=LABTECH CORPORATION)",
        }
        
        sections = {}
        for section, pattern in patterns.items():
            match = re.search(pattern, raw_text, re.DOTALL)
            if match:
                sections[section] = match.group(1).strip()

        # Extract key information
        po_number = "Unknown"
        if "Summary" in sections:
            po_match = re.search(r"PO #\s*(\d+)", sections["Summary"])
            if po_match:
                po_number = po_match.group(1)

        # Create PDF with improved formatting
        pdf = FPDF()
        pdf.add_page()
        pdf.set_auto_page_break(auto=True, margin=15)
        
        # Header
        pdf.set_font('Helvetica', 'B', 16)
        pdf.cell(0, 10, 'PURCHASE ORDER', 0, 1, 'C')
        pdf.set_font('Helvetica', '', 12)
        pdf.cell(0, 10, f'PO Number: {po_number}', 0, 1, 'C')
        pdf.line(10, pdf.get_y(), 200, pdf.get_y())
        
        # Company Information
        pdf.set_font('Helvetica', 'B', 12)
        pdf.cell(0, 10, 'LABTECH CORPORATION', 0, 1, 'L')
        pdf.set_font('Helvetica', '', 10)
        pdf.cell(0, 5, '7707 Lyndon Street', 0, 1, 'L')
        pdf.cell(0, 5, 'Detroit, MI 48238', 0, 1, 'L')
        pdf.cell(0, 5, '313-862-1737', 0, 1, 'L')
        pdf.ln(5)

        # Order Details Section
        if "Purchase Order Details" in sections:
            pdf.set_font('Helvetica', 'B', 12)
            pdf.cell(0, 10, 'ORDER DETAILS', 0, 1, 'L')
            pdf.line(10, pdf.get_y(), 200, pdf.get_y())
            pdf.ln(2)
            
            # Create table headers
            pdf.set_font('Helvetica', 'B', 10)
            pdf.cell(20, 7, 'Qty', 1)
            pdf.cell(90, 7, 'Item', 1)
            pdf.cell(40, 7, 'Part #', 1)
            pdf.cell(40, 7, 'Price', 1)
            pdf.ln()
            
            # Parse and add items
            pdf.set_font('Helvetica', '', 10)
            items_text = sections["Purchase Order Details"]
            for line in items_text.split('\n'):
                if any(x in line.lower() for x in ['qty', 'item', 'part', 'price']):
                    continue
                if line.strip():
                    parts = line.split()
                    if len(parts) >= 2:
                        qty = parts[0] if parts[0] else ''
                        price = parts[-1] if '$' in parts[-1] else ''
                        part = parts[-2] if len(parts) > 2 else ''
                        item = ' '.join(parts[1:-2]) if len(parts) > 3 else ' '.join(parts[1:])
                        
                        pdf.cell(20, 7, qty, 1)
                        pdf.cell(90, 7, item, 1)
                        pdf.cell(40, 7, part, 1)
                        pdf.cell(40, 7, price, 1)
                        pdf.ln()

        # Shipping Details
        if "Shipping Details" in sections:
            pdf.ln(5)
            pdf.set_font('Helvetica', 'B', 12)
            pdf.cell(0, 10, 'SHIPPING DETAILS', 0, 1, 'L')
            pdf.line(10, pdf.get_y(), 200, pdf.get_y())
            pdf.ln(2)
            
            shipping_text = sections["Shipping Details"]
            pdf.set_font('Helvetica', '', 10)
            for line in shipping_text.split('\n'):
                if line.strip():
                    pdf.cell(0, 7, line.strip(), 0, 1)

        # Summary Section
        if "Summary" in sections:
            pdf.ln(5)
            pdf.set_font('Helvetica', 'B', 12)
            pdf.cell(0, 10, 'SUMMARY', 0, 1, 'L')
            pdf.line(10, pdf.get_y(), 200, pdf.get_y())
            pdf.ln(2)
            
            summary_text = sections["Summary"]
            pdf.set_font('Helvetica', '', 10)
            for line in summary_text.split('\n'):
                if 'Order Total' in line:
                    pdf.set_font('Helvetica', 'B', 10)
                if line.strip():
                    pdf.cell(0, 7, line.strip(), 0, 1)

        # Generate filename and save
        filename = f"{customer_id}_{po_number}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        pdf_content = pdf.output(dest='S').encode('latin-1')
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=pdf_content,
            ContentType='application/pdf'
        )
        
        print(f"Generated and uploaded PDF: {filename}")
        return filename

    except Exception as e:
        print(f"Error generating PDF: {e}")
        raise
def upload_pdf_to_bucket(target_bucket, pdf_content, customer_id):
    """
    Uploads a PDF to the specified bucket with metadata to ensure it downloads when accessed.
    """
    file_name = f"{customer_id}_{uuid.uuid4().hex}.pdf"
    try:
        print(f"Uploading PDF to bucket: {target_bucket}, file: {file_name}")
        # Upload the PDF with Content-Disposition set to attachment
        s3_client.put_object(
            Bucket=target_bucket,
            Key=file_name,
            Body=pdf_content,
            ContentType="application/pdf",
            Metadata={"customer_id": customer_id},
            ContentDisposition=f"attachment; filename={file_name}"  # Forces download
        )
        print(f"Successfully uploaded PDF to {target_bucket}/{file_name}")
        print(f"File will always download when accessed.")
    except Exception as e:
        print(f"Error uploading PDF to bucket: {e}")
        raise