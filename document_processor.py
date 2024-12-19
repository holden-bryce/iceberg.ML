import json
from datetime import datetime, timedelta
import re
import requests
import random
import boto3
from transformers import LayoutLMv3Processor, LayoutLMv3ForSequenceClassification
import torch
from PIL import Image
import numpy as np

# Initialize the processor and model
processor = LayoutLMv3Processor.from_pretrained("microsoft/layoutlmv3-base")
model = LayoutLMv3ForSequenceClassification.from_pretrained("microsoft/layoutlmv3-base")

def process_image(image_path):
    try:
        # Load the image
        image = Image.open(image_path)
        
        # Check if the image is loaded correctly
        if image is None:
            print("Image is None. Check the image loading process.")
            return None
        
        # Convert image to a format expected by your model
        image_array = np.array(image)
        
        # Debug: Print the type and shape of the image
        print(f"Image type: {type(image_array)}, Image shape: {image_array.shape}")
        
        return image_array

    except Exception as e:
        print(f"Error loading image: {e}")
        return None

def extract_document_info(content):
    """Enhanced document information extraction using prompt engineering"""
    print("\n=== Starting Document Processing ===")
    
    if isinstance(content, dict):
        raw_text = content.get('raw_text', '')
        key_value_pairs = content.get('key_value_pairs', {})
        tables = content.get('tables', [])
        print(f"\nProcessing tables: {json.dumps(tables, indent=2)}")
    else:
        raw_text = str(content)
        key_value_pairs = {}
        tables = []

    # Extract line items
    line_items = []
    for table in tables:
        if len(table) > 1:
            headers = table[0]
            print(f"\nProcessing table with headers: {headers}")
            
            if is_line_item_table(headers):
                header_map = create_header_map(headers)
                print(f"Header mapping: {json.dumps(header_map, indent=2)}")

                for row in table[1:]:
                    if len(row) >= len(headers) and any(row) and not is_summary_row(row):
                        try:
                            item = extract_line_item(row, header_map, key_value_pairs)
                            if item and item["description"] and not item["description"].startswith('**'):
                                line_items.append(item)
                        except Exception as e:
                            print(f"Error processing row {row}: {str(e)}")

    structured_data = {
        "po_number": extract_field(key_value_pairs, [
            'PO Number', 'P.O.Number', 'Order Number'
        ]),
        "order_date": extract_field(key_value_pairs, [
            'Order Date', 'Invoice Date', 'Ordered'
        ]),
        "vendor_info": {
            "name": extract_field(key_value_pairs, [
                'Vendor Name', 'Ordered From'
            ]),
            "number": extract_field(key_value_pairs, [
                'Vendor #', 'Vendor Number', 'Payer Number'
            ]),
            "address": extract_field(key_value_pairs, [
                'Billing address', 'Ordered From'
            ]),
            "tax_id": extract_field(key_value_pairs, [
                'Tax ID', 'Federal ID number'
            ])
        },
        "shipping_info": {
            "address": extract_field(key_value_pairs, [
                'Ship To :', 'Ship To Address', 'Shipping address'
            ]),
            "method": extract_field(key_value_pairs, [
                'Ship Via', 'Ship Via -', 'Incoterms :'
            ])
        },
        "payment_terms": extract_field(key_value_pairs, [
            'Payment Terms', 'Terms'
        ]),
        "total_amount": extract_number(extract_field(key_value_pairs, [
            'Total', 'Invoice Total', 'Total Order excl. VAT', 'Amount'
        ])),
        "line_items": line_items
    }

    print(f"\nExtracted structured data: {json.dumps(structured_data, indent=2)}")
    return structured_data

def is_line_item_table(headers):
    headers_str = ' '.join(str(h).lower() for h in headers)
    return any([
        'description' in headers_str,
        'material' in headers_str,
        'product' in headers_str
    ])

def is_summary_row(row):
    row_str = ' '.join(str(cell).lower() for cell in row)
    return any(term in row_str for term in ['total', '**', 'subtotal'])

def create_header_map(headers):
    header_map = {}
    headers_str = [str(h).lower() for h in headers]
    
    for i, header in enumerate(headers_str):
        if any(term in header for term in ['desc', 'product', 'material']):
            header_map['description'] = i
        elif any(term in header for term in ['qty', 'quant', 'number of']):
            if 'billing' in header or i > 5:
                header_map['quantity'] = i
            elif 'quantity' not in header_map:
                header_map['quantity'] = i
        elif any(term in header for term in ['price', 'rate']):
            header_map['price'] = i
        elif any(term in header for term in ['uom', 'u m', 'packaging']):
            header_map['uom'] = i
        elif any(term in header for term in ['amount', 'total']):
            header_map['total'] = i
        elif any(term in header for term in ['date', 'delivery']):
            header_map['date'] = i

    print(f"Created header map: {json.dumps(header_map, indent=2)}")
    return header_map

def extract_line_item(row, header_map, key_value_pairs):
    try:
        description = row[header_map.get('description', 0)]
        
        quantity = (
            extract_number(row[header_map['quantity']]) if 'quantity' in header_map
            else extract_number(key_value_pairs.get('Total Quantity', ''))
        )
        
        price_str = row[header_map.get('price', -2)] if 'price' in header_map else ''
        unit_price = extract_number(price_str)
        
        total_str = row[header_map.get('total', -1)] if 'total' in header_map else ''
        total_price = extract_number(total_str)

        return {
            "description": description,
            "quantity": quantity,
            "unit_price": unit_price,
            "total_price": total_price,
            "unit_of_measure": row[header_map.get('uom', 2)] if 'uom' in header_map else "",
            "delivery_date": row[header_map.get('date', 3)] if 'date' in header_map else ""
        }
    except (ValueError, IndexError) as e:
        print(f"Error in extract_line_item: {str(e)}")
        return None

def extract_field(data, possible_keys):
    for key in possible_keys:
        value = data.get(key, '')
        if value and str(value).strip():
            return str(value).replace('USD', '').strip()
    return ''

def extract_number(value):
    if not value:
        return 0
    try:
        if '/' in str(value):
            value = value.split('/')[0]
        
        clean_value = re.sub(r'[^\d.,]', '', str(value))
        return float(clean_value.replace(',', ''))
    except:
        return 0

def extract_with_ml(text_content, image_content=None):
    try:
        if isinstance(image_content, bytes):
            image_content = process_image(image_content)

        if image_content is None:
            print("Image content is None. Exiting ML extraction.")
            return None

        encoding = processor(
            images=image_content,
            text=text_content,
            return_tensors="pt",
            padding="max_length",
            truncation=True
        )

        with torch.no_grad():
            outputs = model(**encoding)

        structured_data = {
            "po_number": extract_field(outputs, text_content, "po_number"),
            "order_date": extract_field(outputs, text_content, "date"),
            "vendor_info": {
                "name": extract_field(outputs, text_content, "vendor_name"),
                "number": extract_field(outputs, text_content, "vendor_number"),
                "address": extract_field(outputs, text_content, "vendor_address"),
                "tax_id": extract_field(outputs, text_content, "tax_id")
            },
            "shipping_info": {
                "address": extract_field(outputs, text_content, "shipping_address"),
                "method": extract_field(outputs, text_content, "shipping_method")
            },
            "payment_terms": extract_field(outputs, text_content, "payment_terms"),
            "total_amount": extract_amount(outputs, text_content),
            "line_items": extract_line_items(outputs, text_content)
        }

        return structured_data

    except Exception as e:
        print(f"ML extraction failed: {str(e)}")
        return None

def convert_pdf_to_image(pdf_content):
    try:
        image = Image.open(pdf_content)
        return image
    except Exception as e:
        print(f"Error converting PDF to image: {str(e)}")
        return None