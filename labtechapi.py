import requests
import json
import re
from datetime import datetime, timedelta
from decimal import Decimal
from auth import get_secret, refresh_access_token

# Constants
DEFAULT_ITEM_REF = "1"  # Default ItemRef for "Services"
DEFAULT_DESCRIPTION = "Services"
DEFAULT_CUSTOMER_NAME = "Woodside"
RETRIES = 3
DELAY = 5

def convert_decimal(value):
    """
    Recursively converts all Decimal values in a data structure to float.
    """
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: convert_decimal(v) for k, v in value.items()}
    if isinstance(value, list):
        return [convert_decimal(v) for v in value]
    return value

def get_access_token():
    """
    Retrieves and refreshes the QuickBooks access token if necessary.
    """
    secrets = get_secret()
    access_token = secrets.get("access_token")
    if not access_token:
        print("Access token not found. Attempting to refresh...")
        access_token = refresh_access_token()
    return access_token

def get_customers_list(company_id):
    """
    Fetches a list of customers from QuickBooks.
    Handles token expiry and retries with a refreshed token.
    """
    api_url = f"https://sandbox-quickbooks.api.intuit.com/v3/company/{company_id}/query"
    query = "SELECT * FROM Customer"

    for attempt in range(RETRIES):
        access_token = get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        try:
            response = requests.get(f"{api_url}?query={query}", headers=headers)

            # Debug raw response
            print(f"Raw response from QuickBooks: {response.text}")

            if response.status_code == 200:
                return response.json().get("QueryResponse", {}).get("Customer", [])
            elif response.status_code == 401:
                print("Authentication failed. Refreshing token...")
                refresh_access_token()
            else:
                print(f"Error fetching customers: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            print(f"Error fetching customers from QuickBooks: {e}")
            return []

    print("All retries exhausted. Failed to fetch customers.")
    return []

def get_quickbooks_customer_id(customer_name, company_id):
    """
    Matches a customer name to its QuickBooks CustomerRef ID.
    Handles retries and logs if the customer is not found.
    """
    customers = get_customers_list(company_id)
    if not customers:
        print(f"No customers found in QuickBooks.")
        return None

    for customer in customers:
        if customer.get("DisplayName", "").lower() == customer_name.lower():
            return customer.get("Id")

    print(f"Customer '{customer_name}' not found in QuickBooks.")
    return None

def format_for_quickbooks(extracted_data, item_refs, customer_id, company_id):
    """
    Formats extracted data to match the QuickBooks API structure.
    Provides meaningful fallbacks for missing customer IDs.
    """
    if not isinstance(extracted_data, dict):
        print("Error: extracted_data is not a dictionary.")
        return None

    try:
        # Get customer ID
        if not customer_id:
            customer_id = get_quickbooks_customer_id(DEFAULT_CUSTOMER_NAME, company_id)
            if not customer_id:
                print(f"Error: Default customer '{DEFAULT_CUSTOMER_NAME}' not found. Aborting formatting.")
                return None

        # Prepare data fields
        invoice_number = re.sub(r"[^\d]", "", str(extracted_data.get("invoice_number", "Unknown Invoice")))
        total_amount = float(re.sub(r"[^\d.]", "", str(extracted_data.get("Total", 0))))
        date_str = extracted_data.get("Date", "")
        txn_date = datetime.now().strftime("%Y-%m-%d")

        # Parse provided date or fallback to current date
        if date_str:
            try:
                txn_date = datetime.strptime(date_str, "%B %d %Y").strftime("%Y-%m-%d")
            except ValueError:
                print(f"Invalid date format: {date_str}. Using current date as fallback.")

        due_date = (datetime.strptime(txn_date, "%Y-%m-%d") + timedelta(days=7)).strftime("%Y-%m-%d")

        # Create line items
        line_items = [{
            "Description": DEFAULT_DESCRIPTION,
            "Amount": total_amount,
            "DetailType": "SalesItemLineDetail",
            "SalesItemLineDetail": {
                "ItemRef": {
                    "value": item_refs.get("Services", DEFAULT_ITEM_REF),
                    "name": "Services"
                },
                "UnitPrice": total_amount,
                "Qty": 1
            }
        }]

        # Format the complete invoice data
        formatted_data = {
            "CustomerRef": {"value": str(customer_id)},
            "DocNumber": invoice_number,
            "TxnDate": txn_date,
            "Line": line_items,
            "TotalAmt": total_amount,
            "DueDate": due_date
        }

        return convert_decimal(formatted_data)

    except Exception as e:
        print(f"Error formatting data for QuickBooks: {e}")
        return None

def send_to_quickbooks(company_id, data):
    """
    Sends formatted invoice data to QuickBooks.
    Handles token expiry and retries with a refreshed token.
    """
    api_url = f"https://sandbox-quickbooks.api.intuit.com/v3/company/{company_id}/invoice"

    for attempt in range(RETRIES):
        access_token = get_access_token()
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

        try:
            response = requests.post(api_url, headers=headers, json=convert_decimal(data))

            if response.status_code in (200, 201):
                print("Data successfully sent to QuickBooks.")
                return {"statusCode": response.status_code, "body": response.json()}
            elif response.status_code == 401:
                print("Access token expired. Refreshing token...")
                refresh_access_token()
            else:
                print(f"Failed to send data: {response.status_code} - {response.text}")
                return {"statusCode": response.status_code, "body": response.text}
        except Exception as e:
            print(f"Error sending data: {e}")
            return {"statusCode": 500, "body": str(e)}

    print("All retries exhausted. Failed to send data.")
    return {"statusCode": 500, "body": "Failed to send data after retries."}

def process_with_labtechapi(invoice_data):
    """
    Main processing logic for LabTech API.
    """
    try:
        secrets = get_secret()
        company_id = secrets.get("company_id")
        if not company_id:
            return {"statusCode": 500, "body": "Missing company_id in secrets."}

        formatted_data = format_for_quickbooks(
            invoice_data, {"Services": DEFAULT_ITEM_REF}, invoice_data.get("Customer ID"), company_id
        )
        if not formatted_data:
            return {"statusCode": 500, "body": "Failed to format invoice data."}

        return send_to_quickbooks(company_id, formatted_data)
    except Exception as e:
        print(f"Unexpected error in LabTech API processing: {e}")
        return {"statusCode": 500, "body": str(e)}
