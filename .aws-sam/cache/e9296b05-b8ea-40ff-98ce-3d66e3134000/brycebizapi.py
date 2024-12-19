import requests
import json
from datetime import datetime, timedelta
from decimal import Decimal

# BryceBiz API Configuration
ACCESS_TOKEN = "eyJlbmMiOiJBMTI4Q0JDLUhTMjU2IiwiYWxnIjoiZGlyIn0..1vXgN_odirWHIOUJeDAQ2A.3sjNryx-udxZysPI7gbu_Zn0FGZOYTYiL7sbPAHgH5oT75-vgqRW8j8wQbie9Ih_nyshY8k_uNNc-8svvd4XkiLpGugheN-YHEDEHEtuktohH-Xx8j9EpuJqJsdgb-1n_7yPo8nEQKbeq5jt4YCWrDHinDedulb_V0SLVyhycmH1flZFUWhLi15-hNZUgqMGphDRpX6sOSqUs4qpCaCombWuTDGKBqQowfMukc5E2slk3NyFEicCA5LmEo02nc5DfFDpuFNav25jlMpsBJCcA8ZAF0Rvwse64K_awHWEd_YreLjYHcuokBxR7lB02u6tmGkedA6RUexNPnnWVXLcOekZkcd32fXXaTgPf9ZtR_Ou-Sjh3AviwHLDD6aAczex4fbxNO5dTo2NaZqvprFUNrhhwoROEWvPI0Su3CrsMK8CmPMxH7HrSUzvnSUynawW1Nh2a0YEsYzM5G8btq33ddI-eVS0vz3SU9WhK62FesoU71Hd9Z2d3Hz4eS1x88cSJ-ehiR3ZVjuizTRZbY4vwtjgukd3xsYXTZadPdmZfHO5-pqi0lBGvwbOL_eydtUVPz3y-xC5Iy8kdHg4nhaKARTkKLHRZ1pqwZ6LPKqAttsvGohKLeUXAxigto0QZI3SiZgDdIkil_aZD4228aptUNtbDLDRKzdwNrt1qIh20psVZFrdF4EJ8aMOuzxpO1z3H8nM4CdFM90yu8kD2VNJtMwoaSKNpQ2srf1jlh2BYd8Xg-WeZsOjkhcH3pm-2cO5.9wYWvSRStzYwOR5Blkc0Pg"
REFRESH_TOKEN = "AB11741295018MTIOfKIPTraW5izh3ICGrDGGntaMu3Q9lTLTJ"
CLIENT_ID = "ABxlj8s58UFeneE90s04cR5vNAIn2uxgP0biQVUKqgRiTUTzxN"
CLIENT_SECRET = "cN2xUb7XSOEnfv8s4QtHCU6feXcQ9IC9he3C60yE"
COMPANY_ID = "9341453438206747"
DEFAULT_CUSTOMER_NAME = "BryceBiz MONEYYY"
DEFAULT_ITEM_REF = "1"  # Update as needed
DEFAULT_DESCRIPTION = "Services"
RETRIES = 3
DELAY = 5

# Utility Functions
def convert_decimal(value):
    """
    Recursively converts Decimal values to float for JSON serialization.
    """
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: convert_decimal(v) for k, v in value.items()}
    if isinstance(value, list):
        return [convert_decimal(v) for v in value]
    return value

# Token Management
def refresh_access_token():
    """
    Refreshes the BryceBiz QuickBooks access token.
    """
    global ACCESS_TOKEN
    url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }

    try:
        response = requests.post(url, headers=headers, data=payload)
        if response.status_code == 200:
            tokens = response.json()
            ACCESS_TOKEN = tokens.get("access_token")
            print("Access token refreshed successfully.")
            return ACCESS_TOKEN
        else:
            print(f"Failed to refresh token: {response.text}")
            return None
    except Exception as e:
        print(f"Error refreshing access token: {e}")
        return None

def test_quickbooks_connection():
    """
    Tests if the current access token is valid by querying the CompanyInfo endpoint.
    """
    url = f"https://sandbox-quickbooks.api.intuit.com/v3/company/{COMPANY_ID}/query"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    query = "SELECT * FROM CompanyInfo"

    try:
        response = requests.get(f"{url}?query={query}", headers=headers)
        if response.status_code == 200:
            print("Access token is valid.")
            return True
        elif response.status_code == 401:
            print("Access token is invalid or expired. Attempting to refresh...")
            return False
        else:
            print(f"Unexpected error: {response.text}")
            return False
    except Exception as e:
        print(f"Error testing QuickBooks connection: {e}")
        return False

# API Logic
def get_customers_list():
    """
    Fetches the list of customers from QuickBooks.
    """
    global ACCESS_TOKEN
    if not test_quickbooks_connection():
        ACCESS_TOKEN = refresh_access_token()

    api_url = f"https://sandbox-quickbooks.api.intuit.com/v3/company/{COMPANY_ID}/query"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    query = "SELECT * FROM Customer"

    try:
        response = requests.get(f"{api_url}?query={query}", headers=headers)
        if response.status_code == 200:
            customers = response.json().get("QueryResponse", {}).get("Customer", [])
            return customers
        else:
            print(f"Failed to fetch customers: {response.text}")
            return []
    except Exception as e:
        print(f"Error fetching customers: {e}")
        return []

def get_customer_id(customer_name):
    """
    Retrieves the customer ID for a given customer name.
    """
    customers = get_customers_list()
    for customer in customers:
        if customer.get("DisplayName", "").lower() == customer_name.lower():
            return customer.get("Id")
    print(f"Customer '{customer_name}' not found.")
    return None

def format_invoice_data(invoice_data):
    """
    Formats invoice data for QuickBooks API.
    """
    customer_id = get_customer_id(DEFAULT_CUSTOMER_NAME)
    if not customer_id:
        print(f"Error: Default customer '{DEFAULT_CUSTOMER_NAME}' not found.")
        return None

    total_amount = float(invoice_data.get("Total", 0))
    txn_date = datetime.now().strftime("%Y-%m-%d")
    due_date = (datetime.strptime(txn_date, "%Y-%m-%d") + timedelta(days=7)).strftime("%Y-%m-%d")

    formatted_data = {
        "CustomerRef": {"value": customer_id},
        "DocNumber": invoice_data.get("invoice_number", "Unknown Invoice"),
        "TxnDate": txn_date,
        "DueDate": due_date,
        "TotalAmt": total_amount,
        "Line": [
            {
                "DetailType": "SalesItemLineDetail",
                "Amount": total_amount,
                "SalesItemLineDetail": {
                    "ItemRef": {"value": DEFAULT_ITEM_REF},
                    "Qty": 1,
                    "UnitPrice": total_amount
                }
            }
        ]
    }
    return formatted_data

def process_with_brycebiz(invoice_data):
    """
    Sends an invoice to QuickBooks.
    """
    formatted_data = format_invoice_data(invoice_data)
    if not formatted_data:
        return {"status": "failure", "message": "Failed to format invoice data"}

    global ACCESS_TOKEN
    if not test_quickbooks_connection():
        ACCESS_TOKEN = refresh_access_token()

    api_url = f"https://sandbox-quickbooks.api.intuit.com/v3/company/{COMPANY_ID}/invoice"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(api_url, headers=headers, json=convert_decimal(formatted_data))
        if response.status_code in (200, 201):
            print("Invoice successfully sent.")
            return {"status": "success", "response": response.json()}
        else:
            print(f"Failed to send invoice: {response.text}")
            return {"status": "failure", "response": response.text}
    except Exception as e:
        print(f"Error sending invoice: {e}")
        return {"status": "failure", "message": str(e)}
