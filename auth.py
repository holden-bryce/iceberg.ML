import requests
import json
import boto3
from botocore.exceptions import ClientError

# Specify your Secrets Manager secret name
SECRET_NAME = "quickbooks/iceberg/creds/api"

# Initialize AWS Secrets Manager client
secrets_client = boto3.client('secretsmanager')

def get_secret(secret_name=SECRET_NAME):
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret_data = json.loads(response['SecretString'])
        print("Secret retrieved successfully:", secret_data)  # Debugging line
        return secret_data
    except ClientError as e:
        print(f"Error retrieving secrets: {e}")
        return None

def update_secret(secret_name, access_token, refresh_token):
    """
    Updates the access token and refresh token in AWS Secrets Manager,
    retaining other existing fields like client_id and client_secret.
    :param secret_name: Name of the secret in Secrets Manager.
    :param access_token: New access token to update.
    :param refresh_token: New refresh token to update.
    """
    try:
        # Retrieve the existing secret to retain other fields
        response = secrets_client.get_secret_value(SecretId=secret_name)
        current_secret = json.loads(response['SecretString'])
        
        # Update only the access and refresh tokens
        current_secret.update({
            "access_token": access_token,
            "refresh_token": refresh_token
        })

        # Store the updated secret back in Secrets Manager
        secrets_client.update_secret(
            SecretId=secret_name,
            SecretString=json.dumps(current_secret)
        )
        print("Secrets updated successfully.")
    except ClientError as e:
        print(f"Error updating secrets: {e}")

def refresh_access_token():
    """
    Refreshes the access token using the refresh token stored in Secrets Manager.
    :return: New access token if successful, None otherwise.
    """
    secrets = get_secret()
    if not secrets:
        print("Failed to retrieve secrets.")
        return None

    # Retrieve necessary fields for token refresh
    refresh_token = secrets.get("refresh_token")
    client_id = secrets.get("client_id")
    client_secret = secrets.get("client_secret")
    company_id = secrets.get("company_id")

    print(f"Attempting token refresh with client_id: {client_id}")

    url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret
    }

    # Attempt the token refresh request
    response = requests.post(url, headers=headers, data=payload)
    if response.status_code == 200:
        tokens = response.json()
        new_access_token = tokens['access_token']
        new_refresh_token = tokens['refresh_token']
        
        # Update Secrets Manager with the new tokens
        update_secret(SECRET_NAME, new_access_token, new_refresh_token)

        # Test authentication with the new token
        if test_quickbooks_connection(company_id, new_access_token):
            return new_access_token
        else:
            print("New access token is invalid.")
            return None
    else:
        print(f"Error refreshing token: {response.text}")
        return None

def test_quickbooks_connection(company_id, access_token):
    """
    Tests if the new access token is valid by making a request to QuickBooks API.
    :param company_id: The QuickBooks company ID.
    :param access_token: The access token to test.
    :return: True if the token is valid, False otherwise.
    """
    api_url = f"https://sandbox-quickbooks.api.intuit.com/v3/company/{company_id}/query"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    query = "SELECT * FROM CompanyInfo"
    response = requests.get(f"{api_url}?query={query}", headers=headers)

    if response.status_code == 200:
        print("Authentication succeeded with new token.")
        return True
    elif response.status_code == 401:
        print("Authentication failed with new token.")
        return False
    else:
        print("Unexpected error:", response.text)
        return False
# New function to get authentication token and company ID

def get_auth_token():
    secrets = get_secret()
    if secrets:
        access_token = secrets.get("access_token")
        company_id = secrets.get("company_id")
        if access_token and company_id and test_quickbooks_connection(company_id, access_token):
            return {"access_token": access_token, "company_id": company_id}
        else:
            print("Access token expired or invalid. Refreshing token...")
            return refresh_access_token()
    else:
        print("Error: Could not retrieve secrets from Secrets Manager.")
        return None