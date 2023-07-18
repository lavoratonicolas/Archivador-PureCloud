from dotenv import load_dotenv
import base64
import requests
import os


def get_token():

    load_dotenv()
    CLIENT_ID = os.environ['GENESYS_CLOUD_CLIENT_ID']
    CLIENT_SECRET = os.environ['GENESYS_CLOUD_CLIENT_SECRET']
    ENVIRONMENT = os.environ['GENESYS_CLOUD_ENVIRONMENT']

    authorization = base64.b64encode(
        bytes(CLIENT_ID + ":" + CLIENT_SECRET, "ISO-8859-1")).decode("ascii")

    request_headers = {
        "Authorization": f"Basic {authorization}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    request_body = {
        "grant_type": "client_credentials"
    }

    response = requests.post(
        f"https://login.{ENVIRONMENT}/oauth/token", data=request_body, headers=request_headers)

    if response.status_code == 200:

        response_json = response.json()
        return response_json['access_token']
    else:
        print(f"Failure: { str(response.status_code) } - { response.reason }")
        return None
