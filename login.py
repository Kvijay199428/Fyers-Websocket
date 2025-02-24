import json
import requests
import pyotp
from urllib import parse
import sys
from fyers_apiv3 import fyersModel
from dotenv import load_dotenv
import time as tm
from urllib.parse import parse_qs, urlparse
import os

# load_dotenv()
dotenv_path = os.path.join(os.path.dirname(__file__), 'api', '.env')

load_dotenv(dotenv_path)
APP_ID = os.getenv('APP_ID')
APP_TYPE = os.getenv('APP_TYPE')
SECRET_KEY = os.getenv('SECRET_KEY')
client_id = f'{APP_ID}-{APP_TYPE}'

FY_ID = os.getenv('FY_ID')
APP_ID_TYPE = os.getenv('APP_ID_TYPE')  # 2denotes web login

TOTP_KEY = os.getenv('TOTP_KEY')
PIN = os.getenv('PIN') # User pin for fyers account
REDIRECT_URI = os.getenv('REDIRECT_URI') # Redirect url from the APP in fyers dashboard


# API endpoints
BASE_URL = "https://api-t2.fyers.in/vagator/v2"
BASE_URL_2 = "https://api-t1.fyers.in/api/v3"
URL_SEND_LOGIN_OTP = BASE_URL + "/send_login_otp"
URL_VERIFY_TOTP = BASE_URL + "/verify_otp"
URL_VERIFY_PIN = BASE_URL + "/verify_pin"
URL_TOKEN = BASE_URL_2 + "/token"
URL_VALIDATE_AUTH_CODE = BASE_URL_2 + "/validate-authcode"

SUCCESS = 1
ERROR = -1



def send_login_otp(fy_id, app_id):
    try:
        result_string = requests.post(url=URL_SEND_LOGIN_OTP, json={
            "fy_id": fy_id, "app_id": app_id})
        if result_string.status_code != 200:
            return [ERROR, result_string.text]
        result = json.loads(result_string.text)
        request_key = result["request_key"]
        return [SUCCESS, request_key]
    except Exception as e:
        return [ERROR, e]


def verify_totp(request_key, totp):
    print("6 digits>>>",totp)
    print("request key>>>",request_key)
    try:
        result_string = requests.post(url=URL_VERIFY_TOTP, json={
            "request_key": request_key, "otp": totp})
        if result_string.status_code != 200:
            return [ERROR, result_string.text]
        result = json.loads(result_string.text)
        request_key = result["request_key"]
        return [SUCCESS, request_key]
    except Exception as e:
        return [ERROR, e]


# def generate_totp(secret):
#     try:
#         generated_totp = pyotp.TOTP(secret).now()
#         return [SUCCESS, generated_totp]

#     except Exception as e:
#         return [ERROR, e]
def generate_totp(secret):
    try:
        # Add error handling for secret formatting
        if ' ' in secret:
            secret = secret.replace(' ', '')
        if '\n' in secret:
            secret = secret.replace('\n', '')
            
        totp = pyotp.TOTP(secret)
        # Get current time
        current_time = tm.time()
        # Generate TOTP at current time
        generated_totp = totp.at(current_time)
        print(f"Generating TOTP for timestamp: {current_time}")
        return [SUCCESS, generated_totp]

    except Exception as e:
        print(f"Error generating TOTP: {str(e)}")
        return [ERROR, e]

def verify_totp(request_key, totp):
    try:
        # Ensure TOTP is string and exactly 6 digits
        totp = str(totp).zfill(6)
        
        payload = {
            "request_key": request_key,
            "otp": totp
        }
        
        print(f"Sending TOTP verification request with TOTP: {totp}")
        result_string = requests.post(url=URL_VERIFY_TOTP, json=payload)
        
        if result_string.status_code != 200:
            error_msg = result_string.text
            print(f"TOTP verification failed with status code {result_string.status_code}: {error_msg}")
            return [ERROR, error_msg]
            
        result = json.loads(result_string.text)
        if 'request_key' not in result:
            print(f"Unexpected response format: {result}")
            return [ERROR, "Invalid response format"]
            
        request_key = result["request_key"]
        return [SUCCESS, request_key]
        
    except Exception as e:
        print(f"Exception during TOTP verification: {str(e)}")
        return [ERROR, e]

def verify_PIN(request_key, pin):
    try:
        payload = {
            "request_key": request_key,
            "identity_type": "pin",
            "identifier": pin
        }

        result_string = requests.post(url=URL_VERIFY_PIN, json=payload)
        if result_string.status_code != 200:
            return [ERROR, result_string.text]

        result = json.loads(result_string.text)
        access_token = result["data"]["access_token"]

        return [SUCCESS, access_token]

    except Exception as e:
        return [ERROR, e]


def token(fy_id, app_id, redirect_uri, app_type, access_token):
    try:
        payload = {
            "fyers_id": fy_id,
            "app_id": app_id,
            "redirect_uri": redirect_uri,
            "appType": app_type,
            "code_challenge": "",
            "state": "sample_state",
            "scope": "",
            "nonce": "",
            "response_type": "code",
            "create_cookie": True
        }
        headers={'Authorization': f'Bearer {access_token}'}

        result_string = requests.post(
            url=URL_TOKEN, json=payload, headers=headers
        )

        if result_string.status_code != 308:
            return [ERROR, result_string.text]

        result = json.loads(result_string.text)
        url = result["Url"]
        auth_code = parse.parse_qs(parse.urlparse(url).query)['auth_code'][0]

        return [SUCCESS, auth_code]

    except Exception as e:
        return [ERROR, e]
    
def save_access_token(access_token):
    """Save the access token to a file in the specified directory"""
    # Create directory if it doesn't exist
    token_dir = "api/token"
    os.makedirs(token_dir, exist_ok=True)
    
    # Save token to file
    token_path = os.path.join(token_dir, "access_token")
    with open(token_path, "w") as f:
        f.write(access_token)
    print(f"Access token saved to {token_path}")

# def main():

#     # Step 1 - Retrieve request_key from send_login_otp API

#     session = fyersModel.SessionModel(client_id=client_id, secret_key=SECRET_KEY, redirect_uri=REDIRECT_URI,response_type='code', grant_type='authorization_code')

#     urlToActivate = session.generate_authcode()
#     print(f'URL to activate APP:  {urlToActivate}')

#     send_otp_result = send_login_otp(fy_id=FY_ID, app_id=APP_ID_TYPE)

#     if send_otp_result[0] != SUCCESS:
#         print(f"send_login_otp msg failure - {send_otp_result[1]}")
#         status=False
#         sys.exit()
#     else:
#         print("send_login_otp msg SUCCESS")
#         status=False


#     # Step 2 - Generate totp
#     generate_totp_result = generate_totp(secret=TOTP_KEY)

#     if generate_totp_result[0] != SUCCESS:
#         print(f"generate_totp msg failure - {generate_totp_result[1]}")
#         sys.exit()
#     else:
#         print("generate_totp msg success")


#     # Step 3 - Verify totp and get request key from verify_otp API
#     for i in range(1, 3):

#         request_key = send_otp_result[1]
#         totp = generate_totp_result[1]
#         print("otp>>>",totp)
#         verify_totp_result = verify_totp(request_key=request_key, totp=totp)
#         # print("r==",verify_totp_result)

#         if verify_totp_result[0] != SUCCESS:
#             print(f"verify_totp_result msg failure - {verify_totp_result[1]}")
#             status=False

#             tm.sleep(1)
#         else:
#             print(f"verify_totp_result msg SUCCESS {verify_totp_result}")
#             status=False
#             break

#     if verify_totp_result[0] ==SUCCESS:

#         request_key_2 = verify_totp_result[1]

#         # Step 4 - Verify pin and send back access token
#         ses = requests.Session()
#         verify_pin_result = verify_PIN(request_key=request_key_2, pin=PIN)
#         if verify_pin_result[0] != SUCCESS:
#             print(f"verify_pin_result got failure - {verify_pin_result[1]}")
#             sys.exit()
#         else:
#             print("verify_pin_result got success")


#         ses.headers.update({
#             'authorization': f"Bearer {verify_pin_result[1]}"
#         })

#          # Step 5 - Get auth code for API V2 App from trade access token
#         token_result = token(
#             fy_id=FY_ID, app_id=APP_ID, redirect_uri=REDIRECT_URI, app_type=APP_TYPE,
#             access_token=verify_pin_result[1]
#         )
#         if token_result[0] != SUCCESS:
#             print(f"token_result msg failure - {token_result[1]}")
#             sys.exit()
#         else:
#             print("token_result msg success")

#         # Step 6 - Get API V2 access token from validating auth code
#         auth_code = token_result[1]
#         session.set_token(auth_code)
#         response = session.generate_token()
#         if response['s'] =='ERROR':
#             print("\n Cannot Login. Check your credentials thoroughly!")
#             status=False
#             tm.sleep(10)
#             sys.exit()

#         access_token = response["access_token"]
#         print(access_token)
#         save_access_token(access_token)
def main():
    try:
        print("Starting Fyers authentication process...")

        # Step 1 - Generate auth code URL and send login OTP
        print("\nStep 1: Generating auth code URL and sending login OTP")
        session = fyersModel.SessionModel(
            client_id=client_id,
            secret_key=SECRET_KEY,
            redirect_uri=REDIRECT_URI,
            response_type='code',
            grant_type='authorization_code'
        )

        urlToActivate = session.generate_authcode()
        print(f'URL to activate APP: {urlToActivate}')

        send_otp_result = send_login_otp(fy_id=FY_ID, app_id=APP_ID_TYPE)
        if send_otp_result[0] != SUCCESS:
            raise Exception(f"Failed to send login OTP: {send_otp_result[1]}")
        print("Login OTP sent successfully")
        
        # Step 2 & 3 - TOTP Verification with retries
        print("\nStep 2 & 3: TOTP Generation and Verification")
        max_retries = 3
        retry_delay = 2  # seconds
        verify_totp_result = None
        
        for attempt in range(max_retries):
            # Generate fresh TOTP for each attempt
            generate_totp_result = generate_totp(secret=TOTP_KEY)
            if generate_totp_result[0] != SUCCESS:
                print(f"TOTP generation failed: {generate_totp_result[1]}")
                if attempt < max_retries - 1:
                    print(f"Retrying in {retry_delay} seconds...")
                    tm.sleep(retry_delay)
                continue

            totp = generate_totp_result[1]
            request_key = send_otp_result[1]
            
            print(f"\nAttempt {attempt + 1}/{max_retries}")
            print(f"Generated TOTP: {totp}")
            
            verify_totp_result = verify_totp(request_key=request_key, totp=totp)
            
            if verify_totp_result[0] == SUCCESS:
                print("TOTP verification successful!")
                break
            else:
                print(f"TOTP verification failed: {verify_totp_result[1]}")
                if attempt < max_retries - 1:
                    print(f"Waiting {retry_delay} seconds before next attempt...")
                    tm.sleep(retry_delay)

        if not verify_totp_result or verify_totp_result[0] != SUCCESS:
            raise Exception("All TOTP verification attempts failed")

        # Step 4 - PIN Verification
        print("\nStep 4: PIN Verification")
        request_key_2 = verify_totp_result[1]
        verify_pin_result = verify_PIN(request_key=request_key_2, pin=PIN)
        
        if verify_pin_result[0] != SUCCESS:
            raise Exception(f"PIN verification failed: {verify_pin_result[1]}")
        print("PIN verification successful")

        # Step 5 - Generate Auth Code
        print("\nStep 5: Generating Auth Code")
        token_result = token(
            fy_id=FY_ID,
            app_id=APP_ID,
            redirect_uri=REDIRECT_URI,
            app_type=APP_TYPE,
            access_token=verify_pin_result[1]
        )
        
        if token_result[0] != SUCCESS:
            raise Exception(f"Auth code generation failed: {token_result[1]}")
        print("Auth code generated successfully")

        # Step 6 - Generate Final Access Token
        print("\nStep 6: Generating Final Access Token")
        auth_code = token_result[1]
        session.set_token(auth_code)
        response = session.generate_token()
        
        if response.get('s') == 'ERROR':
            raise Exception(f"Token generation failed: {response}")
            
        access_token = response["access_token"]
        print("Access token generated successfully")
        
        # Save the access token
        save_access_token(access_token)
        print("\nAuthentication process completed successfully!")
        
        return access_token

    except Exception as e:
        print(f"\nError in authentication process: {str(e)}")
        print("Authentication failed. Please check your credentials and try again.")
        sys.exit(1)

if __name__ == "__main__":
    main()
        
if __name__ == "__main__":
    main()