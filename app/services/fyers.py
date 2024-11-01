# backend/app/services/fyers.py

from fyers_apiv3 import fyersModel
import requests
import time
import json
import os
import pandas as pd
from app.core.config import settings

class FyersService:
    BASE_URL = "https://api.fyers.in"
    
    def __init__(self):
        self.client_id = settings.FYERS_CLIENT_ID     # e.g., "ABCD12345-100"
        self.client_id_hash = settings.FYERS_CLIENT_ID_HASH  # Hash of client_id received from Fyers
        self.refresh_token = settings.FYERS_REFRESH_TOKEN    # Your refresh token
        self.pin = settings.FYERS_PIN                         # Your 4-digit pin

        self.access_token = settings.FYERS_ACCESS_TOKEN       # Will be updated dynamically
        self.token_expires_at = settings.FYERS_TOKEN_EXPIRES_AT  # Unix timestamp

        self.fyers = fyersModel.FyersModel(client_id=self.client_id, token=self.access_token,is_async=False, log_path="")
        print("Initializing FyersService...")
        self.authenticate()

    def authenticate(self):
        current_time = time.time()
        print(f"Current time: {current_time}, Token expires at: {self.token_expires_at}")
        if not self.access_token or current_time >= float(self.token_expires_at):
            # Access token is missing or expired; refresh it
            print("Access token missing or expired, refreshing access token...")
            self.refresh_access_token()
        else:
            # Access token is valid; initialize the FyersModel
            print("Access token is valid, initializing FyersModel...")
            self.fyers = fyersModel.FyersModel(client_id=self.client_id, token=self.access_token, is_async=False)

    def refresh_access_token(self):
        print("Refreshing access token...")
        url = 'https://api-t1.fyers.in/api/v3/validate-refresh-token'
        headers = {
            'Content-Type': 'application/json'
        }
        data = {
            'grant_type': 'refresh_token',
            'appIdHash': self.client_id_hash,
            'refresh_token': self.refresh_token,
            'pin': self.pin  # Your 4-digit pin as a string
        }
        print(f"Sending request to {url} with data: {data}")
        response = requests.post(url, headers=headers, json=data)
        print(f"Response status code: {response.status_code}")
        print(f"Response content: {response.content}")

        try:
            response_data = response.json()
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            print(f"Response text: {response.text}")
            raise Exception("Failed to decode JSON response from Fyers API.")

        if response.status_code == 200 and response_data.get("access_token"):
            self.access_token = response_data["access_token"]
            expires_in = response_data.get("expires_in", 86400)  # Default to 24 hours if not provided
            self.token_expires_at = time.time() + expires_in - 60  # Refresh 1 minute before expiry

            # Save the new access token and expiry time
            self.save_tokens()

            # Initialize FyersModel with the new access token
            print("Access token refreshed successfully. Initializing FyersModel...")
            self.fyers = fyersModel.FyersModel(client_id=self.client_id, token=self.access_token, is_async=False)
        else:
            error_message = response_data.get('message', 'Unknown error')
            print(f"Failed to refresh access token: {error_message}")
            raise Exception(f"Failed to refresh access token: {error_message}")

    def save_tokens(self):
        # Update settings
        settings.FYERS_ACCESS_TOKEN = self.access_token
        settings.FYERS_TOKEN_EXPIRES_AT = str(int(self.token_expires_at))

        # Save to .env or a secure storage
        self.update_env_file({
            "FYERS_ACCESS_TOKEN": self.access_token,
            "FYERS_TOKEN_EXPIRES_AT": str(int(self.token_expires_at))
        })
        print("Tokens saved successfully.")

    def update_env_file(self, new_vars):
        # Read the existing .env file
        env_vars = {}
        if os.path.exists('.env'):
            with open('.env', 'r') as f:
                for line in f:
                    if line.strip() and '=' in line:
                        key, value = line.strip().split('=', 1)
                        env_vars[key] = value

        # Update with new variables
        env_vars.update(new_vars)

        # Write back to .env file
        with open('.env', 'w') as f:
            for key, value in env_vars.items():
                f.write(f"{key}={value}\n")
        print(".env file updated with new tokens.")
    
    def get_option_chain(self, symbol, strike_count):
        print(f"Fetching option chain for symbol: {symbol} with strike count: {strike_count}")
        
        # Prepare data for the API call
        data = {
            "symbol": symbol,
            "strikecount": strike_count,
            "timestamp": ""
        }
        print(f"Data prepared for API call: {data}")

        # Make the API call
        response = self.fyers.optionchain(data=data)
        
        # Check if the response was successful
        if response.get("s") == "ok":
            data = response["data"]
            print(f"Option chain data received")
            
            # Convert "optionsChain" section to a DataFrame
            options_chain_df = pd.DataFrame(data["optionsChain"])
            print(f"Options chain DataFrame created")

            # Filter out rows where the 'ask' column is zero
            options_chain_df = options_chain_df[options_chain_df['ask'] != 0]
            print(f"Options chain DataFrame after filtering out rows with 'ask' == 0")
            
            # Keep only the specified columns
            options_chain_df = options_chain_df[['ask', 'bid', 'option_type', 'strike_price', 'symbol']]
            print(f"Filtered options chain DataFrame: {options_chain_df.head()}")
            
            # Remove the first row from the options_chain_df
            options_chain_df = options_chain_df.iloc[1:]
            print(f"Options chain DataFrame after removing the first row: {options_chain_df.head()}")
            
            return options_chain_df
        else:
            print("Failed to retrieve option chain data")
            return None
