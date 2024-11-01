# backend/app/utils/calculations.py

import pandas as pd
import requests
from app.core.config import settings
from app.services.fyers import FyersService
from app.utils.symbol_utils import get_symbol_name

def get_highest_option_prices(options_chain_df, instrument_name, side: str):
    """
    Get highest option prices for either PE bid or CE ask based on side parameter.
    Args:
        option_chain_df: DataFrame containing option chain data
        instrument_name: Name of the instrument
        side: 'PE' for put options or 'CE' for call options
    """

    # Filter the DataFrame to keep only rows where option_type is equal to side
    filtered_df = options_chain_df[options_chain_df['option_type'] == side]

    # Define a function to set the bid/ask value
    def set_bid_ask(row):
        return row['ask'] if side == 'CE' else row['bid']

    # Apply the function to each row to create the bid/ask column
    filtered_df['bid/ask'] = filtered_df.apply(set_bid_ask, axis=1)

    # Drop the ask and bid columns
    filtered_df = filtered_df.drop(columns=['ask', 'bid'])

    # Add the instrument_name column with the same value for all rows
    filtered_df['instrument_name'] = instrument_name

    # Display the updated DataFrame
    return filtered_df

def get_option_chain_data(instrument_name: str, expiry_date: str, side: str) -> pd.DataFrame:
    print(f"Fetching option chain data for instrument: {instrument_name}, expiry: {expiry_date}, side: {side}")
    fyers_service = FyersService()
    print('Fyer_service instance created')
    symbol, lot_size = get_symbol_name(instrument_name, expiry_date, side)
    print(f"Symbol resolved to: {symbol}")
    strike_count = 40  # Adjust as needed (maximum allowed is 50)

    try:
        # Fetch option chain data
        options_chain_data = fyers_service.get_option_chain(symbol, strike_count)
        print(f"Option chain data fetched successfully: {options_chain_data}")
    except Exception as e:
        # Handle exceptions from the FyersService
        print(f"Error fetching option chain data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
    # Get the highest option prices for the given option chain
    result_df = get_highest_option_prices(options_chain_data, instrument_name, side)

    print(f"Resulting DataFrame: {result_df}")
    return result_df, lot_size

def calculate_margin_and_premium(df: pd.DataFrame, lot_size: int) -> pd.DataFrame:
    # Create copy of DataFrame to avoid modifying original
    result_df = df.copy()
    result_df['margin'] = 0.0
    result_df['premium'] = 0.0
    
    # API endpoint and headers
    url = "https://api.fyers.in/api/v2/span_margin"
    headers = {
        "Authorization": f"{settings.FYERS_CLIENT_ID}:{settings.FYERS_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Process each row
    for idx, row in result_df.iterrows():
        try:
            # Prepare request payload
            payload = {
                "data": [{
                    "symbol": row['symbol'],
                    "qty": lot_size,
                    "side": -1,
                    "type": 2,
                    "productType": "INTRADAY",
                    "limitPrice": 0.0,
                    "stopLoss": 0.0
                }]
            }
            
            # Make API call
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            margin_data = response.json()
            
            # Extract margin from response
            if margin_data.get('data', {}).get('total'):
                result_df.at[idx, 'margin'] = margin_data['data']['total']
            
            # Calculate premium using bid/ask price
            if 'bid/ask' in row:
                result_df.at[idx, 'premium'] = row['bid/ask'] * lot_size
            else:
                # Fallback to last traded price if bid/ask not available
                result_df.at[idx, 'premium'] = row.get('last_traded_price', 0) * lot_size
                
        except requests.RequestException as e:
            print(f"Error calculating margin for symbol {row['symbol']}: {str(e)}")
            continue
            
    return result_df
