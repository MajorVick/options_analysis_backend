# backend/app/routers/option_chain.py

from fastapi import APIRouter, HTTPException
from app.utils.calculations import get_option_chain_data, calculate_margin_and_premium

router = APIRouter()

@router.get("/option-chain")
def option_chain(instrument_name: str, expiry_date: str, side: str):
    print(f"Received request with instrument_name: {instrument_name}, expiry_date: {expiry_date}, side: {side}")
    try:
        data, lot_size = get_option_chain_data(instrument_name, expiry_date, side)
        print(f"Data retrieved: {data}")
        # For testing purposes, you can assign dummy values for margin and premium
        # data['margin_required'] = 0  # Dummy value
        # data['premium_earned'] = 0   # Dummy value
        data = calculate_margin_and_premium(data, lot_size)
        data = data[['instrument_name', 'strike_price', 'option_type', 'bid/ask', 'margin', 'premium']]
        print(f"Modified data with dummy values: {data}")
        return data.to_dict(orient='records')
    except Exception as e:
        # Log the error
        print(f"Error in option_chain endpoint: {str(e)}")
        # Raise an HTTPException with status code 500
        raise HTTPException(status_code=500, detail=str(e))