# backend/app/utils/symbol_utils.py
import pandas as pd

def get_symbol_name(instrument_name: str, expiry_date: str, side: str) -> str:
    """
    Get symbol name based on instrument, expiry date and option type.
    
    Args:
        instrument_name (str): Name of the instrument (e.g. 'HDFCBANK')
        expiry_date (str): Expiry date in 'YYYY-MM-DD' format
        side (str): Option type ('CE' or 'PE')
        
    Returns:
        str: Symbol name
    """
    try:
        # Load data
        script = pd.read_json('https://public.fyers.in/sym_details/NSE_FO_sym_master.json')
        df = script.T
        
        # Select and filter columns
        df = df[['optType', 'underSym', 'expiryDate', 'minLotSize']]
        df = df[df['optType'] != 'XX']
        
        # Convert and format dates
        df['expiryDate'] = pd.to_datetime(df['expiryDate'], unit='s')
        df['expiryDate'] = df['expiryDate'].dt.strftime('%Y-%m-%d')
        
        # Apply filters
        filtered_df = df[
            (df['underSym'] == instrument_name) & 
            (df['expiryDate'] == expiry_date) & 
            (df['optType'] == side)
        ]
        
        if filtered_df.empty:
            return None
            
        # Get the symbol name from the original script data
        symbol_name = filtered_df.index[0]
        lot_size = filtered_df.loc[symbol_name, 'minLotSize']
        return symbol_name, lot_size
        
    except Exception as e:
        print(f"Error getting symbol name: {str(e)}")
        return None

