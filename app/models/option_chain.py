# backend/app/models/option_chain.py
from pydantic import BaseModel
from typing import Optional

class OptionChainData(BaseModel):
    instrument_name: str
    strike_price: float
    side: str
    price: float
    margin_required: Optional[float] = None
    premium_earned: Optional[float] = None
