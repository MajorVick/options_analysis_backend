# backend/app/main.py
from fastapi import FastAPI
from app.routers import option_chain  # Change to absolute import
from app.core.config import settings  # Change to absolute import
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(
    title="Options Trading Analysis API",
    version="1.0.0",
    description="API for fetching option chain data and calculating margins and premiums."
)

# Include routers
app.include_router(option_chain.router, prefix="/api/v1")
