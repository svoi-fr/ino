import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get API keys from environment variables
MISTRAL_API_KEY = os.getenv('MISTRAL_API_KEY')

if not MISTRAL_API_KEY:
    raise ValueError("MISTRAL_API_KEY not found in environment variables") 