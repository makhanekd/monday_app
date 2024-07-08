import os

from dotenv import load_dotenv

load_dotenv()


MONDAY_API_KEY = os.getenv('MONDAY_API_KEY', 'API_KEY')
