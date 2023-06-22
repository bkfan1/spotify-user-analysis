import os
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
SCOPE = os.getenv("SCOPE")
TARGET_DATABASE_URL = os.getenv("TARGET_DATABASE_URL")
EDA_DATABASE_URL = os.getenv("EDA_DATABASE_URL")
