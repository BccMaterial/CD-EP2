from dotenv import load_dotenv
import os

load_dotenv()

db_uri = os.getenv("DB_URI") 
db_user = os.getenv("DB_USER") 
db_password = os.getenv("DB_PASSWORD") 
