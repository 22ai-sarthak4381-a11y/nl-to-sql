import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase = create_client(url, key)

sql = "SELECT 1 as result"

try:
    print(f"Executing: {sql}")
    res = supabase.rpc("execute_sql", {"query": sql}).execute()
    print(f"Success: {res}")
except Exception as e:
    print(f"Error: {e}")
