import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase = create_client(url, key)

table_name = "test_table_123"
sql = f"CREATE TABLE {table_name} (col1 TEXT);"

try:
    print(f"Executing: {sql}")
    res = supabase.rpc("execute_sql", {"query": sql}).execute()
    print(f"Success: {res}")
except Exception as e:
    print(f"Error: {e}")

sql_drop = f"DROP TABLE {table_name};"
try:
    print(f"Executing: {sql_drop}")
    res = supabase.rpc("execute_sql", {"query": sql_drop}).execute()
    print(f"Success: {res}")
except Exception as e:
    print(f"Error: {e}")
