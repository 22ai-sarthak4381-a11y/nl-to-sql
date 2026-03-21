import sys
import os
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from backend.nl_to_sql_api import extract_top_k

test_queries = [
    "top 3 products",
    "highest sales",
    "best performing categories",
    "list all customers",
    "show top 12 users by revenue"
]

print("--- Testing Top-K Detection ---")
for q in test_queries:
    top_k = extract_top_k(q)
    print(f"Query: '{q}' -> Top-K: {top_k}")
