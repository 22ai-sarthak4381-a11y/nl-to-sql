import sys
import os
import json
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from backend.nl_to_sql_api import generate_sql

test_queries = [
    "highest average sales",
    "percentage of discount used",
    "top 3 customers by revenue",
    "total revenue",
    "list all customers"
]

print("--- Testing Consolidated Intelligent SQL Generation ---")
for q in test_queries:
    sql, enhanced, chart = generate_sql(q)
    print(f"\nQuery: '{q}'")
    print(f"SQL: {sql}")
    print(f"Suggested Chart: {chart}")
