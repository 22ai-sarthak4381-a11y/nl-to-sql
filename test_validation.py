import sys
import os
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from backend.nl_to_sql_api import validate_query

test_queries = [
    "total sales",
    "asdjkhaskjdh query random test",
    "what is the weather today",
    "highest revenue by category",
    "abc 123 !@#"
]

print("--- Testing Input Validation ---")
for q in test_queries:
    is_valid, msg = validate_query(q)
    print(f"Query: '{q}' -> Valid: {is_valid} | Message: {msg}")
