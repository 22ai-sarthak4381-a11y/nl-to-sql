import sys
import os
import json
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from backend.nl_to_sql_api import extract_column_hints, COLUMN_SYNONYMS

test_queries = [
    "total revenue",
    "top customers by spending",
    "number of orders",
    "just some plain text"
]

print("--- Testing Column Hint Extraction ---")
for q in test_queries:
    hints = extract_column_hints(q, COLUMN_SYNONYMS)
    print(f"Query: '{q}' -> Hints: {json.dumps(hints)}")
