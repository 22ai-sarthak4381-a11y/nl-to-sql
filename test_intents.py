import sys
import os
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from backend.nl_to_sql_api import detect_intents

test_queries = [
    "highest average sales",
    "percentage of top products",
    "average revenue",
    "total sales by category",
    "just some plain text"
]

print("--- Testing Intent Detection ---")
for q in test_queries:
    intents = detect_intents(q)
    print(f"Query: '{q}' -> Intents: {intents}")
