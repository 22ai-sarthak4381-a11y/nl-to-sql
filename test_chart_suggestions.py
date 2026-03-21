import sys
import os
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from backend.nl_to_sql_api import suggest_chart_type

test_cases = [
    ("sales over time", ["aggregation"], "SELECT date, SUM(amount) FROM table GROUP BY date"),
    ("percentage of category sales", ["ratio"], "SELECT category, amount FROM table"),
    ("top products by revenue", ["ranking"], "SELECT product, SUM(amount) FROM table GROUP BY product"),
    ("total revenue", ["aggregation"], "SELECT SUM(amount) FROM table")
]

print("--- Testing Smart Chart Suggestions ---")
for q, intents, sql in test_cases:
    suggestion = suggest_chart_type(q, intents, sql)
    print(f"Query: '{q}' -> Suggestion: {suggestion}")
