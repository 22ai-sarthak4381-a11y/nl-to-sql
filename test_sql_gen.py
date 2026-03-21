import sys
import os
import re

# Set up environment
sys.path.append(os.path.join(os.getcwd(), 'backend'))
# GROQ_API_KEY will be loaded from .env automatically

from backend.nl_to_sql_api import generate_sql, fix_sql_type_casts

query = "total sales by category"
print(f"--- Running Test for Query: '{query}' ---")

# Test 1: Baseline AI Output (Simulated)
raw_sql = "SELECT purchase_category, SUM(purchase_amount) FROM ecommerce_behavior GROUP BY purchase_category ORDER BY SUM(purchase_amount) DESC;"
print(f"Raw Input to Fixer: {raw_sql}")
fixed_sql = fix_sql_type_casts(raw_sql)
print(f"Fixed Output: {fixed_sql}")

# Test 2: Real AI Flow
try:
    sql, enhanced = generate_sql(query)
    print(f"Final SQL: {sql}")
    print(f"Enhanced Query: {enhanced}")
except Exception as e:
    print(f"Error during Real Gen: {str(e)}")
