import sys
import os
import pandas as pd
import json
import logging

# Ensure backend is in path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from schema_config import set_runtime_schema, get_active_mapping, set_manual_override
from nl_to_sql_api import generate_sql
from app import generate_insight

# Disable excessive logging for clean test output
logging.getLogger('backend.schema_config').setLevel(logging.ERROR)
logging.getLogger('backend.nl_to_sql_api').setLevel(logging.ERROR)

def run_test_case(dataset_name, df, queries):
    print(f"\n{'='*20} TESTING DATASET: {dataset_name} {'='*20}")
    
    # 1. Simulate Upload & Detection
    set_runtime_schema(df)
    mapping = get_active_mapping()
    print(f"Detected Mapping: {json.dumps(mapping, indent=2)}")
    
    results = []
    for q in queries:
        print(f"\nQuery: '{q}'")
        # 2. SQL Generation
        sql, enhanced, chart = generate_sql(q)
        print(f"Generated SQL: {sql}")
        print(f"Suggested Chart: {chart}")
        
        # 3. Insight Simulation (Mock values based on intent)
        # In a real app, this would be the SQL result values
        mock_values = [100, 20, 10, 5, 2] if "total" in q or "avg" in q else [50, 50]
        insight = generate_insight(mock_values)
        print(f"Insight Generated: {insight}")
        
        results.append({
            "query": q,
            "sql": sql,
            "chart": chart,
            "insight": insight
        })
    return results

# --- DATASET 1: Demo Ecommerce (Standard) ---
df_standard = pd.DataFrame({
    'purchase_amount': [100, 200],
    'purchase_category': ['Electronics', 'Home'],
    'discount_used': ['Yes', 'No']
})
queries_standard = ["total sales by category", "percentage of discount used"]

# --- DATASET 2: Different Names (Ecommerce) ---
df_alt_names = pd.DataFrame({
    'total_price': [500, 300],
    'product_type': ['Gadget', 'Tool'],
    'discount_flag': ['True', 'False']
})
queries_alt = ["total revenue by type", "average sales with discount"]

# --- DATASET 3: Different Domain (Education) ---
df_edu = pd.DataFrame({
    'exam_score': [85, 92, 45],
    'subject': ['Math', 'Science', 'Math'],
    'passed_flag': ['Yes', 'Yes', 'No']
})
queries_edu = ["average exam_score by subject", "percentage of students passed"]

# --- EXECUTION ---
all_results = {}

all_results["Dataset_1_Standard"] = run_test_case("Standard Ecommerce", df_standard, queries_standard)
all_results["Dataset_2_Alt_Names"] = run_test_case("Alternative Ecommerce Names", df_alt_names, queries_alt)
all_results["Dataset_3_Education"] = run_test_case("Education Domain", df_edu, queries_edu)

# --- TEST 4: Manual Override Verification ---
print(f"\n{'='*20} TESTING MANUAL OVERRIDE {'='*20}")
set_runtime_schema(df_edu) # Default Education mapping
set_manual_override({"measure": "exam_score", "group": "passed_flag", "discount": None})
print(f"Override applied: Group -> passed_flag")
sql_override, _, _ = generate_sql("total score by passed status")
print(f"SQL with Override: {sql_override}")

# --- TEST 5: Unknown Query Safety ---
print(f"\n{'='*20} TESTING UNKNOWN QUERY SAFETY {'='*20}")
sql_unk, _, _ = generate_sql("asdjkhaskjdh random gibberish")
print(f"Nonsense Result: {sql_unk}")

# Finalizing
with open("system_adaptation_test_results.json", "w", encoding="utf-8") as f:
    json.dump(all_results, f, indent=4)

print("\n\n✅ ALL SYSTEM ADAPTATION TESTS COMPLETED.")
print("Results saved to system_adaptation_test_results.json")
