import pandas as pd
import sys
import os
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from backend.app import generate_python_summary

# Test Data 1: Top Performer
df1 = pd.DataFrame({
    'category': ['Electronics', 'Clothing', 'Home', 'Books'],
    'sales': [1000, 200, 300, 150] # Avg ~412. Electronics > 412 * 1.5 (618) -> Yes. 
})

# Test Data 2: Distribution Even
df2 = pd.DataFrame({
    'category': ['A', 'B', 'C'],
    'sales': [100, 102, 98] # Avg 100. Diff 4. 4/100 = 0.04 < 0.1 -> Yes.
})

# Test Data 3: Underperforming
df3 = pd.DataFrame({
    'category': ['A', 'B', 'C'],
    'sales': [100, 110, 30] # Avg 80. 30 < 80 * 0.5 (40) -> Yes. 
})

# Test Data 4: Not enough data
df4 = pd.DataFrame({
    'category': ['A'],
    'sales': [100]
})

print("--- Testing Intelligent Insights ---")
print(f"Test 1 (Top): '{generate_python_summary(df1)}'")
print(f"Test 2 (Even): '{generate_python_summary(df2)}'")
print(f"Test 3 (Under): '{generate_python_summary(df3)}'")
print(f"Test 4 (Empty): '{generate_python_summary(df4)}'")
