import pandas as pd
import sys
import os
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from backend.app import generate_python_summary

# Test Data 1: Strongly Dominant
# Avg: 1300/4 = 325. Max: 1000. Dom: 1000/325 = 3.07 > 1.7. 
# 2nd: 150. Gap: (1000-150)/325 = 2.6 > 0.5. -> Dominant.
df1 = pd.DataFrame({
    'category': ['A', 'B', 'C', 'D'],
    'sales': [1000, 150, 100, 50]
})

# Test Data 2: Leading (Moderate)
# Avg: 550/4 = 137.5. Max: 250. Dom: 250/137.5 = 1.8. -> Leading.
df2 = pd.DataFrame({
    'category': ['A', 'B', 'C', 'D'],
    'sales': [250, 150, 100, 50]
})

# Test Data 3: Evenly Distributed
df3 = pd.DataFrame({
    'category': ['A', 'B', 'C'],
    'sales': [100, 102, 98]
})

# Test Data 4: Underperforming
# Avg: 210/3 = 70. Min: 30. 30 < 70*0.5 (35) -> Yes.
df4 = pd.DataFrame({
    'category': ['A', 'B', 'C'],
    'sales': [100, 80, 30]
})

print("--- Testing Adaptive Insights ---")
print(f"Test 1 (Dominant): '{generate_python_summary(df1)}'")
print(f"Test 2 (Leading):  '{generate_python_summary(df2)}'")
print(f"Test 3 (Even):     '{generate_python_summary(df3)}'")
print(f"Test 4 (Under):    '{generate_python_summary(df4)}'")
