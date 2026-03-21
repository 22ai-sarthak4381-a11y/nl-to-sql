import pandas as pd
import sys
import os
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from backend.app import generate_python_summary

# Test Case 1: Skewed (Strong Dominance)
# Leader 800/1000 = 80% share. 
df1 = pd.DataFrame({'cat': ['A', 'B', 'C'], 'val': [800, 100, 100]})

# Test Case 2: Moderate Leader (15% < share < 30%)
# Leader 25/110 = 22% share. Dom ~ 2.4 (25/10.4).
df2 = pd.DataFrame({'cat': ['A'] + ['Other'+str(i) for i in range(10)], 'val': [25] + [8.5]*10})

# Test Case 3: Low Percentage Leader (No dominance)
# Leader 9/100 = 9% share.
df3 = pd.DataFrame({'cat': ['A'] + ['Other'+str(i) for i in range(11)], 'val': [9] + [8.27]*11})

# Test Case 4: Equal Values
df4 = pd.DataFrame({'cat': ['A', 'B', 'C'], 'val': [100, 100, 100]})

print("--- Testing Controlled Accuracy Insights ---")
print(f"Test 1 (Strong): '{generate_python_summary(df1)}'")
print(f"Test 2 (Moderate): '{generate_python_summary(df2)}'")
print(f"Test 3 (No Dom):  '{generate_python_summary(df3)}'")
print(f"Test 4 (Even):    '{generate_python_summary(df4)}'")
