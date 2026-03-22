
import sys
import os
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from schema_config import get_active_schema, get_active_mapping

print("--- ACTIVE SCHEMA ---")
schema = get_active_schema()
print(schema)

print("\n--- ACTIVE MAPPING ---")
mapping = get_active_mapping()
print(mapping)
