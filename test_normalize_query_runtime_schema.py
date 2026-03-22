import os
import sys

import pandas as pd


os.environ.setdefault("GROQ_API_KEY", "test-key")
os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "test-key")

sys.path.append(os.path.join(os.getcwd(), "backend"))

from schema_config import set_runtime_schema
from app import normalize_query


def test_normalize_query_switches_with_runtime_schema():
    demo_df = pd.DataFrame({
        "purchase_amount": [100],
        "purchase_category": ["electronics"],
        "location": ["delhi"],
    })
    set_runtime_schema(demo_df)
    demo_normalized = normalize_query("total sales by city")
    assert "purchase_amount" in demo_normalized
    assert "location" in demo_normalized

    upload_df = pd.DataFrame({
        "revenue": [200],
        "product_type": ["gadget"],
        "region": ["mumbai"],
    })
    set_runtime_schema(upload_df)
    upload_normalized = normalize_query("total sales by city")
    assert "revenue" in upload_normalized
    assert "region" in upload_normalized
    assert "purchase_amount" not in upload_normalized


if __name__ == "__main__":
    test_normalize_query_switches_with_runtime_schema()
    print("test_normalize_query_runtime_schema.py passed")
