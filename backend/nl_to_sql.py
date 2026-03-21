# nl_to_sql.py (Groq API version)
import os
from dotenv import load_dotenv
from groq import Groq
import time
import random
import json
import hashlib
import streamlit as st

load_dotenv()

# Initialize Groq client
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

# Default columns
DEFAULT_COLUMNS = [
    "customer_id", "age", "gender", "income_level", "marital_status", "education_level",
    "occupation", "location", "purchase_category", "purchase_amount",
    "frequency_of_purchase", "purchase_channel", "brand_loyalty", "product_rating",
    "time_spent_on_product_researchhours", "social_media_influence", "discount_sensitivity",
    "return_rate", "customer_satisfaction", "engagement_with_ads", "device_used_for_shopping",
    "payment_method", "time_of_purchase", "discount_used", "customer_loyalty_program_member",
    "purchase_intent", "shipping_preference", "time_to_decision"
]

# Path for persistent cache
CACHE_FILE = "ai_query_cache.json"

# Load file cache into memory
def load_file_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r") as f:
                return json.load(f)
        except:
            return {}
    return {}

# Save memory cache to file
def save_file_cache(cache_data):
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(cache_data, f, indent=4)
    except:
        pass

# Model Configuration Mapping
# Maps the user-requested 'groq-1' to a verified flagship model identifier on their account
MODEL_MAPPING = {
    "groq-1": "groq/compound"  # Map to verified flagship model
}

# Columns that are stored as TEXT but contain TRUE numeric values — safe to cast ::NUMERIC
NUMERIC_COLUMNS = {
    "purchase_amount",              # e.g. 150.0
    "age",                          # e.g. 28
    "product_rating",               # e.g. 4.5
    "time_spent_on_product_researchhours",  # e.g. 2.3
    "return_rate",                  # e.g. 0.15
    "customer_satisfaction",        # e.g. 7
    "time_to_decision",             # e.g. 3
}

# Columns that are CATEGORICAL TEXT — values like "Middle", "High", "Daily"
# NEVER cast these to NUMERIC — it will always fail
CATEGORICAL_COLUMNS = {
    "income_level",          # Low / Middle / High
    "frequency_of_purchase", # Daily / Weekly / Monthly / Rarely
    "discount_sensitivity",  # Low / Medium / High
    "brand_loyalty",         # Low / Medium / High
    "social_media_influence",# Low / Medium / High
    "engagement_with_ads",   # Low / Medium / High
    "purchase_intent",       # Low / Medium / High
    "discount_used",         # Yes / No
    "customer_loyalty_program_member",  # Yes / No
    "gender",                # Male / Female
    "marital_status",        # Single / Married / Divorced
    "education_level",       # High School / Bachelor / Master / PhD
    "occupation",            # text
    "location",              # text
    "purchase_category",     # text
    "purchase_channel",      # text
    "device_used_for_shopping", # text
    "payment_method",        # text
    "shipping_preference",   # text
}

# Columns that are DATE/TIME values stored as TEXT — MUST cast ::TIMESTAMP
DATE_COLUMNS = {
    "time_of_purchase",
    "sale_date"
}

# All aggregate/statistical functions that require numeric operands
AGGREGATE_FUNCS = ["SUM", "AVG", "MAX", "MIN", "CORR", "STDDEV", "VARIANCE"]

def fix_sql_type_casts(sql: str) -> str:
    """
    Post-processes generated SQL:
    1. Ensures TRUE numeric columns get ::NUMERIC inside aggregate functions.
    2. Strips any ::NUMERIC cast from CATEGORICAL columns (would cause 'invalid input
       syntax for type numeric: "Middle"' etc.) wherever they appear.
    """
    import re

    # Pass 1 — Add ::NUMERIC for true numeric columns inside aggregate functions
    for func in AGGREGATE_FUNCS:
        for col in NUMERIC_COLUMNS:
            # Match FUNC(col) but NOT FUNC(col::NUMERIC) — avoids double-casting
            pattern = rf'(?i)({func}\(\s*)({col})(\s*\))'
            def add_cast(m):
                # Skip if already cast
                rest = m.string[m.end():m.end()+10]
                if '::NUMERIC' in m.group(0).upper():
                    return m.group(0)
                return f'{m.group(1)}{m.group(2)}::NUMERIC{m.group(3)}'
            sql = re.sub(pattern, add_cast, sql)

    # Pass 2 — Remove any ::NUMERIC cast on categorical columns
    for col in CATEGORICAL_COLUMNS:
        # Pattern: col::NUMERIC or col :: NUMERIC (with optional spaces)
        pattern = rf'(?i)(\b{col}\b)\s*::\s*NUMERIC'
        sql = re.sub(pattern, col, sql)

    # Pass 3 — Ensure Date columns get ::TIMESTAMP when used in extraction functions
    # (Fixes: function pg_catalog.extract(unknown, text) does not exist)
    date_funcs = ["EXTRACT", "DATE_TRUNC", "DATE"]
    for func in date_funcs:
        for col in DATE_COLUMNS:
            # Match FUNC(... col ...) and ensure col has ::TIMESTAMP
            # Simple check: if func is present and col is present, but col::TIMESTAMP is NOT
            if re.search(rf'(?i){func}', sql) and re.search(rf'(?i)\b{col}\b', sql):
                if f'{col}::TIMESTAMP' not in sql.upper():
                    sql = re.sub(rf'(?i)\b({col})\b', r'\1::TIMESTAMP', sql)

    return sql

# SQL generation function using Groq API
def generate_sql_with_groq(nl_query, table_name="ecommerce_behavior", columns=None):
    """
    Converts a natural language query to a PostgreSQL SQL statement using the Groq API.

    Args:
        nl_query (str): The natural language question from the user.
        table_name (str): The target database table name.
        columns (list): Optional list of column names to use.

    Returns:
        str: A valid PostgreSQL SQL query string.

    Raises:
        Exception: On API failure after all retries are exhausted.
    """
    if columns is None:
        columns = DEFAULT_COLUMNS

    columns_str = ", ".join(columns)
    prompt = f"""
Convert the following question into PostgreSQL SQL.

CRITICAL RULES — follow ALL of them exactly:
1. Table name is {table_name}
2. Available columns: {columns_str}

3. COLUMN TYPES (this is essential — do not guess):

   TRUE NUMERIC columns (stored as text but contain numbers — MUST cast with ::NUMERIC
   before using in SUM/AVG/MAX/MIN/CORR or any arithmetic):
     purchase_amount, age, product_rating, return_rate,
     customer_satisfaction, time_to_decision, time_spent_on_product_researchhours

   CATEGORICAL TEXT columns (contain words like Low/Middle/High/Daily/Yes/No —
   NEVER cast these to NUMERIC, doing so will cause a database error):
     income_level, frequency_of_purchase, discount_sensitivity, brand_loyalty,
     social_media_influence, engagement_with_ads, purchase_intent, discount_used,
     customer_loyalty_program_member, gender, marital_status, education_level,
     occupation, location, purchase_category, purchase_channel,
     device_used_for_shopping, payment_method, shipping_preference

   For questions about "factors influencing" something: use a single UNION ALL query,
   NOT multiple separate SELECT statements. Each sub-query picks one categorical factor.
   Example structure for 'what factors influence purchase amount':
     SELECT 'income_level' AS factor, income_level AS value, AVG(purchase_amount::NUMERIC) AS avg_purchase FROM ecommerce_behavior GROUP BY income_level
     UNION ALL
     SELECT 'gender' AS factor, gender AS value, AVG(purchase_amount::NUMERIC) AS avg_purchase FROM ecommerce_behavior GROUP BY gender
     UNION ALL
     SELECT 'purchase_channel' AS factor, purchase_channel AS value, AVG(purchase_amount::NUMERIC) AS avg_purchase FROM ecommerce_behavior GROUP BY purchase_channel
     ORDER BY avg_purchase DESC
   Do NOT use CORR() with categorical columns.

4. Correct casting examples:
   CORRECT:   SUM(purchase_amount::NUMERIC)
   CORRECT:   AVG(age::NUMERIC)
   INCORRECT: SUM(income_level::NUMERIC)  <- income_level is categorical
   INCORRECT: CORR(x, income_level::NUMERIC)  <- never CORR on categorical
   CORRECT:   EXTRACT(MONTH FROM time_of_purchase::TIMESTAMP)
   INCORRECT: EXTRACT(MONTH FROM time_of_purchase)  <- missing cast

5. Use ILIKE for case-insensitive text searches.
6. CRITICAL: Return ONLY a SINGLE SQL query — no markdown, no explanation, no code fences.
7. NEVER return multiple separate SELECT statements. If you need to compare multiple
   groups or factors, combine them into ONE query using UNION ALL.

Question: {nl_query}
"""

    # --- Persistent file cache logic ---
    query_hash = hashlib.md5(f"{table_name}:{prompt}".encode()).hexdigest()
    file_cache = load_file_cache()
    if query_hash in file_cache:
        print(f"✨ [Cache Hit] Returning saved result for: {nl_query[:30]}...")
        return file_cache[query_hash]

    # --- Session cache logic (Streamlit) ---
    if "cached_sql" not in st.session_state:
        st.session_state.cached_sql = {}
    if query_hash in st.session_state.cached_sql:
        print(f"✨ [Session Cache Hit] Returning cached result for: {nl_query[:30]}...")
        return st.session_state.cached_sql[query_hash]

    # --- API call with error handling and retry ---
    max_retries = 3
    base_delay = 2
    for attempt in range(max_retries):
        try:
            print(f"DEBUG: [Attempt {attempt+1}] Calling Groq API for question: {nl_query[:30]}...")
            # Use smart mapping to resolve 'groq-1' to a working model name
            actual_model = MODEL_MAPPING.get("groq-1", "groq-1")
            
            response = client.chat.completions.create(
                model=actual_model,
                messages=[
                    {"role": "system", "content": "You are a SQL expert that converts natural language to PostgreSQL queries. Return only the SQL query, no explanation."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=500
            )

            if response and response.choices[0].message.content:
                sql_text = response.choices[0].message.content.strip()
                # Clean up markdown formatting if the model included it
                sql_text = sql_text.replace("```sql", "").replace("```", "").strip()
                # Safety net: ensure numeric columns are always properly cast
                sql_text = fix_sql_type_casts(sql_text)
                # Save to caches
                st.session_state.cached_sql[query_hash] = sql_text
                file_cache[query_hash] = sql_text
                save_file_cache(file_cache)

                return sql_text
            else:
                raise Exception("Empty response from Groq API")

        except Exception as e:
            err_msg = str(e).lower()

            # Rate limit handling
            if "rate_limit" in err_msg or "rate limit" in err_msg or "429" in err_msg:
                print(f"⚠️ Groq Rate limit hit for question: {nl_query[:30]}...")
                if attempt < max_retries - 1:
                    delay = (base_delay * (2 ** attempt)) + random.uniform(0, 1)
                    print(f"⚠️ Rate limit. Retrying in {delay:.2f}s...")
                    time.sleep(delay)
                    continue
                else:
                    raise Exception("Groq API rate limit exceeded. Please try again in a moment.")

            # Authentication / key errors
            if "authentication" in err_msg or "invalid_api_key" in err_msg or "401" in err_msg:
                print(f"❌ Groq Authentication Error: {e}")
                raise Exception("Groq API authentication failed. Please check your GROQ_API_KEY in the .env file.")

            # Generic retry
            print(f"ERROR: Groq API failure on attempt {attempt+1}: {e}")
            if attempt < max_retries - 1:
                delay = (base_delay * (2 ** attempt)) + random.uniform(0, 1)
                time.sleep(delay)
                continue
            else:
                raise Exception(f"Groq API error after {max_retries} attempts: {str(e)}")


# Backward-compatible alias so dashboard.py import still works
def generate_sql(question, table_name="ecommerce_behavior", columns=None):
    """Alias for generate_sql_with_groq for backward compatibility."""
    return generate_sql_with_groq(question, table_name=table_name, columns=columns)