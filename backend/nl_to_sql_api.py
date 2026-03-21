# backend/nl_to_sql_api.py (Clean version for Flask API)
import os
from dotenv import load_dotenv
from groq import Groq
import time
import random
import json
import hashlib
import re

load_dotenv()

import logging

# Initialize Groq client
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

# Configure Logger for this module
logger = logging.getLogger(__name__)

# --- Column Mapping Configuration ---
COLUMN_SYNONYMS = {
    "purchase_amount": ["sales", "revenue", "spending", "amount", "expenditure"],
    "customer_id": ["customer", "user", "buyer", "client"],
    "purchase_category": ["category", "type", "group", "class"],
    "location": ["city", "state", "region", "place", "area"],
    "product_rating": ["stars", "score", "feedback", "rating"],
    "time_of_purchase": ["date", "time", "when", "recorded"],
    "frequency_of_purchase": ["frequency", "how often", "regularity"],
    "education_level": ["degree", "studies", "qualification"],
    "income_level": ["salary", "earnings", "wealth", "pay"],
    "marital_status": ["relationship", "married", "single"],
    "occupation": ["job", "work", "profession", "role"],
    "purchase_channel": ["method", "channel", "platform", "source"],
    "brand_loyalty": ["loyalty", "affinity"],
    "discount_used": ["coupon", "promo", "voucher"],
    "return_rate": ["refunds", "returns"],
    "customer_satisfaction": ["happiness", "csat"],
    "device_used_for_shopping": ["device", "mobile", "desktop", "app"]
}

def extract_column_hints(query: str, column_mapping: dict):
    """
    Scans the query for synonyms and returns a dictionary of matched columns.
    """
    q_lower = query.lower()
    hints = {}
    for col, synonyms in column_mapping.items():
        for syn in synonyms:
            # Match whole-word synonyms
            if re.search(rf'\b{re.escape(syn)}\b', q_lower):
                hints[syn] = col
                break # Move to next column once a synonym matches
    return hints

# --- Semantic Mapping configuration ---
SEMANTIC_MAP = {
    "best customers": "top customers by total purchase_amount",
    "top customers": "customers with highest total purchase_amount",
    "recent": "last 30 days",
    "high value": "purchase_amount > average purchase_amount",
    "frequent buyers": "customers with high frequency_of_purchase",
    "low rating": "product_rating < 3",
    "highly rated": "product_rating > 4",
    "returning customers": "customers with high return_rate",
    "mobile users": "device_used_for_shopping = 'Smartphone'"
}

def apply_semantic_layer(query):
    if not query or not isinstance(query, str):
        return query
        
    enhanced_query = query
    # Sort by length descending to replace longer phrases first
    for term in sorted(SEMANTIC_MAP.keys(), key=len, reverse=True):
        mapping = SEMANTIC_MAP[term]
        # Case-insensitive whole word/phrase replacement
        pattern = re.compile(rf'\b{re.escape(term)}\b', re.IGNORECASE)
        enhanced_query = pattern.sub(mapping, enhanced_query)
        
    if not enhanced_query or enhanced_query.strip() == "":
        enhanced_query = query
        
    print(f"Enhanced Query: {enhanced_query}")
    logger.info(f"Semantic Layer applied. Enhanced query: {enhanced_query}")
    return enhanced_query

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

def load_file_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_file_cache(cache_data):
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(cache_data, f, indent=4)
    except:
        pass

MODEL_MAPPING = {
    "groq-1": "llama-3.3-70b-versatile"
}

NUMERIC_COLUMNS = {
    "purchase_amount", "age", "product_rating", "time_spent_on_product_researchhours",
    "return_rate", "customer_satisfaction", "time_to_decision",
}

CATEGORICAL_COLUMNS = {
    "income_level", "frequency_of_purchase", "discount_sensitivity", "brand_loyalty",
    "social_media_influence", "engagement_with_ads", "purchase_intent", "discount_used",
    "customer_loyalty_program_member", "gender", "marital_status", "education_level",
    "occupation", "location", "purchase_category", "purchase_channel",
    "device_used_for_shopping", "payment_method", "shipping_preference"
}

DATE_COLUMNS = {
    "time_of_purchase",
    "sale_date"
}

AGGREGATE_FUNCS = ["SUM", "AVG", "MAX", "MIN", "CORR", "STDDEV", "VARIANCE"]

def fix_sql_type_casts(sql: str) -> str:
    # Pass 1 — Add ::NUMERIC for true numeric columns inside aggregate functions
    for func in AGGREGATE_FUNCS:
        for col in NUMERIC_COLUMNS:
            pattern = rf'(?i)({func}\(\s*)({col})(\s*\))'
            def add_cast(m):
                if '::NUMERIC' in m.group(0).upper():
                    return m.group(0)
                return f'{m.group(1)}{m.group(2)}::NUMERIC{m.group(3)}'
            sql = re.sub(pattern, add_cast, sql)

    # Pass 2 — Remove any ::NUMERIC cast on categorical columns
    for col in CATEGORICAL_COLUMNS:
        pattern = rf'(?i)(\b{col}\b)\s*::\s*NUMERIC'
        sql = re.sub(pattern, col, sql)

    # Pass 3 — Ensure Date columns get ::TIMESTAMP
    date_funcs = ["EXTRACT", "DATE_TRUNC", "DATE"]
    for func in date_funcs:
        for col in DATE_COLUMNS:
            if re.search(rf'(?i){func}', sql) and re.search(rf'(?i)\b{col}\b', sql):
                # Ensure we don't double-cast if ::TIMESTAMP or ::DATE already exists
                if f'{col.upper()}::TIMESTAMP' not in sql.upper() and f'{col.upper()}::DATE' not in sql.upper():
                    sql = re.sub(rf'(?i)\b({col})\b(?!\s*::\s*(TIMESTAMP|DATE))', r'\1::TIMESTAMP', sql)

    # Pass 4 — Add ::NUMERIC for numeric comparisons in WHERE clauses (e.g., col > 100)
    def replacer(match):
        column = match.group(1)
        operator = match.group(2)
        number = match.group(3)
        return f"{column}::NUMERIC {operator} {number}"
    
    pattern = r'(\b\w+\b)\s*([<>]=?)\s*(\d+(\.\d+)?)'
    sql = re.sub(pattern, replacer, sql)

    # Pass 5 — Fix incorrect COUNT(condition) patterns (Common AI mistake for percentages)
    # Correct: COUNT(CASE WHEN col = 'val' THEN 1 END)
    # Incorrect: COUNT(col = 'val')
    def fix_conditional_count(match):
        condition = match.group(1)
        # Avoid double-fixing if CASE WHEN is already there
        if "CASE" in condition.upper():
            return match.group(0)
        return f"COUNT(CASE WHEN {condition} THEN 1 END)"
    
    # Matches COUNT( any_condition ) where any_condition contains = or > or <
    sql = re.sub(r'(?i)COUNT\(([^)]*=[^)]*|[^)]*>[^)]*|[^)]*<[^)]*)\)', fix_conditional_count, sql)

    # Pass 6 — Ensure Categorical Data Normalization (LOWER(TRIM(COALESCE(col, ''))) = 'lowercase')
    # Target: col = 'Value' -> LOWER(TRIM(COALESCE(col, ''))) = 'value'
    def categorical_normalization_fix(match):
        column = match.group(1)
        value = match.group(2).lower()
        # Avoid double-fixing, fixing numeric comparisons, or fixing SQL keywords
        if "LOWER(" in column.upper() or column.upper() in ["LIMIT", "ORDER", "GROUP", "OFFSET", "WHERE"]:
            return match.group(0)
        return f"LOWER(TRIM(COALESCE({column}, ''))) = '{value}'"

    # Matches [column] = '[Value]'
    sql = re.sub(r'(\b[a-zA-Z_][a-zA-Z0-9_]*\b)\s*=\s*\'([^\']*)\'', categorical_normalization_fix, sql)

    return sql


# --- Intent Detection Layer ---
def detect_intents(query: str):
    """
    Identifies specific business intents in the user query using keyword mapping.
    Supports multiple intents per query.
    """
    q = query.lower()
    intents = []
    
    # 1. Ranking Intent
    if any(word in q for word in ["highest", "lowest", "top", "bottom", "rank", "peak", "best", "worst"]):
        intents.append("ranking")
        
    # 2. Aggregation Intent
    if any(word in q for word in ["average", "avg", "mean", "sum", "total", "revenue", "sales", "earnings"]):
        intents.append("aggregation")
        
    # 3. Ratio/Percentage Intent
    if any(word in q for word in ["percentage", "ratio", "proportion", "%", "fraction"]):
        intents.append("ratio")
        
    # Default to general if no specific intent found
    if not intents:
        intents.append("general")
        
    return intents

def suggest_chart_type(query: str, intents: list, sql: str):
    """
    Suggests the most appropriate chart type for the data based on intent and query structure.
    """
    q = query.lower()
    
    # 1. Time-related words -> Line Chart
    if any(word in q for word in ["date", "month", "year", "time", "trend", "over time", "day", "weekly", "monthly", "timeline"]):
        return "line"
        
    # 2. Ratio Intent or % -> Pie Chart
    if "ratio" in intents or "%" in q or "percentage" in q or "proportion" in q:
        return "pie"
        
    # 3. Aggregation with Grouping -> Bar Chart
    if "GROUP BY" in sql.upper():
        return "bar"
        
    # 4. Default for single results -> KPI
    return "kpi"

def extract_top_k(query: str):
    """
    Identifies Top-K preferences (like 'top 5' or 'best') from the user query.
    """
    q = query.lower()
    
    # 1. Explicit pattern: "top 5", "top 10", etc.
    match = re.search(r'top\s+(\d+)', q)
    if match:
        return int(match.group(1))
        
    # 2. Implicit ranking words -> suggest 5
    if any(word in q for word in ["highest", "best", "most", "top", "rank", "peak", "worst", "lowest"]):
        return 5
        
    return None

def get_fallback_sql(user_query, table_name="ecommerce_behavior"):
    """
    Returns a predefined SQL query based on keywords when AI fails.
    """
    q = user_query.lower()
    logger.info(f"Fallback logic triggered for query: {user_query}")
    print(f"🔄 Applying fallback logic for: '{user_query}'")
    
    # Intent Mapping
    is_top = any(word in q for word in ["highest", "top", "best", "rank"])
    is_revenue = any(word in q for word in ["sales", "revenue", "earnings", "income", "total amount"])
    is_count = any(word in q for word in ["count", "number", "how many", "users", "orders"])
    is_avg = any(word in q for word in ["average", "avg", "mean", "per user"])

    metric = "COUNT(*)" if is_count else "SUM(purchase_amount::NUMERIC)" if is_revenue else "AVG(purchase_amount::NUMERIC)" if is_avg else "*"
    alias = "total_count" if is_count else "total_revenue" if is_revenue else "average_spend" if is_avg else "records"

    if "category" in q:
        sql = f"SELECT purchase_category, {metric} AS {alias} FROM {table_name} GROUP BY purchase_category"
        if is_top: sql += f" ORDER BY {alias} DESC LIMIT 1"
        return sql + ";"

    if "location" in q or "city" in q:
        sql = f"SELECT location, {metric} AS {alias} FROM {table_name} GROUP BY location"
        if is_top: sql += f" ORDER BY {alias} DESC LIMIT 1"
        return sql + ";"

    if is_revenue:
        return f"SELECT SUM(purchase_amount::NUMERIC) AS total_revenue FROM {table_name};"
    elif is_count:
        return f"SELECT COUNT(*) AS total_count FROM {table_name};"
    elif is_avg:
        return f"SELECT AVG(purchase_amount::NUMERIC) AS average_spend FROM {table_name};"
    
    # Block default SELECT * unless explicitly asked
    if any(word in q for word in ["all data", "show everything", "everything", "all records"]):
        return f"SELECT * FROM {table_name} LIMIT 10;"
    
    # If totally unclear, return None so the caller can stop processing
    return None

def validate_query(query: str):
    """
    Uses AI to classify if a query is meaningful/relevant or garbage/invalid.
    Returns (is_valid, message)
    """
    if len(query.strip()) < 3:
        return False, "Query is too short. Please ask a meaningful question."

    try:
        actual_model = MODEL_MAPPING.get("groq-1", "llama-3.3-70b-versatile")
        prompt = f"""
        Classify the following user query for a business dashboard as 'VALID' or 'INVALID'.
        A query is VALID if it is a meaningful request for data, analytics, or business insights (e.g., "total sales", "highest revenue", "customer count").
        A query is INVALID if it is garbage text, random characters, completely unrelated to business data, or non-sensical (e.g., "asdjkhaskjdh", "random test query abc", "what is the weather").

        Query: "{query}"

        Return ONLY the word 'VALID' or 'INVALID'.
        """
        
        response = client.chat.completions.create(
            model=actual_model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=10
        )
        
        result = response.choices[0].message.content.strip().upper()
        if "INVALID" in result:
            return False, "Invalid or unclear query. Please ask a meaningful question."
        return True, None
    except:
        # Fallback to basic keyword check if AI validation fails
        keywords = ["sales", "revenue", "count", "top", "highest", "average", "avg", "total", "category", "location"]
        if any(w in query.lower() for w in keywords):
            return True, None
        return False, "Unclear query. Please rephrase with more business context."

def generate_sql(nl_query, table_name="ecommerce_behavior", columns=None):
    if columns is None:
        columns = DEFAULT_COLUMNS
    
    # Step -1: Input Validation Layer (AI-Driven Pre-Check)
    is_valid, error_msg = validate_query(nl_query)
    if not is_valid:
        logger.warning(f"BLOCKED: Invalid query detected: {nl_query}")
        return None, nl_query, error_msg # Return None as SQL to trigger error in app.py
    
    # Step 0: Apply Semantic Layer
    enhanced_query = apply_semantic_layer(nl_query)
    
    # Step 0.5: Detect Intents (Additive Enhancement)
    detected_intents = detect_intents(enhanced_query)
    intents_str = ", ".join(detected_intents)
    
    # Step 0.6: Extract Column Hints (Additive Enhancement)
    # Using nl_query (original) to catch synonyms accurately
    column_hints = extract_column_hints(nl_query, COLUMN_SYNONYMS)
    hints_str = json.dumps(column_hints) if column_hints else "None"
    
    # Step 0.7: Detect Top-K Preference (Additive Enhancement)
    top_k = extract_top_k(nl_query)
    top_k_str = str(top_k) if top_k else "None"
    
    # Step 1: AI-based SQL generation
    try:
        columns_str = ", ".join(columns)
        prompt = f"""
You are an expert SQL generator for a Business Insights Dashboard.
Your goal is to convert the user's natural language question into a valid, optimized PostgreSQL query for the table: {table_name}.

--- TABLE SCHEMA ---
- Customer_ID (INT)
- Age (INT)
- Gender (TEXT)
- Income_Level (TEXT)
- Marital_Status (TEXT)
- Education_Level (TEXT)
- Occupation (TEXT)
- Location (TEXT)
- Purchase_Category (TEXT)
- Purchase_Amount (FLOAT) -- This is the 'Sales' or 'Revenue' or 'Spending'
- Frequency_of_Purchase (TEXT)
- Purchase_Channel (TEXT)
- Brand_Loyalty (TEXT)
- Product_Rating (FLOAT)
- Time_Spent_on_Product_Researchhours (FLOAT)
- Social_Media_Influence (TEXT)
- Discount_Sensitivity (TEXT)
- Return_Rate (FLOAT)
- Customer_Satisfaction (INT)
- Engagement_with_Ads (TEXT)
- Device_Used_for_Shopping (TEXT)
- Payment_Method (TEXT)
- Time_of_Purchase (TEXT)
- Discount_Used (TEXT) -- Yes/No
- Customer_Loyalty_Program_Member (TEXT) -- Yes/No
- Purchase_Intent (TEXT)
- Shipping_Preference (TEXT)
- Time_to_Decision (FLOAT)

--- SEMANTIC GUIDANCE (CONTEXT) ---
1. DETECTED INTENTS: {intents_str}
2. COLUMN HINTS: {hints_str} (Mapping of user terms to actual DB columns)
3. TOP-K PREFERENCE: {top_k_str} (Apply LIMIT N if specified or ranking detected)

--- GENERATION RULES ---
1. GENERAL:
   - Use ONLY these SQL functions: COUNT, SUM, AVG, MAX, MIN.
   - Return ONLY the SQL query. No explanation, no markdown blocks.

2. METRIC SELECTION:
   - "Sales", "Revenue", "Earnings", "Total Amount" -> Use SUM(purchase_amount)
   - "Count", "Number", "How many", "Orders", "Transactions" -> Use COUNT(*)
   - "Average", "Mean", "Per User" -> Use AVG(purchase_amount)

3. PERCENTAGE / RATIO calculations (CRITICAL):
   - DO NOT use COUNT(condition).
   - ALWAYS use: SUM(CASE WHEN <condition> THEN 1 ELSE 0 END)
   - PERCENTAGE: (SUM(CASE WHEN <condition> THEN 1 ELSE 0 END) * 100.0) / NULLIF(COUNT(*), 0)
   - RATIO: (SUM(CASE WHEN <condition> THEN 1 ELSE 0 END) * 1.0) / NULLIF(COUNT(*), 0)
   - ALWAYS use "100.0" or "1.0" for floating point division.

4. CATEGORICAL FILTERS:
   - Always use LOWER(TRIM(COALESCE(column_name, ''))) = 'lowercase_val'.
   - For Yes/No columns like 'Discount_Used', use: LOWER(TRIM(COALESCE(discount_used, ''))) = 'yes'.

5. TOP-K / LIMIT:
   - If 'Top-K Preference' is a number, apply LIMIT accordingly.
   - If user asks for "highest", "best", "top", always include ORDER BY and LIMIT.
   - Otherwise, DO NOT apply LIMIT unless explicitly asked for a subset.

--- USER QUESTION ---
{enhanced_query}
"""

        query_hash = hashlib.md5(f"{table_name}:{prompt}".encode()).hexdigest()
        file_cache = load_file_cache()
        if query_hash in file_cache:
            cached_val = file_cache[query_hash]
            if isinstance(cached_val, dict):
                return cached_val["sql"], enhanced_query, cached_val["chart"]
            return cached_val, enhanced_query, "bar" # Backward compatibility for old cache

        actual_model = MODEL_MAPPING.get("groq-1", "groq-1")
        response = client.chat.completions.create(
            model=actual_model,
            messages=[
                {"role": "system", "content": "You are a SQL expert. Return only the SQL query, no explanation."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=500
        )

        if response and response.choices[0].message.content:
            sql_text = response.choices[0].message.content.strip()
            sql_text = sql_text.replace("```sql", "").replace("```", "").strip()
            sql_text = fix_sql_type_casts(sql_text)
            
            print(f"Final SQL: {sql_text}")
            logger.info(f"AI SQL Generation complete. Length: {len(sql_text)}")
            
            # Step 1.5: Chart Suggestion (Additive Enhancement)
            suggested_chart = suggest_chart_type(nl_query, detected_intents, sql_text)
            
            file_cache[query_hash] = {"sql": sql_text, "chart": suggested_chart}
            save_file_cache(file_cache)
            return sql_text, enhanced_query, suggested_chart
        else:
            raise Exception("AI returned empty content")
            
    except Exception as e:
        print(f"⚠️ AI Generation Error: {str(e)}")
        # Step 2: Fallback Layer
        sql_fallback = get_fallback_sql(nl_query, table_name)
        # Attempt minimal chart detection for fallback
        fallback_chart = suggest_chart_type(nl_query, ["general"], sql_fallback or "")
        return sql_fallback, enhanced_query, fallback_chart

