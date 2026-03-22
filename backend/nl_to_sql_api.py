print("USING nl_to_sql_api.py")
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
from schema_config import SCHEMA, get_metric_col, get_category_col, get_location_col, get_date_col, get_table_name, get_active_schema, get_active_mapping, refine_mapping_with_synonyms

# Initialize Groq client
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

# Configure Logger for this module
logger = logging.getLogger(__name__)

def match_column(user_term, columns):
    """
    Safely matches a user term to an actual column in the dataset schema.
    Returns the column name in LOWERCASE to match PostgreSQL standards.
    """
    if not user_term or not columns:
        return None
    user_term = user_term.lower()
    for col in columns:
        if user_term in col.lower():
            return col.lower() # FORCE LOWERCASE
    return None

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

# Legacy Fallback Definitions (Used if SCHEMA is missing or for backward compatibility)
DEFAULT_COLUMNS = SCHEMA.get("all_columns", [
    "customer_id", "age", "gender", "income_level", "marital_status", "education_level",
    "occupation", "location", "purchase_category", "purchase_amount",
    "frequency_of_purchase", "purchase_channel", "brand_loyalty", "product_rating",
    "time_spent_on_product_researchhours", "social_media_influence", "discount_sensitivity",
    "return_rate", "customer_satisfaction", "engagement_with_ads", "device_used_for_shopping",
    "payment_method", "time_of_purchase", "discount_used", "customer_loyalty_program_member",
    "purchase_intent", "shipping_preference", "time_to_decision"
])

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

NUMERIC_COLUMNS = set(SCHEMA.get("numeric_columns", [
    "purchase_amount", "age", "product_rating", "time_spent_on_product_researchhours",
    "return_rate", "customer_satisfaction", "time_to_decision",
]))

CATEGORICAL_COLUMNS = set(SCHEMA.get("categorical_columns", [
    "income_level", "frequency_of_purchase", "discount_sensitivity", "brand_loyalty",
    "social_media_influence", "engagement_with_ads", "purchase_intent", "discount_used",
    "customer_loyalty_program_member", "gender", "marital_status", "education_level",
    "occupation", "location", "purchase_category", "purchase_channel",
    "device_used_for_shopping", "payment_method", "shipping_preference"
]))

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
    Supports multiple intents per query. Returns empty list if no clear intent found.
    """
    q = query.lower()
    intents = []
    
    # 1. Ranking Intent
    if any(word in q for word in ["highest", "lowest", "top", "bottom", "rank", "peak", "best", "worst"]):
        intents.append("ranking")
        
    # 2. Aggregation Intent
    if any(word in q for word in ["average", "avg", "mean", "sum", "total", "revenue", "sales", "earnings", "count", "number of", "how many"]):
        intents.append("aggregation")
        
    # 3. Ratio/Percentage Intent
    if any(word in q for word in ["percentage", "ratio", "proportion", "%", "fraction"]):
        intents.append("ratio")
        
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

# --- Dynamic SQL Generation Engine ---
def generate_dynamic_sql(query, schema, mapping, table_name):
    """
    Programmatically constructs SQL based on schema mapping for standard intents.
    Enforces strict column validation and case-safe naming to prevent SQL errors.
    """
    if not schema or not mapping:
        return None, None

    q = query.lower()
    cols = schema.get("columns", [])
    numeric_cols = schema.get("numeric", [])
    categorical_cols = schema.get("categorical", [])
    
    # --- PHASE 1: Validate & Fallback columns ---
    # We ensure measure, group, and discount roles point to REAL columns in the dataset.
    
    # 1. Measure (Numerical)
    measure = mapping.get("measure")
    if not measure or measure not in cols:
        measure = numeric_cols[0] if numeric_cols else None
        
    # 2. Group (Categorical)
    group = mapping.get("group")
    if not group or group not in cols:
        group = categorical_cols[0] if categorical_cols else None
        
    # 3. Discount (Filter/Flag)
    discount = mapping.get("discount")
    if discount and discount not in cols:
        discount = None 

    # --- PHASE 1.5: Strict Validation & Fallback ---
    # Return user-friendly error if we cannot find columns to fulfill the query
    if not measure or measure not in cols:
        available_cols = ", ".join(cols[:10])
        return f"Error: Could not find a suitable numeric column. Available columns: [{available_cols}]", None
    
    if not group or group not in cols:
        available_cols = ", ".join(cols[:10])
        return f"Error: Could not find a suitable grouping column. Available columns: [{available_cols}]", None

    # --- PHASE 2: Build Intent Logic ---
    is_count = any(word in q for word in ["count", "number of", "how many"])
    is_total = any(word in q for word in ["total", "sum", "revenue", "sales", "spending", "amount"])
    is_avg = any(word in q for word in ["average", "avg", "mean"])
    grouping_keywords = ["by ", "per ", "breakdown", "distribution", "split by"]
    is_grouped = any(word in q for word in grouping_keywords) or (group.lower() in q if group else False)
    
    # --- PHASE 3: Calculate Metrics (Forced Lowercase) ---
    chart = "bar"
    is_percentage = "percentage" in q or "percent" in q or "%" in q
    
    if is_percentage and discount:
        metric_sql = f"(SUM(CASE WHEN LOWER(TRIM({discount}::TEXT)) IN ('yes', 'true', '1') THEN 1 ELSE 0 END) * 100.0) / NULLIF(COUNT(*), 0)"
        alias = "percentage_share"
        chart = "pie"
    else:
        metric_sql = f"COUNT(*)" if is_count else f"SUM({measure}::NUMERIC)" if is_total else f"AVG({measure}::NUMERIC)" if is_avg else f"SUM({measure}::NUMERIC)"
        alias = "total_count" if is_count else "total_value"
    
    # --- PHASE 4: Construct Query (Forced Lowercase - NO QUOTES) ---
    if is_grouped and group:
        sql = f"SELECT {group}, {metric_sql} AS {alias} FROM {table_name} GROUP BY {group} ORDER BY {alias} DESC"
        chart = "bar" if not is_percentage else "pie"
    else:
        sql = f"SELECT {metric_sql} AS {alias} FROM {table_name}"
        chart = "kpi" if not is_percentage else "pie"
        
    # Apply optional filters
    if not is_percentage and "discount" in q and discount:
        where_clause = f" WHERE LOWER(TRIM({discount}::TEXT)) IN ('yes', 'true', '1')"
        if "GROUP BY" in sql:
            sql = sql.replace(" GROUP BY", f"{where_clause} GROUP BY")
        else:
            sql += where_clause
            
    final_sql = sql.strip() + ";"
    
    # --- MANDATORY DEBUG LOGGING (Requirement: REVISED LABELS) ---
    print("DB COLUMNS:", [c.lower() for c in cols])
    print("FINAL GROUP:", group)
    print("FINAL MEASURE:", measure)
    print("FINAL SQL:", final_sql)
    
    return final_sql, chart

def get_fallback_sql(user_query, table_name=None):
    """
    Returns a predefined SQL query based on keywords when AI fails.
    Highly decoupled using schema abstraction.
    """
    if not table_name:
        table_name = get_table_name()
        
    q = user_query.lower()
    logger.info(f"Fallback logic triggered for query: {user_query}")
    print(f"🔄 Applying fallback logic for: '{user_query}'")
    
    # Decoupled Schema References
    metric_col = get_metric_col()
    category_col = get_category_col()
    location_col = get_location_col()
    
    # Intent Mapping
    is_top = any(word in q for word in ["highest", "top", "best", "rank"])
    is_revenue = any(word in q for word in ["sales", "revenue", "earnings", "income", "total amount"])
    is_count = any(word in q for word in ["count", "number", "how many", "users", "orders"])
    is_avg = any(word in q for word in ["average", "avg", "mean", "per user"])

    # Force lowercase for PostgreSQL standards
    metric_col = metric_col.lower() if metric_col else ""
    category_col = category_col.lower() if category_col else ""
    location_col = location_col.lower() if location_col else ""

    metric_sql = "COUNT(*)" if is_count else f"SUM({metric_col}::NUMERIC)" if is_revenue else f"AVG({metric_col}::NUMERIC)" if is_avg else "*"
    alias = "total_count" if is_count else "total_revenue" if is_revenue else "average_spend" if is_avg else "records"

    if "category" in q or category_col in q:
        sql = f"SELECT {category_col}, {metric_sql} AS {alias} FROM {table_name} GROUP BY {category_col}"
        if is_top: sql += f" ORDER BY {alias} DESC LIMIT 1"
        return sql + ";"

    if "location" in q or "city" in q or location_col in q:
        sql = f"SELECT {location_col}, {metric_sql} AS {alias} FROM {table_name} GROUP BY {location_col}"
        if is_top: sql += f" ORDER BY {alias} DESC LIMIT 1"
        return sql + ";"

    if is_revenue:
        return f"SELECT SUM({metric_col}::NUMERIC) AS total_revenue FROM {table_name};"
    elif is_count:
        return f"SELECT COUNT(*) AS total_count FROM {table_name};"
    elif is_avg:
        return f"SELECT AVG({metric_col}::NUMERIC) AS average_spend FROM {table_name};"
    
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
    intents_str = ", ".join(detected_intents) if detected_intents else "general"
    
    # Step 0.6: Extract Column Hints (Additive Enhancement)
    # Using nl_query (original) to catch synonyms accurately
    column_hints = extract_column_hints(nl_query, COLUMN_SYNONYMS)
    hints_str = json.dumps(column_hints) if column_hints else "None"
    
    # --- Step 0.7: Unknown Query Handling (New Safety Layer) ---
    active_schema = get_active_schema()
    active_mapping = get_active_mapping()
    active_mapping = refine_mapping_with_synonyms(nl_query, active_mapping)
    
    # Check if we can proceed with any form of generation
    # We check both the original AND enhanced query for safety
    safe_keywords = ["total", "sum", "avg", "count", "list", "show", "purchase_amount", "location", "category", "rating", "satisfaction"]
    has_basic_intent = any(k in nl_query.lower() or k in enhanced_query.lower() for k in safe_keywords)
    
    if not detected_intents and not has_basic_intent:
        # If mapping is also weak or empty, it's garbage.
        if not active_mapping or not active_mapping.get("measure") or active_mapping.get("measure") == SCHEMA.get("metric_column"):
             # Weak evidence of intent -> Block nonsense
             logger.warning(f"Unknown query blocked: {nl_query}")
             return None, nl_query, "Sorry, I couldn't understand the query. Try rephrasing with more business details."

    # Step 0.8: Detect Top-K Preference (Additive Enhancement)
    top_k = extract_top_k(nl_query)
    top_k_str = str(top_k) if top_k else "None"
    
    # --- Step 0.8: Dynamic SQL Generation Layer (New High-Priority Tier) ---
    active_schema = get_active_schema()
    active_mapping = get_active_mapping()
    
    # Contextualize mapping based on query synonyms
    active_mapping = refine_mapping_with_synonyms(nl_query, active_mapping)
    print("Column Mapping:", active_mapping)
    
    # --- Step 0.8: Safe Fallback (Guaranteed Analytical Roles) ---
    if not active_mapping.get("measure"):
        active_mapping["measure"] = active_schema.get("numeric", [None])[0]
    if not active_mapping.get("group"):
        active_mapping["group"] = active_schema.get("categorical", [None])[0]
    
    if active_mapping["measure"]: print(f"Fallback Applied: Measure -> {active_mapping['measure']}")
    if active_mapping["group"]: print(f"Fallback Applied: Group -> {active_mapping['group']}")
    
    # Try dynamic generation for standard analytical intents
    dynamic_sql, dynamic_chart = generate_dynamic_sql(enhanced_query, active_schema, active_mapping, table_name)
    
    if dynamic_sql:
        if dynamic_sql.startswith("Error:"):
            logger.warning(f"Dynamic SQL Generator failed validation: {dynamic_sql}")
            return None, nl_query, dynamic_sql # Return None as SQL to trigger clean error in app.py
        logger.info(f"Dynamic SQL Generator successful for: {nl_query}")
        return dynamic_sql, enhanced_query, dynamic_chart

    # Step 1: AI-based SQL generation (LLM Fallback/Primary for Complex Queries)
    try:
        columns_str = ", ".join(columns)
        prompt = f"""
You are an expert SQL generator for a Business Insights Dashboard.
Your goal is to convert the user's natural language question into a valid, optimized PostgreSQL query for the table: {table_name}.

--- TABLE SCHEMA (STRICT) ---
The following columns are the ONLY ones available in the database table '{table_name}'.
{chr(10).join([f"- {col} ({'NUMERIC' if col in active_schema.get('numeric', []) else 'TEXT'})" for col in active_schema.get('columns', [])])}

⚠️ SCHEMA INTEGRITY RULE:
- Use ONLY the columns listed above. 
- DO NOT guess, invent, or assume column names (like 'product_id', 'revenue', etc.) if they are not explicitly listed in the schema.
- If a requested metric or dimension is missing, use the most relevant column from the schema or the provided COLUMN HINTS.

--- SEMANTIC GUIDANCE (CONTEXT) ---
1. DETECTED INTENTS: {intents_str}
2. COLUMN HINTS: {hints_str} (Mapping of user terms to actual DB columns)
3. TOP-K PREFERENCE: {top_k_str} (Apply LIMIT N if specified or ranking detected)

--- GENERATION RULES ---
1. GENERAL:
   - Use ONLY these SQL functions: COUNT, SUM, AVG, MAX, MIN.
   - Return ONLY the SQL query. No explanation, no markdown blocks.

2. METRIC SELECTION:
   - "Sales", "Revenue", "Earnings", "Total Amount" -> Use SUM({active_mapping.get('measure', 'value')}::NUMERIC)
   - "Count", "Number", "How many", "Orders", "Transactions" -> Use COUNT(*)
   - "Average", "Mean", "Per User" -> Use AVG({active_mapping.get('measure', 'value')}::NUMERIC)

3. PERCENTAGE / RATIO calculations (CRITICAL):
   - DO NOT use COUNT(condition).
   - ALWAYS use: SUM(CASE WHEN <condition> THEN 1 ELSE 0 END)
   - PERCENTAGE: (SUM(CASE WHEN <condition> THEN 1 ELSE 0 END) * 100.0) / NULLIF(COUNT(*), 0)
   - RATIO: (SUM(CASE WHEN <condition> THEN 1 ELSE 0 END) * 1.0) / NULLIF(COUNT(*), 0)
   - ALWAYS use "100.0" or "1.0" for floating point division.

4. CATEGORICAL FILTERS:
   - Always use LOWER(TRIM(COALESCE(column_name, ''))) = 'lowercase_val'.
   - For filter/discount columns, e.g. '{active_mapping.get('discount', 'discount_used')}', use: LOWER(TRIM(COALESCE({active_mapping.get('discount', 'discount_used')}, ''))) = 'yes'.

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
            
            # --- MANDATORY DEBUG LOGS ---
            print("Schema Columns:", active_schema.get("columns", []))
            print("Mapping:", active_mapping)
            print("Generated SQL:", sql_text)
            
            # --- Step 1.2: SQL Schema Auditor & Auto-Correction (New Safety Layer) ---
            # Automatically corrects known hallucinations based on active schema
            sql_upper = sql_text.upper()
            
            # Defensive Column Resolution
            suggested_group = active_mapping.get("group") or (active_schema.get("categorical", [None])[0]) or (active_schema.get("columns", ["column_1"])[0])
            suggested_measure = active_mapping.get("measure") or (active_schema.get("numeric", [None])[0]) or (active_schema.get("columns", ["column_1"])[0])
            
            hallucination_map = {
                "PRODUCT_ID": suggested_group,
                "PRODUCTID": suggested_group,
                "REVENUE": suggested_measure,
                "SALES": suggested_measure
            }
            
            for hallucination, correction in hallucination_map.items():
                if hallucination in sql_upper:
                    # Check if it was a real column (already case-insensitive)
                    existing_cols = [c.lower() for c in active_schema.get("columns", [])]
                    if hallucination.lower() not in existing_cols:
                        logger.warning(f"AUDIT FAIL: Auto-correcting hallucinated column '{hallucination}' in generated SQL.")
                        sql_text = re.sub(rf'(?i)\b{hallucination}\b', str(correction), sql_text)
            
            print("Final SQL Query:", sql_text)
            
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

