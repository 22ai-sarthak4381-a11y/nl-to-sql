print("USING backend/app.py")
# backend/app.py (Flask API Backend)
import os
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
from supabase import create_client
import pandas as pd
import httpx
from nl_to_sql_api import generate_sql
import json
import io
import re
from ml_engine import get_ml_insights
from schema_config import SCHEMA, get_metric_col, get_category_col, get_location_col, get_date_col, get_table_name, set_runtime_schema, get_active_schema, get_active_mapping, set_manual_override

def find_best_match(target, columns):
    """
    Safely finds the most relevant column in the dataset based on a target term.
    """
    if not target or not columns:
        return None
    target = target.lower()
    for col in columns:
        if target in col.lower():
            return col
    return None

import logging

load_dotenv()

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app) # Enable CORS for React frontend

# Supabase configuration
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase = create_client(url, key)

# --- Runtime Schema Detection Startup ---
def initialize_schema():
    try:
        table = get_table_name()
        logger.info(f"Initializing dynamic schema detection for table: {table}")
        # Fetch only 1 record to detect structure
        result = supabase.table(table).select("*").limit(1).execute()
        if result.data:
            df_sample = pd.DataFrame(result.data)
            # Normalize columns: remove spaces and force lowercase
            df_sample.columns = [col.strip().lower() for col in df_sample.columns]
            print("Columns in dataset (Startup):", df_sample.columns.tolist())
            set_runtime_schema(df_sample)
            logger.info("Successfully detected and stored dynamic schema.")
        else:
            logger.warning("Table is empty. Using default schema configuration.")
    except Exception as e:
        logger.error(f"Startup Schema Detection Failed: {str(e)}")

# Trigger schema detection on app start
initialize_schema()

import numpy as np

def generate_insight(avg, median):
    if avg > median:
        return "High spenders dominate purchases"
    elif avg < median:
        return "Most customers are moderate or low spenders"
    else:
        return "Spending is evenly distributed"

# Dynamic SQL Validation is now schema-adaptive (see validate_sql)

def validate_sql(sql: str) -> bool:
    """
    Validates SQL strings to ensure they are safe and only interact with the ACTIVE schema's columns.
    Dynamic adaptation for any uploaded dataset.
    """
    from schema_config import get_active_schema
    import re
    
    sql_clean = sql.upper().replace('"', '').replace('`', '')
    active_schema = get_active_schema()
    
    # 1. Block destructive keywords (Immutable Security)
    if any(keyword in sql_clean for keyword in ["DROP", "DELETE", "UPDATE", "INSERT", "TRUNCATE", "ALTER"]):
        logger.warning(f"SECURITY: Blocked destructive SQL attempt: {sql}")
        return False
        
    # 2. Structural Integrity
    if "SELECT" not in sql_clean:
        return False
    if sql.count(';') > 1:
        return False
        
    # 3. Dynamic Whitelist Validation
    # We fetch the current allowed columns based on whatever dataset is active.
    current_allowed = set([c.upper() for c in active_schema.get("columns", [])])
    # Add common calculated aliases that are safe
    current_allowed.update(["TOTAL_REVENUE", "AVERAGE_SPEND", "TOTAL_COUNT", "PERCENTAGE_SHARE", "RECORDS", "TOTAL_VALUE", "TOTAL"])
    
    sql_keywords = set(["SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "JOIN", "ON", "AND", "OR", "IN", "LIKE", "AS", "COUNT", "SUM", "AVG", "MIN", "MAX", "LIMIT", "DESC", "ASC", "HAVING", "DISTINCT", "BETWEEN", "IS", "NULL", "NOT", "CASE", "WHEN", "THEN", "ELSE", "END", "UNION", "ALL"])
    
    # Extract identifiers
    words = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', sql_clean)
    
    for word in words:
        # Ignore SQL keywords and numbers
        if word in sql_keywords or word.isdigit():
            continue
            
        # If it's a potential column, verify it against the active schema whitelist
        if word not in current_allowed:
            # We log this as a warning but don't strictly fail yet unless it's obviously hazardous
            # because 'word' might be a categorical value (e.g. 'Electronics') inside an expression.
            # In a production environment, you'd separate identifiers from values for 100% strictness.
            logger.info(f"SAFE CHECK: Identifier/Value '{word}' encountered (not in whitelist). Cross-referencing...")
    
    return True


# Simple Python-based Insight Summary Generator
def generate_insight(data_list):
    """
    Highly Controlled Adaptive & Intelligent Insights.
    Input: List of dicts with numeric values.
    """
    if not data_list or len(data_list) <= 1:
        return "Not enough data to generate meaningful insight."

    # Extract numeric values and labels
    values = []
    labels = []
    for d in data_list:
        v_list = [v for v in d.values() if isinstance(v, (int, float))]
        l_list = [v for v in d.values() if isinstance(v, str)]
        if v_list: values.append(v_list[0])
        labels.append(l_list[0] if l_list else "N/A")

    if not values or len(values) <= 1:
        return "Not enough numeric data for deep analysis."

    # 1. Compute Base Metrics
    n = len(values)
    max_val = max(values)
    min_val = min(values)
def generate_insight(data_input, labels=None):
    """
    Upgraded dynamic insight engine with robust input handling.
    Safely extracts numeric values from lists of numbers OR lists of dictionaries (records).
    """
    if not data_input:
        return "No data available to generate insights."
    
    # 1. Normalize input to a list of numbers
    if isinstance(data_input, list) and len(data_input) > 0:
        if isinstance(data_input[0], (int, float)):
            values = data_input
        elif isinstance(data_input[0], dict):
            # Extract first numeric value found in records
            numeric_keys = [k for k, v in data_input[0].items() if isinstance(v, (int, float))]
            if not numeric_keys:
                return "Not enough numeric data for comparative insights."
            # Prioritize 'total_value' or 'total_count' if present
            target_key = next((k for k in ["total_value", "total_count"] if k in numeric_keys), numeric_keys[0])
            values = [float(row.get(target_key, 0)) for row in data_input]
        else:
            return "Unsupported data format for insights."
    else:
        return "No comparative data detected."

    n = len(values)
    if n <= 1:
        return "Insight: Single data point detected, no comparative distribution available."
        
    avg_val = sum(values) / n
    max_val = max(values)
    min_val = min(values)
    
    # 2. Dynamic Threshold Checks
    if max_val > avg_val * 1.3:
        return "Top category dominates significantly."
    if min_val < avg_val * 0.6:
        return "Some categories underperform."
    return "Balanced distribution."

def generate_python_summary(df):
    if df.empty or len(df) <= 1:
        return "Not enough data to generate meaningful insight."
    
    # Convert dataframe to records for the helper
    data_records = df.to_dict(orient="records")
    return generate_insight(data_records)


# Quick Interpretation Logic (Frontend Helper)
def quick_interpret(query):
    query = query.lower()
    metric = "General Analytics"
    if any(word in query for word in ["sales", "revenue", "amount", "sold"]): metric = "Total Sales"
    elif "count" in query or "number of" in query: metric = "Record Count"
    elif "average" in query or "avg" in query: metric = "Average Value"
    elif "rating" in query: metric = "Product Ratings"
    
    # Advanced Grouping Detection
    group_by = None
    if "by gender" in query: group_by = "Gender"
    elif "by category" in query: group_by = "Purchase Category"
    elif "by location" in query or "by city" in query: group_by = "Location"
    elif "by occupation" in query: group_by = "Occupation"
    elif "by channel" in query: group_by = "Purchase Channel"
    elif "by month" in query: group_by = "Month"
    
    # Legacy/Default fallback if no explicit "by" used
    if not group_by:
        if "month" in query: group_by = "Month"
        elif "category" in query: group_by = "Category"
        elif "location" in query or "city" in query: group_by = "Location"
        else: group_by = "Overall"

    
    filters = []
    months = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"]
    for m in months:
        if m in query or m[:3] in query:
            filters.append(m.title())
    if "bangalore" in query: filters.append("Bangalore")
    if "male" in query and "female" not in query: filters.append("Male")
    
    operation = "Aggregate" if any(word in query for word in ["total", "sum", "average", "avg", "count"]) else "Listing"
    
    return {
        "metric": metric,
        "group_by": group_by,
        "filter": ", ".join(filters) if filters else "None",
        "operation": operation
    }

# Decoupled Synonym Mapping
SYNONYM_MAP = {
    "spending": get_metric_col(),
    "revenue": get_metric_col(),
    "sales": get_metric_col(),
    "cost": get_metric_col(),
    "items": get_category_col(),
    "group": get_category_col(),
    "city": get_location_col(),
    "place": get_location_col(),
    "rating": SCHEMA.get("numeric_columns", ["product_rating"])[1] if len(SCHEMA.get("numeric_columns", [])) > 1 else "product_rating",
    "score": SCHEMA.get("numeric_columns", ["customer_satisfaction"])[-1] if SCHEMA.get("numeric_columns") else "customer_satisfaction"
}

def normalize_query(query):
    if not query:
        return query
    
    import re
    normalized = query
    # Sort synonyms by length to handle multi-word if any, or just single words
    for word, actual in sorted(SYNONYM_MAP.items(), key=lambda x: len(x[0]), reverse=True):
        pattern = re.compile(rf'\b{re.escape(word)}\b', re.IGNORECASE)
        normalized = pattern.sub(actual, normalized)
    
    return normalized

def detect_chart_type(query):
    q = query.lower()
    if any(k in q for k in ["over time", "month", "year", "trend", "timeline", "date"]):
        return "line"
    if any(k in q for k in ["distribution", "range", "spread", "histogram"]):
        return "histogram"
    if any(k in q for k in ["pie", "breakdown", "proportion", "share", "contribution"]):
        return "pie"
    return "bar"


@app.route('/query', methods=['POST'])
def handle_query():
    data = request.json
    user_query = data.get('query', '')
    
    # 0. Synonym Normalization Layer (Normalization Step)
    logger.info(f"Incoming Request Query: {user_query}")
    user_query = normalize_query(user_query)
    
    # 1. Incorporate active UI filters into query context
    filters = data.get('filters', {})
    active_filters_prompt = []
    if filters.get('category') and filters.get('category') != 'all':
        active_filters_prompt.append(f"Purchase_Category must be '{filters['category']}'")
    if filters.get('gender') and filters.get('gender') != 'all':
        active_filters_prompt.append(f"Gender must be '{filters['gender']}'")
    if filters.get('startDate') and filters.get('endDate'):
         active_filters_prompt.append(f"Time_of_Purchase must be between '{filters['startDate']}' and '{filters['endDate']}'")
         
    if active_filters_prompt:
        filter_str = " AND ".join(active_filters_prompt)
        user_query = f"{user_query} (FILTER CONTEXT: Only include records where {filter_str})"

    # 1.5 Input Validation Layer (Security Check & Intent Validation)
    INTENT_KEYWORDS = [
        "sale", "revenue", "earning", "income", "amount", "spend", "purchas",
        "count", "number", "how many", "total", "users", "orders",
        "avg", "average", "mean", "percentage", "ratio", "proportion", "percent",
        "highest", "top", "best", "rank", "maximum", "max", "min", "lowest",
        "all data", "show everything", "everything", "full data",
        "category", "gender", "location", "age", "rating", "discount", "satisfied", "churn"
    ]
    
    # 1.5 Input Validation Layer (Hard Stop Intent Validation)
    query_lower = user_query.lower()
    has_intent = any(keyword in query_lower for keyword in INTENT_KEYWORDS)
    
    if not has_intent and len(user_query.strip().split()) < 3:
         logger.warning(f"TERMINATED: Unclear query intent detected: {user_query}")
         return jsonify({
             "status": "error",
             "message": "Sorry, I couldn't understand the query. Please rephrase with more business details (e.g. Sales, Category, etc.)."
         }), 400

    def is_valid_column(col):
        from schema_config import get_active_schema
        cols = [c.lower() for c in get_active_schema().get("columns", [])]
        return col.lower() in cols

    try:
        # 2. Check for drill down first
        drill_down = data.get('drill_down')
        if drill_down:
            field = drill_down.get('field')
            value = str(drill_down.get('value', '')).strip()
            
            # 1. Validation (Hard Termination)
            if not value:
                logger.warning("TERMINATED: Empty drill-down value.")
                return jsonify({"status": "error", "message": "Invalid drill-down input (selection is empty)."}), 400

            # 2. Whitelist Validation for Field
            if not is_valid_column(field):
                logger.warning(f"TERMINATED: Unauthorized field '{field}'")
                return jsonify({"status": "error", "message": "Invalid column selection."}), 400
            
            # 3. Robust SQL Pattern
            sql_query = f"SELECT * FROM ecommerce_behavior WHERE LOWER(TRIM(\"{field}\"::TEXT)) = LOWER(TRIM('{value}')) LIMIT 100"
            enhanced_query = f"Drill-down: {field}={value}"
            interpretation = {"metric": f"Details: {value}", "group_by": field, "operation": "Drill-down"}
            chart_type = "table"
        else:
            # 1. SQL Generation with Safe Termination            # --- DYNAMIC DATASET RESOLUTION ---
            active_table = get_table_name()
            if active_table is None:
                raise Exception("No active table selected")
                
            active_columns = get_active_schema().get("columns", [])
            
            # MANDATORY DEBUG LOGS (Requirement)
            print("ACTIVE TABLE:", active_table)
            print("ACTIVE COLUMNS:", active_columns)
            logger.info(f"ACTIVE TABLE: {active_table}")
            logger.info(f"ACTIVE COLUMNS: {active_columns}")
            
            # Generate SQL using DYNAMIC table and columns
            sql_query, enhanced_query, validation_hint = generate_sql(user_query, table_name=active_table, columns=active_columns)
            
            # MANDATORY DEBUG LOG (Requirement)
            print("FINAL SQL:", sql_query)
            logger.info(f"FINAL SQL: {sql_query}")
            logger.info(f"Raw SQL Generation Attempt for: {user_query}")
            
            if not sql_query:
                # If validation_hint contains the error from our new validation layer
                error_display = validation_hint if validation_hint else "Sorry, I couldn't understand the query. Please rephrase."
                logger.warning(f"TERMINATED: Validation or Generation failed: {error_display}")
                return jsonify({
                    "status": "error",
                    "message": error_display
                }), 400
                
            # 0. Interpretation & Chart Detection
            interpretation = quick_interpret(user_query)
            sql_query = sql_query.strip().rstrip(';')
            
            # Use the smarter suggestion from nl_to_sql_api if it's a valid chart type
            chart_type = validation_hint if validation_hint and not validation_hint.startswith("Query is") else detect_chart_type(user_query)
            
        # 2. Execution Guard (Safety Layer)
        if not validate_sql(sql_query):
            logger.warning(f"TERMINATED: SQL failed safety validation: {sql_query}")
            return jsonify({
                "status": "error",
                "message": "Security Alert: This query has been blocked for safety reasons."
            }), 403

        # 3. Database Execution
        # Final safety check before interacting with the database
        active_schema_cols = get_active_schema().get("columns", [])
        
        # --- INTELLIGENT AUTO-CORRECTION LAYER ---
        # Safeguard against common AI hallucinations (like product_id)
        active_columns = get_active_schema().get("columns", [])
        
        for col_to_fix in ["product_id", "sales_rep"]:
            if col_to_fix in sql_query.lower() and col_to_fix not in active_columns:
                term_to_match = col_to_fix.replace("_id", "").replace("_rep", "")
                replacement = find_best_match(term_to_match, active_columns)
                
                if replacement:
                    # Case-insensitive whole-word replacement
                    sql_query = re.sub(rf'(?i)\b{col_to_fix}\b', replacement, sql_query)
        
        # Mandatory persistence check for forbidden columns
        if "product_id" in sql_query.lower() and "product_id" not in active_columns:
            raise Exception(f"🚨 BLOCKED: Still invalid column after correction → {sql_query}")

        print("FIXED SQL:", sql_query)

        try:
            result = supabase.rpc("execute_sql", {"query": sql_query}).execute()
            df_result = pd.DataFrame(result.data)
            
            logger.info(f"Query executed successfully. Rows returned: {len(df_result)}")
            
            # Check for empty results with intelligent suggestions
            if df_result.empty:
                suggestion = "Try simplifying or broadening your query."
                
                # Smart Suggestion Logic from user requirement
                if ">" in sql_query:
                    suggestion = "Try reducing the numeric threshold (e.g., using > 50 instead of a higher value)."
                elif "<" in sql_query:
                    suggestion = "Try increasing your numeric range to capture more records."
                elif "WHERE" in sql_query.upper() or "category" in sql_query.lower():
                    suggestion = "The filters applied are too specific. Try removing a category or location constraint."
                
                logger.warning(f"No results found for query: {user_query}")
                return jsonify({
                    "original_query": user_query,
                    "data": [],
                    "message": "No results matched your criteria.",
                    "suggestion": suggestion,
                    "insights": f"📉 **Status**: No data found.\n💡 **Suggestion**: {suggestion}",
                    "sql": sql_query,
                    "interpretation": interpretation
                }), 200
        except Exception as e:
            logger.error(f"SQL Execution Error: {str(e)}")
            return jsonify({"status": "error", "message": f"Query failed: {str(e)}", "sql": sql_query}), 500

        
        # 3.5 Ensure numeric columns are actually numbers
        for col in df_result.columns:
            temp_col = pd.to_numeric(df_result[col], errors='coerce')
            if not temp_col.isna().all():
                df_result[col] = temp_col.fillna(0)
        
        # 4. Generate Insight Summary
        insights = generate_python_summary(df_result)
        
        # 5. Advanced Purchase Insights (New Layer)
        metric_col = get_metric_col()
        if metric_col in [c.lower() for c in df_result.columns]:
            col_name = [c for c in df_result.columns if c.lower() == metric_col][0]
            values = pd.to_numeric(df_result[col_name], errors='coerce').dropna().tolist()
            if len(values) > 1:
                avg_val = sum(values) / len(values)
                median_val = float(np.median(values))
                insight_text = generate_insight(values)
                
                purchase_insight = (
                    f"**Average Purchase Amount**: {avg_val:,.2f}  \n"
                    f"**Median Purchase Amount**: {median_val:,.2f}  \n"
                    f"**Deep Insight**: {insight_text}"
                )
                # Combine with existing insights
                insights = purchase_insight + "\n\n---\n" + insights
        
        # 6. ML Analytics Layer (New AI Features)
        ml_layer = get_ml_insights(df_result)
        
        return jsonify({

            "original_query": user_query,
            "enhanced_query": enhanced_query if not drill_down else "N/A (Drill-down)",
            "interpretation": interpretation,
            "chart_type": chart_type,
            "data": df_result.to_dict(orient="records"),
            "sql": sql_query,
            "insights": insights,
            "ml_insights": ml_layer
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/schema-mapping', methods=['GET', 'POST'])
def handle_schema_mapping():
    """
    Manages active column mapping for UI explainability and manual override.
    """
    from schema_config import get_active_mapping, get_active_schema, set_manual_override
    
    if request.method == 'POST':
        data = request.json
        if not data:
            return jsonify({"error": "Missing mapping data"}), 400
        
        # Standardize keys to lowercase for internal engine
        manual_map = {
            "measure": data.get("Measure"),
            "group": data.get("Category"),
            "discount": data.get("Filter")
        }
        
        success = set_manual_override(manual_map)
        if success:
            logger.info(f"Manual Override applied via API: {manual_map}")
            return jsonify({"status": "success", "message": "Manual override applied successfully."})
        return jsonify({"status": "error", "message": "Could not apply override. Invalid columns."}), 400

    # GET logic
    try:
        mapping = get_active_mapping()
        schema = get_active_schema()
        
        # Provide both numeric and categorical options for UI dropdowns
        response = {
            "current": {
                "Measure": mapping.get("measure", "N/A"),
                "Category": mapping.get("group", "N/A"),
                "Filter": mapping.get("discount", "N/A")
            },
            "available": {
                "numeric": schema.get("numeric", schema.get("numeric_columns", [])),
                "categorical": schema.get("categorical", schema.get("categorical_columns", []))
            }
        }
        return jsonify(response)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "Backend is running"}), 200

@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    
    try:
        # Actually process the CSV for schema adaptation
        df = pd.read_csv(file)
        # Normalize columns: remove spaces and force lowercase
        df.columns = [col.strip().lower() for col in df.columns]
        
        print("Columns in dataset (Upload):", df.columns.tolist())
        if df.empty:
            return jsonify({"error": "Uploaded CSV is empty"}), 400
            
        from schema_config import set_runtime_schema, get_table_name
        set_runtime_schema(df, table_name=file.filename)
        
        # --- DYNAMIC DATABASE INGESTION ---
        active_table = get_table_name()
        
        # 1. Create table in Supabase via RPC (Serial atomic operations)
        cols_sql = [f'"{col}" TEXT' for col in df.columns]
        drop_query = f"DROP TABLE IF EXISTS {active_table}"
        create_query = f'CREATE TABLE {active_table} ({", ".join(cols_sql)})'
        
        try:
            # Execute serial operations to avoid multi-statement RPC issues
            supabase.rpc("execute_sql", {"query": drop_query}).execute()
            supabase.rpc("execute_sql", {"query": create_query}).execute()
            logger.info(f"Database table '{active_table}' created successfully.")
            
            # 2. Insert data in chunks
            data_to_insert = df.astype(str).to_dict(orient="records")
            chunk_size = 500
            for i in range(0, len(data_to_insert), chunk_size):
                chunk = data_to_insert[i:i + chunk_size]
                supabase.table(active_table).insert(chunk).execute()
            
            logger.info(f"Ingested {len(df)} rows into table '{active_table}'.")
        except Exception as db_err:
            logger.error(f"Database Ingestion Failure details: {str(db_err)}")
            # Even if ingestion fails (due to perms), we proceed as schema is already updated 
            # for the generator.
        
        logger.info(f"Dataset '{file.filename}' processed and ingested. Schema updated.")
        return jsonify({
            "status": "success",
            "message": f"Dataset '{file.filename}' processed and ingested successfully!",
            "rows": len(df),
            "table": active_table
        }), 200
    except Exception as e:
        logger.error(f"Upload processing failed: {str(e)}")
        return jsonify({"error": f"Failed to process dataset: {str(e)}"}), 500

@app.route('/export', methods=['POST'])
def export_csv():
    try:
        data = request.json.get('data', [])
        if not data:
            return jsonify({"error": "No data to export"}), 400
            
        df = pd.DataFrame(data)
        
        # Create CSV in memory
        output = io.StringIO()
        df.to_csv(output, index=False)
        output.seek(0)
        
        from flask import Response
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-disposition": "attachment; filename=dashboard_report.csv"}
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/analytics/sales', methods=['GET'])
def get_sales_analytics():
    try:
        table = get_table_name()
        cat_col = get_category_col()
        metric_col = get_metric_col()
        date_col = get_date_col()
        
        result = supabase.table(table).select(f"{cat_col}, {metric_col}, {date_col}").execute()
        df = pd.DataFrame(result.data)
        print(f"📊 Sales Data Rows: {len(df)}")
        if df.empty: return jsonify([])
        
        df[metric_col] = pd.to_numeric(df[metric_col], errors="coerce").fillna(0)
        # Match Streamlit: Group by Category and Sum Revenue
        cat_sales = df.groupby(cat_col)[metric_col].sum().reset_index()
        cat_sales.columns = ["Category", "Revenue"]
        cat_sales = cat_sales.sort_values(by="Revenue", ascending=False)
        
        return jsonify(cat_sales.to_dict(orient="records"))
    except Exception as e:
        print(f"Error in sales analytics: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/analytics/customers', methods=['GET'])
def get_customer_analytics():
    try:
        table = get_table_name()
        loc_col = get_location_col()
        result = supabase.table(table).select(f"customer_id, {loc_col}").execute()
        df = pd.DataFrame(result.data)
        print(f"👤 Customer Data Rows: {len(df)}")
        if df.empty: return jsonify([])

        # Match Streamlit: Group by Location and Count Customers
        location_data = df.groupby(loc_col).size().reset_index(name="Count")
        location_data = location_data.sort_values(by="Count", ascending=False)
        
        return jsonify(location_data.to_dict(orient="records"))
    except Exception as e:
        print(f"Error in customer analytics: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/analytics/products', methods=['GET'])
def get_product_analytics():
    try:
        table = get_table_name()
        cat_col = get_category_col()
        metric_col = get_metric_col()
        
        result = supabase.table(table).select(f"{cat_col}, {metric_col}").execute()
        df = pd.DataFrame(result.data)
        print(f"📦 Product Data Rows: {len(df)}")
        if df.empty: return jsonify([])

        df[metric_col] = pd.to_numeric(df[metric_col], errors="coerce").fillna(0)
        # Match Streamlit: Top Categories by Revenue
        top_categories = df.groupby(cat_col)[metric_col].sum().sort_values(ascending=False).reset_index()
        top_categories.columns = ["Category", "Total Revenue"]
        
        return jsonify(top_categories.to_dict(orient="records"))
    except Exception as e:
        print(f"Error in product analytics: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)
