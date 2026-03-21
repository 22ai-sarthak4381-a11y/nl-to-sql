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
from ml_engine import get_ml_insights

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

import numpy as np

def generate_insight(avg, median):
    if avg > median:
        return "High spenders dominate purchases"
    elif avg < median:
        return "Most customers are moderate or low spenders"
    else:
        return "Spending is evenly distributed"

ALLOWED_COLUMNS = [
    "purchase_amount",
    "purchase_category",
    "gender",
    "purchase_date",
    "discount_used",
    "location",
    "occupation",
    "purchase_channel",
    "time_of_purchase",
    "customer_satisfaction",
    "return_rate",
    "age",
    "product_rating",
    "customer_id",
    "total_revenue",
    "average_spend",
    "total_count",
    "percentage",
    "discount_applied",
    "records"
]

def validate_sql(sql: str) -> bool:
    """
    Validates SQL strings to ensure they are safe and only interact with allowed columns.
    """
    import re
    sql_clean = sql.upper().replace('"', '').replace('`', '')
    
    # 1. Block destructive keywords
    if any(keyword in sql_clean for keyword in ["DROP", "DELETE", "UPDATE", "INSERT", "TRUNCATE", "ALTER"]):
        logger.warning(f"SECURITY: Blocked destructive SQL: {sql}")
        return False
        
    # 2. Must contain SELECT
    if "SELECT" not in sql_clean:
        return False
        
    # 3. Block multiple statements
    if sql.count(';') > 1:
        return False
        
    # 4. Whitelist Column Validation
    # Use Regex to extract column names properly (handling SELECT, WHERE, GROUP BY, etc.)
    # We look for identifiers that aren't keywords
    sql_keywords = set(["SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "JOIN", "ON", "AND", "OR", "IN", "LIKE", "AS", "COUNT", "SUM", "AVG", "MIN", "MAX", "LIMIT", "DESC", "ASC", "HAVING", "DISTINCT", "BETWEEN", "IS", "NULL", "NOT", "CASE", "WHEN", "THEN", "ELSE", "END", "ECOMMERCE_BEHAVIOR", "*", "UNION", "ALL"])
    
    # Simple regex to find words (potential identifiers)
    words = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', sql_clean)
    
    for word in words:
        if word not in sql_keywords and not word.isdigit():
            if word.lower() not in [c.lower() for c in ALLOWED_COLUMNS]:
                # It might be a value (e.g. 'Clothing'), but in SQL clean we only have identifiers
                # because values are usually in single quotes.
                # However, re.findall will catch values if they are alphanumeric.
                # To be safer, we check if it matches the pattern of a column we know.
                logger.warning(f"VALIDATION: Potential unauthorized field detected: '{word}'")
                # return False # Uncomment for very strict enforcement, but values like 'Bangalore' might trigger it.
    
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
    total_sum = sum(values)
    avg_val = total_sum / n
    
    # Identify Top Category and Second Highest
    sorted_data = sorted(zip(values, labels), key=lambda x: x[0], reverse=True)
    max_val, top_category = sorted_data[0]
    second_val = sorted_data[1][0] if n > 1 else max_val

    # 2. Derived Metrics (Safe division with 1e-6)
    divisor = avg_val + 1e-6
    dominance_ratio = max_val / divisor
    gap_ratio = (max_val - second_val) / divisor
    spread_ratio = (max_val - min_val) / divisor
    percentage_share = (max_val / (total_sum + 1e-6)) * 100

    # 3. Priority-based Controlled Logic
    # Tier 1: Strong Dominance (Strict)
    if dominance_ratio > 1.7 and gap_ratio > 0.5 and percentage_share > 30:
        insight = f"{top_category} strongly dominates, contributing approximately {percentage_share:.1f}% of the total."
        
    # Tier 2: Moderate Leader
    elif dominance_ratio > 1.3 and percentage_share > 15:
        insight = f"{top_category} is the leading category with noticeably higher values."
        
    # Tier 3: No real dominance (Small share leader)
    elif percentage_share < 10:
        insight = "No single category dominates; values are fairly distributed across categories."
        
    # Tier 4: Even Distribution
    elif spread_ratio < 0.1:
        insight = "Values are very evenly distributed across categories."
        
    # Tier 5: Underperformance Detection
    elif min_val < avg_val * 0.5:
        insight = "Some categories are significantly underperforming compared to the average."
        
    # Default Case
    else:
        insight = "There is moderate variation across categories."

    return insight

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

# --- Synonym & Semantic Normalization Layer ---
SYNONYM_MAP = {
    "spending": "purchase_amount",
    "revenue": "purchase_amount",
    "sales": "purchase_amount",
    "cost": "purchase_amount",
    "items": "purchase_category",
    "group": "purchase_category",
    "city": "location",
    "place": "location",
    "rating": "product_rating",
    "score": "customer_satisfaction"
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
        return col.lower() in [c.lower() for c in ALLOWED_COLUMNS]

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
            # 1. SQL Generation with Safe Termination & Input Validation
            sql_query, enhanced_query, validation_hint = generate_sql(user_query)
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
        if "purchase_amount" in [c.lower() for c in df_result.columns]:
            col_name = [c for c in df_result.columns if c.lower() == "purchase_amount"][0]
            values = pd.to_numeric(df_result[col_name], errors='coerce').dropna().tolist()
            if len(values) > 1:
                avg_val = sum(values) / len(values)
                median_val = float(np.median(values))
                insight_text = generate_insight(avg_val, median_val)
                
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

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "Backend is running"}), 200

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    
    # In a real app, you'd process this CSV and upload to Supabase
    # For now, we'll simulate success
    return jsonify({"message": f"File {file.filename} uploaded and processed successfully!"}), 200

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
        result = supabase.table("ecommerce_behavior").select("purchase_category, purchase_amount, time_of_purchase").execute()
        df = pd.DataFrame(result.data)
        print(f"📊 Sales Data Rows: {len(df)}")
        if df.empty: return jsonify([])
        
        df["purchase_amount"] = pd.to_numeric(df["purchase_amount"], errors="coerce").fillna(0)
        # Match Streamlit: Group by Category and Sum Revenue
        cat_sales = df.groupby("purchase_category")["purchase_amount"].sum().reset_index()
        cat_sales.columns = ["Category", "Revenue"]
        cat_sales = cat_sales.sort_values(by="Revenue", ascending=False)
        
        return jsonify(cat_sales.to_dict(orient="records"))
    except Exception as e:
        print(f"Error in sales analytics: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/analytics/customers', methods=['GET'])
def get_customer_analytics():
    try:
        result = supabase.table("ecommerce_behavior").select("customer_id, location").execute()
        df = pd.DataFrame(result.data)
        print(f"👤 Customer Data Rows: {len(df)}")
        if df.empty: return jsonify([])

        # Match Streamlit: Group by Location and Count Customers
        location_data = df.groupby("location").size().reset_index(name="Count")
        location_data = location_data.sort_values(by="Count", ascending=False)
        
        return jsonify(location_data.to_dict(orient="records"))
    except Exception as e:
        print(f"Error in customer analytics: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/analytics/products', methods=['GET'])
def get_product_analytics():
    try:
        result = supabase.table("ecommerce_behavior").select("purchase_category, purchase_amount").execute()
        df = pd.DataFrame(result.data)
        print(f"📦 Product Data Rows: {len(df)}")
        if df.empty: return jsonify([])

        df["purchase_amount"] = pd.to_numeric(df["purchase_amount"], errors="coerce").fillna(0)
        # Match Streamlit: Top Categories by Revenue
        top_categories = df.groupby("purchase_category")["purchase_amount"].sum().sort_values(ascending=False).reset_index()
        top_categories.columns = ["Category", "Total Revenue"]
        
        return jsonify(top_categories.to_dict(orient="records"))
    except Exception as e:
        print(f"Error in product analytics: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)
