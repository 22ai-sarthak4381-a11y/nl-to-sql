# dashboard.py (Groq API powered Streamlit integration with full feature set)
import os
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
import plotly.express as px
import httpx
import time
from supabase import create_client

# Import Groq-powered SQL generator and defaults
from nl_to_sql import generate_sql, generate_sql_with_groq, DEFAULT_COLUMNS

# Analytics modules
from analytics.customer_dashboard import run_customer_analytics
from analytics.product_dashboard import run_product_analytics
from data_manager import process_csv_for_supabase, upload_to_supabase

# ------------------- Python Summary -------------------
def generate_python_summary(df):
    if df.empty:
        return "No data available to generate insights."

    summary = "### 💡 Dynamic BI Insights\n"
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()

    if numeric_cols:
        main_val = numeric_cols[0]
        data = df[main_val].dropna().tolist()
        if not data:
            return "No numeric records found for deeper analysis."
            
        max_val = max(data)
        avg_val = sum(data) / len(data)

        # 1. Performance Insights
        if max_val > avg_val * 1.5:
            summary += "- 🚀 **Performance Alert**: The top data segment significantly outperforms others (high skew detected).\n"
        elif avg_val < 200:
            summary += "- 📉 **Observation**: Spending and revenue indicators are currently below moderate thresholds.\n"
        else:
            summary += "- ✅ **Stability**: The results shows a moderate and stable distribution across segments.\n"

        # 2. Market Leadership
        if len(data) > 1:
            sorted_data = sorted(data, reverse=True)
            if sorted_data[0] > sorted_data[1] * 1.2:
                summary += "- 🏆 **Leadership Detected**: A clear dominant segment has emerged in this dataset.\n"

        # 3. Simple Stats
        for col in numeric_cols[:2]: # Show first two main metrics
            summary += f"- **{col.title().replace('_', ' ')}**: Average {df[col].mean():,.2f} | Peak {df[col].max():,.2f}\n"

    else:
        summary += f"- ℹ️ **Note**: The results include {len(df)} records for the requested parameters.\n"

    return summary


# ------------------- Load environment and Supabase -------------------
load_dotenv()
st.set_page_config(page_title="AI Business Dashboard", layout="wide")
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase = create_client(url, key)

# ------------------- Sidebar Settings -------------------
st.sidebar.title("🛠️ Settings")
data_mode = st.sidebar.radio(
    "Data Source",
    ["Demo Dataset", "Upload Dataset"],
    help="Choose between the built-in demo data or your own CSV file."
)

if st.sidebar.button("🗑️ Clear AI Cache", help="Clears the persistent AI query cache if you're hitting errors or want fresh results."):
    cache_file = "ai_query_cache.json"
    if os.path.exists(cache_file):
        os.remove(cache_file)
    st.cache_data.clear()
    if "cached_sql" in st.session_state:
        del st.session_state.cached_sql
    st.sidebar.success("Cache cleared successfully!")
    time.sleep(1)
    st.rerun()

# Default table and columns
active_table = "ecommerce_behavior"
active_columns = DEFAULT_COLUMNS

# ------------------- Upload Dataset Logic -------------------
if data_mode == "Upload Dataset":
    st.sidebar.markdown("---")
    st.sidebar.subheader("📤 Upload New Data")
    uploaded_file = st.sidebar.file_uploader("Upload CSV Dataset", type=["csv"])
    
    if uploaded_file:
        df_preview = pd.read_csv(uploaded_file)
        st.sidebar.subheader("📄 Data Preview")
        st.sidebar.dataframe(df_preview.head(), use_container_width=True)
        
        if "last_uploaded" not in st.session_state or st.session_state.last_uploaded != uploaded_file.name:
            with st.sidebar:
                with st.spinner("Processing dataset..."):
                    uploaded_file.seek(0)
                    df_up, table_name, columns, create_sql = process_csv_for_supabase(uploaded_file)
                    try:
                        supabase.rpc("execute_sql", {"query": create_sql}).execute()
                        upload_to_supabase(supabase, table_name, df_up)
                        st.session_state.uploaded_table = table_name
                        st.session_state.uploaded_columns = columns
                        st.session_state.last_uploaded = uploaded_file.name
                        st.sidebar.success(f"Successfully uploaded: {table_name}")
                    except Exception as e:
                        st.sidebar.error(f"Upload failed: {e}")
    
    if "uploaded_table" in st.session_state:
        active_table = st.session_state.uploaded_table
        active_columns = st.session_state.uploaded_columns
        st.sidebar.info(f"Active Table: {active_table}")
    else:
        st.sidebar.warning("Please upload a CSV file to begin.")

# ------------------- Quick Query Interpretation (Hardcoded Logic) -------------------
def quick_interpret(query):
    query = query.lower()
    
    # Simple metric detection
    metric = "General Analytics"
    if any(word in query for word in ["sales", "revenue", "amount", "sold"]): metric = "Total Sales"
    elif "count" in query or "number of" in query: metric = "Record Count"
    elif "average" in query or "avg" in query: metric = "Average Value"
    elif "rating" in query: metric = "Product Ratings"
    
    # Advanced grouping detection
    group_by = None
    if "by gender" in query: group_by = "Gender"
    elif "by category" in query: group_by = "Purchase Category"
    elif "by location" in query or "by city" in query: group_by = "Location"
    elif "by channel" in query: group_by = "Purchase Channel"
    elif "by month" in query: group_by = "Month"
    elif "by occupation" in query: group_by = "Occupation"
    
    # Legacy fallback if no explicit "by" used
    if not group_by:
        if "month" in query: group_by = "Month"
        elif "category" in query: group_by = "Category"
        elif "location" in query or "city" in query: group_by = "Location"
        elif "gender" in query: group_by = "Gender"
        elif "channel" in query: group_by = "Purchase Channel"
        else: group_by = "Overall"

    
    # Simple filter detection (basic extraction)
    filters = []
    months = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"]
    for m in months:
        if m in query or m[:3] in query:
            filters.append(m.title())
    
    if "bangalore" in query: filters.append("Bangalore")
    if "mumbai" in query: filters.append("Mumbai")
    if "male" in query and "female" not in query: filters.append("Male")
    if "female" in query: filters.append("Female")
    
    return {
        "metric": metric,
        "group_by": group_by,
        "filter": ", ".join(filters) if filters else "None"
    }

def generate_insight(avg, median):
    if avg > median:
        return "High spenders dominate purchases"
    elif avg < median:
        return "Most customers are moderate or low spenders"
    else:
        return "Spending is evenly distributed"

def validate_sql(sql: str) -> bool:

    """
    Validates SQL strings to ensure they are safe and only contain SELECT queries.
    """
    sql_upper = sql.upper()
    
    # 1. Block destructive keywords
    if any(keyword in sql_upper for keyword in ["DROP", "DELETE", "UPDATE"]):
        return False
        
    # 2. Must contain SELECT
    if "SELECT" not in sql_upper:
        return False
        
    # 3. Block multiple statements (more than one semicolon)
    if sql.count(';') > 1:
        return False
        
    return True


# ------------------- AI Query Dashboard Page -------------------
def run_ai_query_dashboard():
    st.title("📊 AI Business Insights Dashboard")
    st.info("💡 **Feature Status**: Powered by Groq API (groq-1). Persistent Caching and Multi-attempt Retries enabled for maximum reliability.")
    
    if "df" not in st.session_state:
        st.session_state.df = None
    if "sql_query" not in st.session_state:
        st.session_state.sql_query = None
    if "question" not in st.session_state:
        st.session_state.question = ""

    # --- USE st.form to fix the "off-by-one" / delayed execution bug ---
    with st.form("ai_query_form", clear_on_submit=False):
        # Capture question input directly in the form
        user_input = st.text_input("Ask your business question", value="", placeholder="e.g., Show total sales by category in March")
        submit_clicked = st.form_submit_button("🚀 Run AI Analytics")

    if submit_clicked and user_input:
        # Cooldown to protect AI API quota
        if "last_call" not in st.session_state:
            st.session_state.last_call = 0
            
        time_since_last = time.time() - st.session_state.last_call
        if time_since_last < 5:
            st.warning(f"⏳ Please wait {5 - int(time_since_last)} seconds before the next query to protect your rate limits.")
            st.stop()
            
        st.session_state.last_call = time.time()
        # Immediately process the current input, and store it for downstream logic like charts/insights
        query_to_run = user_input.strip()
        st.session_state.question = query_to_run
        
        with st.spinner("Analyzing your question..."):
            try:
                # Quota-safe SQL generation using the CURRENT input
                columns_list = list(active_columns)
                sql_query = generate_sql(query_to_run, table_name=active_table, columns=columns_list)
            except Exception as e:
                st.error("🚫 AI Query failed. Please try again or wait for quota reset.")
                st.write(f"Details: {e}")
                return

            # ---------- Sanitize and validate the SQL query ----------
            def sanitize_sql(raw: str) -> str:
                """
                Cleans up AI-generated SQL before sending to Supabase:
                1. Strips markdown fences
                2. Extracts from first SELECT onward
                3. If the model returned MULTIPLE separate SELECT statements,
                   automatically joins them with UNION ALL into a single query.
                """
                import re
                # Strip markdown fences
                raw = raw.replace("```sql", "").replace("```", "").strip()
                # Find first SELECT
                select_idx = raw.upper().find("SELECT")
                if select_idx != -1:
                    raw = raw[select_idx:]
                # Split on statement boundaries (semicolon followed by optional whitespace + SELECT)
                # This detects multiple separate statements
                parts = re.split(r';\s*(?=SELECT)', raw, flags=re.IGNORECASE)
                parts = [p.strip().rstrip(';') for p in parts if p.strip()]
                if len(parts) > 1:
                    # Multiple statements — stitch into a single UNION ALL
                    # Wrap each part in a subquery to avoid ORDER BY conflicts
                    sub_parts = []
                    for part in parts:
                        # Strip trailing ORDER BY from individual parts (only final gets it)
                        clean = re.sub(r'\s+ORDER\s+BY\s+.+$', '', part, flags=re.IGNORECASE | re.DOTALL).strip()
                        sub_parts.append(clean)
                    combined = "\nUNION ALL\n".join(sub_parts)
                    raw = combined
                else:
                    raw = parts[0] if parts else raw
                    raw = raw.rstrip(';')
                return raw

            sql_query = sanitize_sql(sql_query)

            if not sql_query:
                st.error("🚫 AI returned an empty response. Please try again.")
                return

            st.session_state.sql_query = sql_query

            # Execute SQL in Supabase with retry logic and safety validation
            print("Generated SQL:", sql_query)
            print("Validation Result:", validate_sql(sql_query))

            if validate_sql(sql_query):
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        result = supabase.rpc("execute_sql", {"query": sql_query}).execute()
                        df_result = pd.DataFrame(result.data)
                        
                        if df_result.empty:
                            suggestion = "Try rephrasing your question or using broader categories."
                            if ">" in sql_query or "<" in sql_query:
                                suggestion = "Threshold alert: The criteria might be too strict. Try lowering your numeric thresholds."
                            elif "WHERE" in sql_query.upper():
                                suggestion = "No records matched your filters. Try removing a specific condition (like city or category)."
                            
                            st.warning(f"🔍 No results matched your criteria.\n\n💡 **Suggestion**: {suggestion}")
                            st.session_state.df = None
                            break

                        
                        st.session_state.df = df_result
                        break
                    except (httpx.RemoteProtocolError, httpx.ReadTimeout, httpx.ConnectError) as e:
                        if attempt < max_retries - 1:
                            time.sleep(1)
                            continue
                        else:
                            st.error(f"📡 Connection failed after multiple attempts: {str(e)}")
                            st.session_state.df = None
                    except Exception as e:
                        st.error(f"❌ Query execution failed: {str(e)}")
                        st.session_state.df = None
                        break
            else:

                st.error("🚫 Blocked unsafe SQL! Security restrictions applied.")
                print("Blocked unsafe SQL:", sql_query)
                st.session_state.df = None


    # Display Interpretation and Generated SQL
    if st.session_state.question:
        interpretation = quick_interpret(st.session_state.question)
        with st.container():
            st.markdown(f"""
            <div style="background-color: rgba(255, 255, 255, 0.05); padding: 15px; border-radius: 10px; border-left: 5px solid #00d4ff; margin-bottom: 20px;">
                <h4 style="margin-top: 0; color: #00d4ff;">🧭 Interpreted Query</h4>
                <ul style="list-style-type: none; padding-left: 5px; margin-bottom: 0;">
                    <li><strong>Metric:</strong> {interpretation['metric']}</li>
                    <li><strong>Grouped by:</strong> {interpretation['group_by']}</li>
                    <li><strong>Filter:</strong> {interpretation['filter']}</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)

    if st.session_state.sql_query:
        with st.expander("🛠️ View Generated SQL"):
            st.code(st.session_state.sql_query)

    # Display Results, Filters, and KPIs
    if st.session_state.df is not None:
        df = st.session_state.df.copy()
        
        if df.empty:
            st.warning("No results found for your query. Try rephrasing.")
        else:
            st.markdown("---")
            st.subheader("🔍 Filter Results")
            filter_col1, filter_col2, filter_col3 = st.columns(3)
            
            with filter_col1:
                date_col = "sale_date" if "sale_date" in df.columns else ("time_of_purchase" if "time_of_purchase" in df.columns else None)
                if date_col:
                    df[date_col] = pd.to_datetime(df[date_col])
                    min_date, max_date = df[date_col].min().date(), df[date_col].max().date()
                    if min_date == max_date:
                        selected_dates = st.date_input("📅 Date Range", value=(min_date, max_date))
                    else:
                        selected_dates = st.date_input("📅 Date Range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
                    if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
                        start_date, end_date = selected_dates
                        df = df[(df[date_col].dt.date >= start_date) & (df[date_col].dt.date <= end_date)]
                        
            with filter_col2:
                loc_col = "city" if "city" in df.columns else ("location" if "location" in df.columns else None)
                if loc_col:
                    locations = df[loc_col].dropna().unique().tolist()
                    selected_loc = st.multiselect("🏙️ Location Filter", options=locations, default=locations)
                    if selected_loc:
                        df = df[df[loc_col].isin(selected_loc)]
                        
            with filter_col3:
                prod_col = "product_name" if "product_name" in df.columns else ("purchase_category" if "purchase_category" in df.columns else None)
                if prod_col:
                    products = df[prod_col].dropna().unique().tolist()
                    selected_product = st.multiselect("📦 Product Filter", options=products, default=products)
                    if selected_product:
                        df = df[df[prod_col].isin(selected_product)]

            if df.empty:
                st.warning("No results match your filters.")
            else:
                # KPIs Section
                st.markdown("---")
                st.subheader("📈 Dashboard KPIs")
                col1, col2, col3 = st.columns(3)
                sales_col = "sales_amount" if "sales_amount" in df.columns else ("total_sales" if "total_sales" in df.columns else ("purchase_amount" if "purchase_amount" in df.columns else None))
                product_col = "product_name" if "product_name" in df.columns else ("purchase_category" if "purchase_category" in df.columns else None)
                if sales_col: df[sales_col] = pd.to_numeric(df[sales_col], errors="coerce")
                total_sales_val = df[sales_col].sum() if sales_col else 0
                total_products_val = df[product_col].nunique() if product_col else 0
                avg_sales_val = df[sales_col].mean() if sales_col else 0
                with col1: st.metric("💰 Total Sales", f"₹{total_sales_val:,.0f}" if total_sales_val else "₹0")
                with col2: st.metric("📦 Total Products", f"{total_products_val:,}" if total_products_val else "0")
                with col3: st.metric("📊 Average Sales", f"₹{avg_sales_val:,.2f}" if avg_sales_val else "₹0.00")

                # Table and Charts
                st.markdown("---")
                col_table, col_chart = st.columns(2)
                with col_table:
                    st.subheader("📋 Result Table")
                    st.dataframe(df, use_container_width=True)
                with col_chart:
                    # Get current question from session state safely
                    current_q = st.session_state.get("question", "")
                    question_lower = current_q.lower()
                    if "by" in question_lower or "distribution" in question_lower or "trend" in question_lower:
                        st.subheader("📊 Chart Visualization")
                        cols = df.columns.tolist()
                        if len(cols) >= 2:
                            x_col, y_col = cols[0], cols[1]
                            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                            if numeric_cols: y_col = numeric_cols[0]
                            fig = px.bar(df, x=x_col, y=y_col, title=f"{y_col} by {x_col}", template="plotly_dark")
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.bar_chart(df)

                st.markdown("---")
                st.subheader("💡 Data Insight Summary")
                base_insights = generate_python_summary(df)
                
                advanced_insight = ""
                # New Layer: Purchase Amount Insights
                if "purchase_amount" in [c.lower() for c in df.columns]:
                    col_name = [c for c in df.columns if c.lower() == "purchase_amount"][0]
                    col_data = pd.to_numeric(df[col_name], errors="coerce").dropna()
                    if len(col_data) > 1:
                        avg_val = col_data.mean()
                        median_val = col_data.median()
                        insight_text = generate_insight(avg_val, median_val)
                        advanced_insight = (
                            f"**Average Purchase Amount**: ₹{avg_val:,.2f}  \n"
                            f"**Median Purchase Amount**: ₹{median_val:,.2f}  \n"
                            f"**Deep Insight**: {insight_text}  \n\n---\n"
                        )
                
                st.info(advanced_insight + base_insights)
                st.caption("Summary enhanced with Statistical Insight Layer.")


# ------------------- Sales Analytics Dashboard Page -------------------
def run_sales_dashboard():
    st.title("💰 Sales Analytics")
    with st.spinner("Loading sales data..."):
        try:
            max_retries = 3
            df = pd.DataFrame()
            for attempt in range(max_retries):
                try:
                    result = supabase.table("ecommerce_behavior").select("purchase_category, purchase_amount, time_of_purchase").execute()
                    df = pd.DataFrame(result.data)
                    break
                except (httpx.RemoteProtocolError, httpx.ReadTimeout, httpx.ConnectError) as e:
                    if attempt < max_retries - 1: time.sleep(1); continue
                    else: st.error("Connection failed."); st.write(e); df = pd.DataFrame(); break
            
            if df.empty:
                st.warning("No data available for sales analysis.")
                return

            df["purchase_amount"] = pd.to_numeric(df["purchase_amount"], errors="coerce")
            df["time_of_purchase"] = pd.to_datetime(df["time_of_purchase"])
            df["Day"] = df["time_of_purchase"].dt.day_name()
            df["Hour"] = df["time_of_purchase"].dt.hour
            cat_sales = df.groupby("purchase_category")["purchase_amount"].sum().reset_index()
            cat_sales.columns = ["Category", "Revenue"]

            chart_col1, chart_col2 = st.columns(2)
            with chart_col1:
                st.subheader("Sales by Category")
                st.plotly_chart(px.bar(cat_sales, x="Category", y="Revenue", title="Revenue per Category", template="plotly_dark", color="Revenue", color_continuous_scale="Viridis"), use_container_width=True)
            with chart_col2:
                st.subheader("Revenue Distribution")
                st.plotly_chart(px.pie(cat_sales, values="Revenue", names="Category", title="Revenue %", template="plotly_dark", hole=0.4), use_container_width=True)

            st.markdown("---")
            st.subheader("🕒 Demand Heatmap (Day vs Hour)")
            heatmap_data = df.pivot_table(values="purchase_amount", index="Day", columns="Hour", aggfunc="sum").fillna(0)
            days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            heatmap_data = heatmap_data.reindex(days_order)
            st.plotly_chart(px.imshow(heatmap_data, labels=dict(x="Hour", y="Day", color="Rev"), x=[f"{h}:00" for h in heatmap_data.columns], y=heatmap_data.index, title="Sales Heatmap", color_continuous_scale="Viridis", template="plotly_dark"), use_container_width=True)
            st.metric("Total Cumulative Revenue", f"₹{cat_sales['Revenue'].sum():,.2f}")
        except Exception as e:
            st.error("Failed to load sales analytics."); st.write(e)

def run_customer_dashboard():
    st.title("👤 Customer Analytics")
    run_customer_analytics(supabase)

def run_product_dashboard():
    st.title("📦 Product Analytics")
    run_product_analytics(supabase)

# ------------------- Sidebar Navigation Layout -------------------
st.sidebar.title("Dashboard Navigation")
page = st.sidebar.selectbox(
    "Select Dashboard",
    ["AI Query", "Sales Analytics", "Customer Analytics", "Product Analytics"]
)

if page == "AI Query":
    run_ai_query_dashboard()
elif page == "Sales Analytics":
    run_sales_dashboard()
elif page == "Customer Analytics":
    run_customer_dashboard()
elif page == "Product Analytics":
    run_product_dashboard()