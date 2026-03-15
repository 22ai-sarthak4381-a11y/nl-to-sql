import os
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
import plotly.express as px
import httpx
import time
from supabase import create_client
from nl_to_sql import generate_sql, generate_insight
from analytics.customer_dashboard import run_customer_analytics
from analytics.product_dashboard import run_product_analytics

load_dotenv()

st.set_page_config(page_title="AI Business Dashboard", layout="wide")

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
# Increase timeout to handle complex queries or slow connections
supabase = create_client(url, key)
# Note: In newer versions of supabase-py, we can set timeout via ClientOptions.
# If the error persists, consider increasing global httpx timeout if available.

def run_ai_query_dashboard():
    st.title("📊 AI Business Insights Dashboard")
    
    # Initialize session state variables to handle re-runs properly with filters
    if "df" not in st.session_state:
        st.session_state.df = None
    if "sql_query" not in st.session_state:
        st.session_state.sql_query = None
    if "question" not in st.session_state:
        st.session_state.question = ""

    question = st.text_input("Ask your business question", value=st.session_state.question)

    if st.button("Run Query") and question:
        st.session_state.question = question
        
        with st.spinner("Analyzing your question..."):
            # Generate SQL from AI
            sql_query = generate_sql(question)
            
            # Remove markdown formatting if present
            sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
            
            # Extract strictly the SQL part starting with SELECT
            select_idx = sql_query.upper().find("SELECT")
            if select_idx != -1:
                sql_query = sql_query[select_idx:]

            # Remove trailing semicolon for Supabase RPC execution
            sql_query = sql_query.replace(";", "")
            
            st.session_state.sql_query = sql_query

            # Retry logic for robust connection
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # Execute SQL in Supabase
                    result = supabase.rpc("execute_sql", {"query": sql_query}).execute()
                    
                    # Convert JSON result to DataFrame
                    st.session_state.df = pd.DataFrame(result.data)
                    break # Success!
                except (httpx.RemoteProtocolError, httpx.ReadTimeout, httpx.ConnectError) as e:
                    if attempt < max_retries - 1:
                        time.sleep(1) # Wait before retry
                        continue
                    else:
                        st.error("Connection failed after multiple attempts. The Supabase server might be busy or timing out.")
                        st.write(e)
                        st.session_state.df = None
                except Exception as e:
                    st.error("Query execution failed")
                    st.write(e)
                    st.session_state.df = None
                    break

    # Display Results if they exist in session state
    if st.session_state.sql_query:
        with st.expander("View Generated SQL"):
            st.code(st.session_state.sql_query)

    if st.session_state.df is not None:
        df = st.session_state.df.copy()
        
        if df.empty:
            st.warning("No results found for your query. Try rephrasing or asking something else.")
        else:
            st.markdown("---")
            st.subheader("🔍 Filter Results")
            
            # Dynamic Filters Layout
            filter_col1, filter_col2, filter_col3 = st.columns(3)
            
            with filter_col1:
                date_col = "sale_date" if "sale_date" in df.columns else ("time_of_purchase" if "time_of_purchase" in df.columns else None)
                if date_col:
                    df[date_col] = pd.to_datetime(df[date_col])
                    min_date = df[date_col].min().date()
                    max_date = df[date_col].max().date()
                    
                    # Handling single date edge case
                    if min_date == max_date:
                        selected_dates = st.date_input("📅 Select Date Range", value=(min_date, max_date))
                    else:
                        selected_dates = st.date_input(
                            "📅 Select Date Range", 
                            value=(min_date, max_date), 
                            min_value=min_date, 
                            max_value=max_date
                        )
                        
                    if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
                        start_date, end_date = selected_dates
                        df = df[(df[date_col].dt.date >= start_date) & (df[date_col].dt.date <= end_date)]
                        
            with filter_col2:
                loc_col = "city" if "city" in df.columns else ("location" if "location" in df.columns else None)
                if loc_col:
                    locations = df[loc_col].dropna().unique().tolist()
                    selected_loc = st.multiselect("🏙️ Filter by Location", options=locations, default=locations)
                    if selected_loc:
                        df = df[df[loc_col].isin(selected_loc)]
                        
            with filter_col3:
                prod_col = "product_name" if "product_name" in df.columns else ("purchase_category" if "purchase_category" in df.columns else None)
                if prod_col:
                    products = df[prod_col].dropna().unique().tolist()
                    selected_product = st.multiselect("📦 Filter by Product", options=products, default=products)
                    if selected_product:
                        df = df[df[prod_col].isin(selected_product)]

            # Check if df is empty after applying filters
            if df.empty:
                st.warning("No results found flexibly matching your filters. Please adjust them.")
            else:
                # Dashboard KPIs
                st.markdown("---")
                st.subheader("📈 Dashboard KPIs")
                col1, col2, col3 = st.columns(3)
                
                # Safely compute metrics if columns exist or are aliased as total_sales
                sales_col = "sales_amount" if "sales_amount" in df.columns else ("total_sales" if "total_sales" in df.columns else ("purchase_amount" if "purchase_amount" in df.columns else None))
                product_col = "product_name" if "product_name" in df.columns else ("purchase_category" if "purchase_category" in df.columns else None)

                if sales_col:
                    df[sales_col] = pd.to_numeric(df[sales_col], errors="coerce")

                total_sales_val = df[sales_col].sum() if sales_col else 0
                total_products_val = df[product_col].nunique() if product_col else 0
                avg_sales_val = df[sales_col].mean() if sales_col else 0

                with col1:
                    st.metric("💰 Total Sales", f"₹{total_sales_val:,.0f}" if total_sales_val else "₹0")
                with col2:
                    st.metric("📦 Total Products", f"{total_products_val:,}" if total_products_val else "0")
                with col3:
                    st.metric("📊 Average Sales", f"₹{avg_sales_val:,.2f}" if avg_sales_val else "₹0.00")

                st.markdown("---")
                
                # Step 16: Improve UI Design (Side-by-side columns)
                col_table, col_chart = st.columns(2)

                with col_table:
                    st.subheader("📋 Result Table")
                    st.dataframe(df, use_container_width=True)

                with col_chart:
                    question_lower = st.session_state.question.lower()
                    if "by" in question_lower or "distribution" in question_lower or "trend" in question_lower:
                        st.subheader("📊 Chart Visualization")
                        if len(df.columns) >= 2:
                            cols = df.columns.tolist()
                            x_col = cols[0]
                            y_col = cols[1]
                            
                            # Find numeric column for Y axis if possible
                            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                            if numeric_cols:
                                y_col = numeric_cols[0]
                            
                            fig = px.bar(df, x=x_col, y=y_col, title=f"{y_col} by {x_col}", template="plotly_dark")
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.bar_chart(df)

                # Step 15: Add AI Explanation
                st.markdown("---")
                st.subheader("💡 AI Generated Insight")
                with st.spinner("Generating context-aware insight..."):
                    try:
                        # Send top 10 rows to AI for summarization
                        insight_text = generate_insight(st.session_state.question, df.head(10).to_csv(index=False))
                        st.success(insight_text)
                    except Exception as e:
                        st.warning("Could not generate AI insight at this time.")

def run_sales_dashboard():
    st.title("💰 Sales Analytics")
    
    with st.spinner("Loading sales data..."):
        try:
            # Retry logic for robust connection
            max_retries = 3
            df = pd.DataFrame()
            for attempt in range(max_retries):
                try:
                    # Fetch data from Supabase
                    result = supabase.table("ecommerce_behavior").select("purchase_category, purchase_amount, time_of_purchase").execute()
                    df = pd.DataFrame(result.data)
                    break
                except (httpx.RemoteProtocolError, httpx.ReadTimeout, httpx.ConnectError) as e:
                    if attempt < max_retries - 1:
                        time.sleep(1)
                        continue
                    else:
                        raise e
            
            if df.empty:
                st.warning("No data available for sales analysis.")
                return

            # Convert purchase_amount and time_of_purchase
            df["purchase_amount"] = pd.to_numeric(df["purchase_amount"], errors="coerce")
            df["time_of_purchase"] = pd.to_datetime(df["time_of_purchase"])
            
            # Extract Day and Hour for heatmap
            df["Day"] = df["time_of_purchase"].dt.day_name()
            df["Hour"] = df["time_of_purchase"].dt.hour
            
            # Group by category and sum purchase_amount
            category_sales = df.groupby("purchase_category")["purchase_amount"].sum().reset_index()
            category_sales.columns = ["Category", "Revenue"]

            # Create two columns for charts
            chart_col1, chart_col2 = st.columns(2)
            
            with chart_col1:
                st.subheader("Sales by Category")
                # Create Plotly Bar Chart
                fig_bar = px.bar(
                    category_sales,
                    x="Category",
                    y="Revenue",
                    title="Total Revenue by Purchase Category",
                    labels={"Revenue": "Total Revenue (₹)", "Category": "Product Category"},
                    template="plotly_dark",
                    color="Revenue",
                    color_continuous_scale="Viridis"
                )
                st.plotly_chart(fig_bar, use_container_width=True)
            
            with chart_col2:
                st.subheader("Revenue Distribution")
                # Create Plotly Pie Chart
                fig_pie = px.pie(
                    category_sales,
                    values="Revenue",
                    names="Category",
                    title="Revenue % by Category",
                    template="plotly_dark",
                    hole=0.4 # Donut style for premium look
                )
                st.plotly_chart(fig_pie, use_container_width=True)

            st.markdown("---")
            
            # Demand Heatmap Section
            st.subheader("🕒 Demand Heatmap (Day vs Hour)")
            
            # Create Pivot for Heatmap
            heatmap_data = df.pivot_table(
                values="purchase_amount",
                index="Day",
                columns="Hour",
                aggfunc="sum"
            ).fillna(0)
            
            # Order days correctly
            days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            heatmap_data = heatmap_data.reindex(days_order)

            fig_heatmap = px.imshow(
                heatmap_data,
                labels=dict(x="Hour of Day", y="Day of Week", color="Revenue (₹)"),
                x=[f"{h}:00" for h in heatmap_data.columns],
                y=heatmap_data.index,
                title="Sales Heatmap: When do customers spend the most?",
                color_continuous_scale="Viridis",
                template="plotly_dark"
            )
            st.plotly_chart(fig_heatmap, use_container_width=True)
            
            # Additional KPI for this page
            total_rev = category_sales["Revenue"].sum()
            st.metric("Total Cumulative Revenue", f"₹{total_rev:,.2f}")
            
        except Exception as e:
            st.error("Failed to load sales analytics data.")
            st.write(e)

def run_customer_dashboard():
    st.title("👤 Customer Analytics")
    run_customer_analytics(supabase)

def run_product_dashboard():
    st.title("📦 Product Analytics")
    run_product_analytics(supabase)

# Sidebar Navigation
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