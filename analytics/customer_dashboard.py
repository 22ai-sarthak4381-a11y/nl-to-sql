import streamlit as st
import pandas as pd
import plotly.express as px
import httpx
import time

def run_customer_analytics(supabase):
    st.subheader("👤 Customer Deep Dive")
    
    with st.spinner("Analyzing customer data..."):
        try:
            # Retry logic for robust connection
            max_retries = 3
            df = pd.DataFrame()
            for attempt in range(max_retries):
                try:
                    # Fetch relevant customer data
                    result = supabase.table("ecommerce_behavior").select("customer_id, age, income_level, gender, location").execute()
                    df = pd.DataFrame(result.data)
                    break
                except (httpx.RemoteProtocolError, httpx.ReadTimeout, httpx.ConnectError) as e:
                    if attempt < max_retries - 1:
                        time.sleep(1)
                        continue
                    else:
                        raise e
            
            if df.empty:
                st.warning("No customer data available.")
                return

            # KPI Calculations
            total_customers = df["customer_id"].nunique()
            
            # Repeat customers logic
            cust_freq = df.groupby("customer_id").size()
            repeat_customers = cust_freq[cust_freq > 1].count()
            repeat_rate = (repeat_customers / total_customers) * 100 if total_customers > 0 else 0

            # Display KPIs
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Customers", f"{total_customers:,}")
            with col2:
                st.metric("Repeat Customers", f"{repeat_customers:,}")
            with col3:
                st.metric("Repeat Rate", f"{repeat_rate:.1f}%")

            st.markdown("---")
            
            # Demographics Visualizations
            st.subheader("Customer Demographics")
            demo_col1, demo_col2 = st.columns(2)
            
            with demo_col1:
                # Age distribution
                df["age"] = pd.to_numeric(df["age"], errors="coerce")
                fig_age = px.histogram(df, x="age", nbins=20, title="Age Distribution", template="plotly_dark", color_discrete_sequence=['#636EFA'])
                st.plotly_chart(fig_age, use_container_width=True)
                
            with demo_col2:
                # Income level distribution
                income_order = ["Low", "Medium", "High"] # Assuming these values exist based on common ecommerce datasets
                fig_income = px.histogram(df, x="income_level", title="Income Level Distribution", template="plotly_dark", category_orders={"income_level": income_order})
                st.plotly_chart(fig_income, use_container_width=True)

            # Location Analysis
            st.subheader("Customer Distribution by Location")
            location_data = df.groupby("location").size().reset_index(name="Count")
            fig_loc = px.pie(location_data, values="Count", names="location", title="Top Locations", template="plotly_dark", hole=0.4)
            st.plotly_chart(fig_loc, use_container_width=True)

        except Exception as e:
            st.error("Failed to load customer analytics.")
            st.write(e)
