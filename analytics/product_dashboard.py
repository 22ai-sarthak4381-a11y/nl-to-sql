import streamlit as st
import pandas as pd
import plotly.express as px
import httpx
import time

def run_product_analytics(supabase):
    st.subheader("📦 Product Performance Analytics")
    
    with st.spinner("Analyzing product data..."):
        try:
            # Retry logic for robust connection
            max_retries = 3
            df = pd.DataFrame()
            for attempt in range(max_retries):
                try:
                    # Fetch relevant product data
                    # We fetch purchase_category, purchase_amount, and product_rating
                    result = supabase.table("ecommerce_behavior").select("purchase_category, purchase_amount, product_rating").execute()
                    df = pd.DataFrame(result.data)
                    break
                except (httpx.RemoteProtocolError, httpx.ReadTimeout, httpx.ConnectError) as e:
                    if attempt < max_retries - 1:
                        time.sleep(1)
                        continue
                    else:
                        raise e
            
            if df.empty:
                st.warning("No product data available.")
                return

            # Data Preparation
            df["purchase_amount"] = pd.to_numeric(df["purchase_amount"], errors="coerce")
            df["product_rating"] = pd.to_numeric(df["product_rating"], errors="coerce")
            
            # Charts Layout
            prod_col1, prod_col2 = st.columns(2)
            
            with prod_col1:
                # 1. Top Selling Categories (by Revenue)
                st.subheader("Top Categories by Revenue")
                top_categories = df.groupby("purchase_category")["purchase_amount"].sum().sort_values(ascending=False).reset_index()
                top_categories.columns = ["Category", "Total Revenue"]
                
                fig_rev = px.bar(
                    top_categories.head(10),
                    x="Category",
                    y="Total Revenue",
                    title="Top 10 Selling Categories (₹)",
                    template="plotly_dark",
                    color="Total Revenue",
                    color_continuous_scale="Reds"
                )
                st.plotly_chart(fig_rev, use_container_width=True)

            with prod_col2:
                # 2. Category Ratings vs Revenue
                st.subheader("Category Quality vs Volume")
                
                # Group by category for avg rating and total revenue
                cat_stats = df.groupby("purchase_category").agg({
                    "purchase_amount": "sum",
                    "product_rating": "mean"
                }).reset_index()
                cat_stats.columns = ["Category", "Total Revenue", "Average Rating"]
                
                fig_scatter = px.scatter(
                    cat_stats,
                    x="Total Revenue",
                    y="Average Rating",
                    size="Total Revenue",
                    color="Category",
                    hover_name="Category",
                    title="Revenue vs Customer Satisfaction (Rating)",
                    template="plotly_dark",
                    size_max=60
                )
                st.plotly_chart(fig_scatter, use_container_width=True)

            # 3. Product Rating Distribution
            st.subheader("Product Ratings Overview")
            fig_rating = px.histogram(
                df, 
                x="product_rating", 
                title="Distribution of Product Ratings",
                template="plotly_dark",
                color_discrete_sequence=['#FFD700'], # Gold
                nbins=10
            )
            st.plotly_chart(fig_rating, use_container_width=True)

        except Exception as e:
            st.error("Failed to load product analytics.")
            st.write(e)
