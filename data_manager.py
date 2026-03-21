import pandas as pd
import re
import time

def clean_column_name(name):
    """Sanitize column names for PostgreSQL."""
    name = str(name).strip().lower()
    name = re.sub(r'[^a-z0-9]', '_', name)
    name = re.sub(r'_+', '_', name)
    return name.strip('_')

def process_csv_for_supabase(uploaded_file):
    """
    Reads CSV, cleans schema, and prepares table creation SQL.
    Returns: df, table_name, columns_list, create_table_sql
    """
    df = pd.read_csv(uploaded_file)
    
    # Original columns for mapping if needed, but we'll just rename them
    df.columns = [clean_column_name(col) for col in df.columns]
    
    # Generate unique table name
    timestamp = int(time.time())
    table_name = f"user_data_{timestamp}"
    
    # Generate CREATE TABLE SQL (using TEXT for simplicity/compatibility)
    cols_sql = [f'"{col}" TEXT' for col in df.columns]
    create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols_sql)});'
    
    return df, table_name, list(df.columns), create_table_sql

def upload_to_supabase(supabase, table_name, df):
    """Inserts dataframe rows into the created table."""
    # Convert all data to string to match our TEXT schema
    df = df.astype(str)
    
    # Convert to list of dicts for Supabase insert
    data = df.to_dict(orient="records")
    
    # Insert in chunks to avoid payload limits
    chunk_size = 500
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        supabase.table(table_name).insert(chunk).execute()
    
    return True
