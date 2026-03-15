import os
from dotenv import load_dotenv
from google import genai
import time

load_dotenv()
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

def generate_sql(question):

    prompt = f"""
    Convert the following question into PostgreSQL SQL.

    Rules:
    1. Table name is ecommerce_behavior
    2. Use columns: customer_id, age, gender, income_level, marital_status, education_level, occupation, location, purchase_category, purchase_amount, frequency_of_purchase, purchase_channel, brand_loyalty, product_rating, time_spent_on_product_researchhours, social_media_influence, discount_sensitivity, return_rate, customer_satisfaction, engagement_with_ads, device_used_for_shopping, payment_method, time_of_purchase, discount_used, customer_loyalty_program_member, purchase_intent, shipping_preference, time_to_decision
    3. Use case-insensitive search with ILIKE
    4. Support date filtering if time_of_purchase is queried. Assume current year 2026.
    5. CRITICAL: ALL NUMBER COLUMNS (like purchase_amount, age, product_rating, return_rate, time_spent_on_product_researchhours) are stored as TEXT. If you use aggregate functions (SUM, AVG) or if you use mathematical operators (>, <, =, etc.), you MUST cast the column to numeric first. Example: CAST(age AS NUMERIC) > 18 or SUM(CAST(purchase_amount AS NUMERIC)).
    6. Only return SQL query. Do not include a trailing semicolon (;) and do not include any comments.

    Question: {question}
    """
    response = client.models.generate_content(
    model="gemini-3.1-flash-lite-preview",
    contents=prompt
  )
    time.sleep(5)

    return response.text

def generate_insight(question, df_data):
    prompt = f"""
    You are an AI data analyst. Briefly answer the user's business question based on the following data (which is the result of a database query answering their question). 
    Keep it strictly professional, concise (1-2 sentences), and directly address the user's question. 
    Use Indian Rupees (₹) for any currency values.
    Do not mention SQL or the database, just analyze the data. Highlight the most important number or trend.
    
    Question: {question}
    Data: {df_data}
    """
    response = client.models.generate_content(
        model="gemini-3.1-flash-lite-preview",
        contents=prompt
    )
    return response.text
