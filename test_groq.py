import os
from groq import Groq
from dotenv import load_dotenv

load_dotenv()
client = Groq()
try:
    response = client.chat.completions.create(
        model="groq/llama-3-8b-8192", # Using a known available model id
        messages=[{"role": "user", "content": "Return the word 'OK'."}],
        max_tokens=10
    )
    print(f"Groq Test: {response.choices[0].message.content}")
except Exception as e:
    print(f"Groq Test Failed: {str(e)}")
