import os
from dotenv import load_dotenv
from groq import Groq

load_dotenv()

try:
    client = Groq(api_key=os.getenv("GROQ_API_KEY"))
    response = client.chat.completions.create(
        model="groq/compound", # Actual flagship model for groq-1 identifier
        messages=[{"role": "user", "content": "Ping"}],
        max_tokens=10
    )
    print("Groq API Success!")
    print(response.choices[0].message.content)
except Exception as e:
    print(f"Error: {e}")
