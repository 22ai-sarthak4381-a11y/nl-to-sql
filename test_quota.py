import time
import os
from dotenv import load_dotenv
from groq import Groq

load_dotenv()
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

# Groq-branded model
available_models = ["groq/compound"] # Maps to the user's 'groq-1' requirement

print("Testing Groq model for access and quota...")
working_models = []

for model_name in available_models:
    try:
        print(f"Testing {model_name}...")
        response = client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": "Hello"}],
            max_tokens=5
        )
        print(f"✅ {model_name} works!")
        working_models.append(model_name)
    except Exception as e:
        err_msg = str(e).lower()
        if "authentication" in err_msg or "invalid_api_key" in err_msg or "401" in err_msg:
            print(f"❌ {model_name} failed: Authentication error (check GROQ_API_KEY in .env).")
        elif "rate_limit" in err_msg or "429" in err_msg:
            print(f"❌ {model_name} failed: Rate limit exceeded.")
        else:
            print(f"❌ {model_name} failed with error: {str(e)[:150]}...")

print("\n--- WORKING MODELS ---")
for wm in working_models:
    print(wm)
