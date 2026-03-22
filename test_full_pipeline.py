import requests
import io
import json

csv_data = "Date,Product,Sales,Region\n2026-03-21,Laptop,1500,North\n2026-03-22,Phone,800,South"
csv_file = io.BytesIO(csv_data.encode())

URL = "http://127.0.0.1:5000"

# Step 1: Upload
r_upload = requests.post(f"{URL}/api/upload", files={'file': ('sales_data.csv', csv_file)})
upload_res = r_upload.json()

# Step 2: Query
r_query = requests.post(f"{URL}/query", json={"query": "total sales by product"})
query_res = r_query.json()

# Minimalist output for result check
out = {
    "upload_status": upload_res.get('status'),
    "active_table": upload_res.get('table'),
    "query_status": query_res.get('status'),
    "first_row": query_res.get('data', [{}])[0] if query_res.get('data') else "NONE"
}
with open("test_out.txt", "w") as f:
    f.write(json.dumps(out, indent=2))
