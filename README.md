# NLP to SQL AI Project 📊

## Problem Statement
Business users and analysts frequently require immediate, ad-hoc data insights to make informed decisions. However, they consistently face a bottleneck in relying on data engineers because they lack the required SQL expertise. This project aims to bridge the gap by allowing users to instantly generate complex SQL queries and visualize the results simply by asking questions in natural English.

## Architecture
1. **User Question:** The user types their question into the React dashboard input field.
2. **AI Translation:** The system sends the prompt to the `Groq API (groq/compound)` which generates a structured PostgreSQL query matching the application schema.
3. **Database Query:** The generated SQL is transmitted to a cloud-hosted `Supabase PostgreSQL` instance via a custom RPC function.
4. **Insight Visualization:** The results are parsed by `Pandas` and automatically rendered on the React UI containing KPI Metrics, side-by-side Tables and dynamic Charts, and natural language AI Summaries.

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- Node.js 18+

### 1. Set up environment variables
```bash
cp backend/.env.example backend/.env
# Edit backend/.env and fill in your GROQ_API_KEY, SUPABASE_URL, and SUPABASE_KEY
```

### 2. Install backend dependencies
```bash
cd backend
pip install -r ../requirements.txt
```

### 3. Install frontend dependencies
```bash
cd frontend
npm install
```

### 4. Run the app

**Windows:** Double-click `RUN_DASHBOARD_IN_CHROME.bat` or `RUN_DASHBOARD_IN_EDGE.bat`.

**Linux / macOS:**
```bash
# Terminal 1 – start the Flask backend
cd backend && python app.py

# Terminal 2 – start the React frontend
cd frontend && npm run dev
```

Then open **http://localhost:5173** in your browser.

## Screenshots
*(Add screenshots of your application here!)*
- Screenshot 1: 
- Screenshot 2:

## Technologies Used
- **Frontend / Visualization:** React (Vite) + Recharts
- **Backend Processing:** Python 3, Flask, Pandas
- **Database Architecture:** Supabase (PostgreSQL)
- **Natural Language Processing:** Groq API (`groq` package, model: `groq/compound`)

## Results
The completed system serves as a fully functional Business Intelligence tool. Users can type questions such as *"sales in Bangalore in March"*, and the AI reliably translates standard date strings to date ranges, executes the database request without case-sensitivity errors, and builds out dynamic visual comparisons accurately over the web application.
