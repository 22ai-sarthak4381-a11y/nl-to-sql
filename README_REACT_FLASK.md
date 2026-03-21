# React + Flask Migration Guide 🚀

This project has been migrated from Streamlit to a modern **React (Frontend)** and **Flask (Backend)** architecture.

## 🏗️ Architecture
1.  **Frontend**: React (Vite) + Recharts for visualization
2.  **Backend**: Flask API + Groq SQL Generator
3.  **Database**: Supabase (PostgreSQL)

---

## 🛠️ Step 1: Set up the Backend
1.  Navigate to the `backend` folder.
2.  Ensure your `.env` file contains your `GROQ_API_KEY`, `SUPABASE_URL`, and `SUPABASE_KEY`.
3.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
4.  Run the Flask server:
    ```bash
    python app.py
    ```
    *The server will start on `http://localhost:5000`.*

---

## 🎨 Step 2: Set up the Frontend
1.  Navigate to the `frontend` folder.
2.  Install dependencies:
    ```bash
    npm install
    ```
3.  Start the React app:
    ```bash
    npm run dev
    ```
    *The app will start on (typically) `http://localhost:5173`.*

---

## 🚀 Step 3: Use the Dashboard
1.  Open Chrome or Edge to `http://localhost:5173`.
2.  Ask any business question like *"Show total sales by month in Bangalore"*.
3.  View the **Interpreted Query** breakdown and the results in clear Tables and Bar Charts!

---

## 📝 Technical Note
- **API Endpoint**: `POST /query`
- **Logic**: The backend keeps the exact logic from the original project (caching, type casting for dates and numbers).
- **CORS**: Enabled on the backend to allow communication between the React app and Flask.
