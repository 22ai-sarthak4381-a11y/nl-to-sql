@echo off
:: This script starts the AI Business Dashboard and opens it specifically in Microsoft Edge.

echo ----------------------------------------------------
echo 🚀 AI Business Insights Dashboard - Edge Launcher
echo ----------------------------------------------------

:: Kill any existing streamlit processes to avoid port conflicts
taskkill /IM streamlit.exe /F 2>nul

echo Starting server in background...
:: Use python -m streamlit to ensure the command is found correctly
start /B python -m streamlit run dashboard.py --server.headless true

echo Waiting 5 seconds for server to initialize...
timeout /t 5 /nobreak > nul

echo 🌐 Opening Dashboard in Microsoft Edge...
start msedge "http://localhost:8501"

echo.
echo Dashboard is now running in your background!
echo Keep this window open while using the dashboard.
echo Press any key to stop the server and exit...
pause > nul

:: Clean up when the user types a key
taskkill /IM streamlit.exe /F 2>nul
exit
