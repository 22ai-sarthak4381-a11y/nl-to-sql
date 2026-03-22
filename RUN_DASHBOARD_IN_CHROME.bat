@echo off
:: This script starts the AI Business Dashboard (Flask + React) and opens it in Chrome.

echo ----------------------------------------------------
echo 🚀 AI Business Insights Dashboard - Chrome Launcher
echo ----------------------------------------------------

:: Navigate to backend and start Flask server
echo Starting Flask backend on http://localhost:5000 ...
start "Flask Backend" cmd /k "cd /d %~dp0backend && python app.py"

:: Wait until Flask is listening on port 5000
echo Waiting for Flask backend to be ready...
:waitFlask
timeout /t 2 /nobreak > nul
netstat -an | find "0.0.0.0:5000" >nul 2>&1 || netstat -an | find "127.0.0.1:5000" >nul 2>&1
if errorlevel 1 goto waitFlask
echo Flask backend is ready.

:: Navigate to frontend and start React dev server
echo Starting React frontend on http://localhost:5173 ...
start "React Frontend" cmd /k "cd /d %~dp0frontend && npm run dev"

:: Wait until React is listening on port 5173
echo Waiting for React frontend to be ready...
:waitReact
timeout /t 2 /nobreak > nul
netstat -an | find "0.0.0.0:5173" >nul 2>&1 || netstat -an | find "127.0.0.1:5173" >nul 2>&1
if errorlevel 1 goto waitReact
echo React frontend is ready.

echo 🌐 Opening Dashboard in Google Chrome...
start chrome "http://localhost:5173"

echo.
echo Dashboard is now running!
echo Close the Flask Backend and React Frontend windows to stop.
pause > nul
exit
