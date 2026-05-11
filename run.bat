@echo off
REM Launcher for News Pulse pipeline (Windows).
echo Installing dependencies...
pip install -r requirements.txt

echo Starting data generator in a new window...
start "data_generator" cmd /k python data_generator.py

echo Starting streaming pipeline (Ctrl+C to stop)...
python news_pulse.py

echo Building dashboard...
python visualize.py

echo Done. Open output\dashboard.html in your browser.
pause
