#!/usr/bin/env bash
# Launcher for News Pulse pipeline (Linux / Mac).
# Starts the generator and pipeline in parallel.

set -e
echo "Installing dependencies..."
pip install -r requirements.txt

echo "Starting data generator in background..."
python data_generator.py &
GEN_PID=$!
trap "kill $GEN_PID 2>/dev/null" EXIT

echo "Starting streaming pipeline (Ctrl+C to stop)..."
python news_pulse.py

echo "Building dashboard..."
python visualize.py

echo "Done. Open output/dashboard.html in your browser."
