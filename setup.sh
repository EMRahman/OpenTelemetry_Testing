#!/usr/bin/env bash
set -e

# Change directory to script folder
cd "$(dirname "$0")"

echo "=========================================================="
echo "  Setting up OpenTelemetry Real-Time Alerting POC (macOS)"
echo "=========================================================="

if [ ! -d ".venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv .venv
fi

echo "📦 Installing/verifying dependencies..."
source .venv/bin/activate
pip install -r requirements.txt --quiet

echo ""
echo "✅ Environment is ready!"
echo ""
echo "To start the POC:"
echo "  1. Terminal 1 (Consumer & Dashboard):"
echo "       source .venv/bin/activate"
echo "       python -m dashboard.consumer"
echo ""
echo "  2. Terminal 2 (Producer):"
echo "       source .venv/bin/activate"
echo "       python -m producer.producer"
echo ""
echo "  3. Open your browser at: http://localhost:4318"
echo "=========================================================="
