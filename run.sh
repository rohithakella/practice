#!/bin/bash
set -e

cd "$(dirname "$0")"

source .venv/bin/activate

echo "=== Running main ==="
python src/practice/main.py

echo ""
echo "=== Running tests ==="
pytest tests/
