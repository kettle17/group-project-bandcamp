#!/bin/bash
# Setup script for pipeline.

if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

source venv/bin/activate

echo "Installing required packages..."
pip install -r requirements.txt

if [ ! -f ".env" ]; then
    echo "Creating .env file..."
    cat > .env << EOL
EOL
fi

export PYTHONPATH="${PYTHONPATH}:$(pwd)"