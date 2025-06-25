#!/bin/bash

# Create virtual environment
python3 -m venv env

# Activate virtual environment
source env/bin/activate

# Upgrade pip and install packages
pip install --upgrade pip
pip install -r requirements.txt

echo "✅ Virtual environment set up and packages installed."
echo "🔁 To activate later, run: source env/bin/activate"