#!/bin/zsh

#Start virtual environment
echo "Setting up the virtual environment..."
uv venv economist
source economist/bin/activate
# Install Python dependencies
uv pip install -r requirements.txt

echo "Python dependencies installed."
# Navigate to the data folder
cd data

# Run schema.sql into the DuckDB database
duckdb economist.duckdb < schema.sql

echo "Setup complete. The DuckDB database has been initialized with the schema."