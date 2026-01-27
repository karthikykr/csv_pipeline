# Lightweight Python base
FROM python:3.11-slim

# Set work directory
WORKDIR /app

# Install system deps (needed by polars)
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Default command
CMD ["python", "run_pipeline.py"]
