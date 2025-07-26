FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create workspace directory with proper permissions
RUN mkdir -p /workspace && chmod 777 /workspace

# Make the entry point executable
RUN chmod +x run_single_volume.py

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run as root to avoid permission issues
USER root

# Default command
ENTRYPOINT ["python", "run_single_volume.py"]
