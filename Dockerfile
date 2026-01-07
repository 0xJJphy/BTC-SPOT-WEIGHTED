FROM python:3.11-slim

WORKDIR /app

# Disable Python output buffering for real-time logs
ENV PYTHONUNBUFFERED=1

# Install PostgreSQL client for health checks
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY btc_spot_pipeline.py .

# Run pipeline
CMD ["python", "btc_spot_pipeline.py"]
