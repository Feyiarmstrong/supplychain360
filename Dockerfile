FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && apt-get clean

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir --timeout 300 -r requirements.txt

# Copy ingestion scripts
COPY ingestion/ ./ingestion/
COPY .env.example .env.example

# Set working directory to ingestion
WORKDIR /app/ingestion

CMD ["python", "main.py"]