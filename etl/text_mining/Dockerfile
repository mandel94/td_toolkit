# Dockerfile for Text Mining Pipeline
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy Python modules
COPY __init__.py ./
COPY *.py ./
COPY extractors/ ./extractors/
COPY scrapers/ ./scrapers/
COPY processors/ ./processors/
COPY messaging/ ./messaging/
COPY storage/ ./storage/

# Create data directory (will be mounted as volume)
RUN mkdir -p data/scraped

# Set Python path
ENV PYTHONPATH=/app:/app/..:${PYTHONPATH}

# Default command (can be overridden)
CMD ["python", "test_async_pipeline.py", "--test", "all"]
