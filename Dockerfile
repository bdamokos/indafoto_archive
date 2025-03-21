FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    sqlite3 \
    libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /data/db /data/archive /app/logs

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DB_PATH=/data/db/indafoto.db
ENV ARCHIVE_PATH=/data/archive

# Volume for persistent data
VOLUME ["/data"]

# Default command (will be overridden by docker-compose)
CMD ["python", "indafoto.py"] 