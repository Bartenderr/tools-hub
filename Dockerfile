# Use Python 3.10 slim image for smaller size
FROM python:3.10-slim

# Update SSL certificates (for Oracle minimal environments)
RUN apt-get update && apt-get install -y ca-certificates && update-ca-certificates

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Create app directory and non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser
WORKDIR /app


# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY . .

# Create necessary directories and set permissions
RUN mkdir -p /app/logs /app/uploads && \
    chown -R appuser:appuser /app && \
    chmod -R 755 /app

# Switch to non-root user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8030/api/health')" || exit 1

# Expose port
EXPOSE 8030

# Run the application with gunicorn for production
CMD ["gunicorn", "--bind", "0.0.0.0:8030", "--workers", "2", "--threads", "4", "--timeout", "120", "--max-requests", "1000", "--max-requests-jitter", "100", "--reload", "app:create_app()"]
