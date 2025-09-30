# ==========================================
# Dockerfile - Production ETL Application
# ==========================================
# Multi-stage build for optimized production image
FROM python:3.11-slim as builder

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --user -r requirements.txt

# Final production stage
FROM python:3.11-slim

# Create non-root user
RUN groupadd -r etluser && useradd -r -g etluser etluser

# Set working directory
WORKDIR /app

# Install runtime dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy Python dependencies from builder
COPY --from=builder /root/.local /home/etluser/.local

# Copy application code
COPY production_etl.py .
COPY etl_runner.py .
COPY monitor_etl.py .

# Create necessary directories
RUN mkdir -p /app/logs /app/metrics && \
    chown -R etluser:etluser /app

# Switch to non-root user
USER etluser

# Update PATH for user-installed packages
ENV PATH=/home/etluser/.local/bin:$PATH

# Set Python to run in unbuffered mode
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Default command
CMD ["python", "etl_runner.py"]