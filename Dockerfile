# Multi-stage build for slim image
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# Final stage
FROM python:3.11-slim

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH

# Copy backend code
COPY . .

# Environment Defaults (Override in Cloud Run)
ENV PORT=8080
ENV ALLOWED_ORIGINS="https://scout-ui.vercel.app,http://localhost:3000"

# Cloud Run Entrypoint
CMD exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT}
