FROM python:3.9-slim

WORKDIR /app

# Install system dependencies (kept minimal)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage cache
COPY requirements.txt .

# Install Python dependencies
RUN pip3 install -r requirements.txt

# Copy the rest of the application
COPY . .

# Expose the port Streamlit runs on (Cloud Run expects 8080 by default)
EXPOSE 8080

# Run the application
# server.port=8080 is crucial for Cloud Run
# server.address=0.0.0.0 is required for external access
ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8080", "--server.address=0.0.0.0", "--server.maxUploadSize=4096", "--server.enableCORS=false", "--server.enableXsrfProtection=false"]
