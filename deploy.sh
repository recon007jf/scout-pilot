#!/bin/bash

# Configuration
APP_NAME="scout-dashboard"
REGION="us-central1" # Change if needed

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null
then
    echo "❌ gcloud CLI could not be found. Please install the Google Cloud SDK."
    exit 1
fi

echo "🦅 Preparing to deploy Point C Scout to Google Cloud Run..."

# Get Project ID
PROJECT_ID=$(gcloud config get-value project)
if [ -z "$PROJECT_ID" ]; then
    echo "❌ No default project set. Please run 'gcloud config set project YOUR_PROJECT_ID'"
    exit 1
fi
echo "   📂 Using Project: $PROJECT_ID"

# Check for credentials.json
if [ ! -f "credentials.json" ]; then
    echo "❌ credentials.json not found! It is required for the app to run."
    exit 1
fi

# Build the container image using Cloud Build
echo "   🏗️  Building container image (this may take a few minutes)..."
gcloud builds submit --tag gcr.io/$PROJECT_ID/$APP_NAME

# Deploy to Cloud Run
echo "   🚀 Deploying to Cloud Run..."
# Note: We are NOT setting secrets here for simplicity, assuming .env is copied or env vars are set manually.
# Ideally, we should parse .env and pass them as flags.

# Read .env and convert to comma-separated string for --set-env-vars
if [ -f ".env" ]; then
    ENV_VARS=$(grep -v '^#' .env | xargs | sed 's/ /,/g')
    echo "   🔑 Setting environment variables from .env..."
else
    ENV_VARS=""
    echo "   ⚠️  No .env file found."
fi

gcloud run deploy $APP_NAME \
  --image gcr.io/$PROJECT_ID/$APP_NAME \
  --platform managed \
  --region $REGION \
  --allow-unauthenticated \
  --memory 4Gi \
  --cpu 2 \
  --timeout 3600 \
  --set-env-vars "$ENV_VARS"

echo "✅ Deployment Complete!"
