# Point C Scout

**Point C Scout** is a B2B Sales Intelligence Tool designed for simplicity and financial safety.

## Tech Stack

* **Language:** Python
* **UI:** Streamlit
* **Database:** Google Sheets
* **AI:** Google Gemini 1.5 Pro
* **Hosting:** Google Cloud Run

## Local Setup & Testing

### 1. Environment Setup

It is recommended to use a virtual environment.

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the Application

```bash
streamlit run app.py
```

The application will open in your default web browser at `http://localhost:8501`.

## Deployment (Google Cloud Run)

*Instructions for deploying to Cloud Run will be added in the "Circuit Breakers" phase.*

---

## 🚀 2026 Architecture Pivot (Effective Dec 2025)

### 1. Enrichment Engine: People Data Labs (PDL)

- **Status:** ACTIVE
* **Change:** Replaced Clay/Apollo webhooks with direct Python-to-PDL API integration.
* **Why:** Commercial OEM rights, API-first stability, and cost control.

### 2. Frontend Evolution: Next.js + Supabase

- **Status:** PLANNED (Jan 2026)
* **Legacy:** Streamlit is officially **DEPRECATED**. No new features will be built in Streamlit.
* **New Stack:** Next.js (React), Tailwind CSS, Supabase Auth.
* **Why:** Commercial-grade UX, better state management, and separation of concerns.

### 3. System Design (The "Clean" Core)

- **Core Engine (Python):** Pure business logic (Scoring, Dedupe, Enrichment). Must remain UI-Agnostic.
* **API Layer:** Thin interface exposing Core to the Client.
* **UI Client:** Next.js Dashboard (Talks only to API).

---
