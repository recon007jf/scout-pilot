# 🧭 Detailed Tech Stack Setup Walkthrough

This guide provides step-by-step instructions to get your environment ready.

## 🚨 Priority 1: Google Cloud & Sheets (The Database)

### Part A: Create the Project
1.  Go to the [Google Cloud Console](https://console.cloud.google.com/).
2.  Sign in with your Google account.
3.  Click the **Project Dropdown** (top left, next to the Google Cloud logo).
4.  Click **"New Project"** (top right of the modal).
5.  **Project Name:** Enter `point-c-scout`.
6.  Click **Create**. Wait a moment for it to finish.
7.  **Select the Project:** Click the notification bell or the project dropdown to select your new `point-c-scout` project.

### Part B: Enable APIs
1.  In the Search bar at the top, type **"Google Sheets API"**.
2.  Click on "Google Sheets API" (Marketplace).
3.  Click **Enable**.
4.  Repeat steps 1-3 for **"Google Drive API"**.

### Part C: Create Service Account (The "Robot User")
1.  In the Search bar, type **"Service Accounts"** and select it (under IAM & Admin).
2.  Click **"+ CREATE SERVICE ACCOUNT"** (top).
3.  **Service account name:** Enter `scout-bot`.
4.  Click **Create and Continue**.
5.  **Role:** Select **"Editor"** (Basic > Editor). *Note: For production, we'd be more restrictive, but this is fine for now.*
6.  Click **Done**.

### Part D: Get the Key (The Password)
1.  You should now see `scout-bot` in the list. Click on the **Email address** of the service account (it looks like `scout-bot@point-c-scout.iam.gserviceaccount.com`).
2.  Copy this email address and save it somewhere! You need it for Part E.
3.  Go to the **KEYS** tab (top bar).
4.  Click **ADD KEY** > **Create new key**.
5.  Select **JSON**.
6.  Click **Create**.
7.  A file will download to your computer. **This is the Master Key.**
8.  **Action:** Rename this file to `service_account.json` and move it into your project folder: `/Users/josephlf/.gemini/antigravity/brain/aac4f1c5-a379-4f0d-9e9e-fe1246cf2bd8/`.

### Part E: The Master Sheet
1.  Go to [Google Sheets](https://sheets.new).
2.  Name the sheet **"Point C Scout Master"**.
3.  Create 3 Tabs (at the bottom):
    *   Rename "Sheet1" to `Leads`
    *   Add a tab and name it `Logs`
    *   Add a tab and name it `DNC_List`
4.  **Share with the Robot:**
    *   Click the big **Share** button (top right).
    *   Paste the **Service Account Email** you copied in Part D (e.g., `scout-bot@...`).
    *   Make sure "Editor" is selected.
    *   Click **Send** (uncheck "Notify people" if you want).
5.  **Get the URL:** Copy the full URL of the spreadsheet.

---

## 🟠 Priority 2: Serper API (The Hunter)

1.  Go to [serper.dev](https://serper.dev).
2.  Click **Sign Up**.
3.  Create an account (Google Login is easiest).
4.  You will land on the Dashboard.
5.  Look for **"API Key"** on the main screen.
6.  Copy that string (it starts with `API_...` or similar).

---

## ✅ Final Step: Tell the Code
Once you have the **JSON file** in the folder and the **Serper Key**, let me know, and I will help you configure the `.streamlit/secrets.toml` file so the app can use them.
