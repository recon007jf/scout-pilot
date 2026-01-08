import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

PDL_API_KEY = os.getenv("PDL_API_KEY")
PDL_COMPANY_ENRICH_URL = "https://api.peopledatalabs.com/v5/company/enrich"
PDL_SEARCH_URL = "https://api.peopledatalabs.com/v5/person/search"
PDL_PERSON_ENRICH_URL = "https://api.peopledatalabs.com/v5/person/enrich"

class EnrichmentService:
    def __init__(self):
        if not PDL_API_KEY:
            raise ValueError("PDL_API_KEY not found in environment")
        self.api_key = PDL_API_KEY

    def find_person(self, company_name=None, website=None, location=None, title_keywords=None):
        """
        Finds a person using a 2-step "Identity Resolution" strategy.
        1. Resolve Company Identity (Name/Location -> Canonical ID)
        2. Search for Person (Canonical ID + Title)
        
        Returns the single best match.
        """
        if not (company_name or website) or not title_keywords:
            return {"error": "Missing usage criteria"}

        # STEP 1: Resolve Company Identity
        resolved_company = self._resolve_company(company_name, website, location)
        
        if not resolved_company:
            print(f"⚠️ Company Resolution Failed for: {company_name or website}")
            return {"success": False, "error": "Company Resolution Failed"}
            
        company_id = resolved_company.get('id')
        canonical_name = resolved_company.get('name')
        
        print(f"✅ Resolved Company: {canonical_name} (ID: {company_id})")

        # STEP 2: Person Search using Canonical ID (High Precision) - SQL Syntax
        # Query: SELECT * FROM person WHERE job_company_id='...' AND (job_title='...' OR ...)
        
        # Use Company ID for precise matching
        if company_id:
            company_clause = f"job_company_id='{company_id}'"
        else:
             return {"success": False, "error": "Resolved company has no ID"}
            
        # Title Criteria
        # SQL: job_title='Title' OR job_title='Title2'
        title_clauses = [f"job_title='{t}'" for t in title_keywords]
        title_part = " OR ".join(title_clauses)
        
        sql_query = f"SELECT * FROM person WHERE {company_clause} AND ({title_part})"
        
        params = {
            "sql": sql_query,
            "size": 1, 
            "pretty": True
        }
        
        headers = {
            "X-Api-Key": self.api_key,
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.get(PDL_SEARCH_URL, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data['status'] == 200:
                if data['data']:
                    person = data['data'][0]
                
                    # Check if we need to hydrate (Search often returns bools for emails)
                    work_email_raw = person.get('work_email')
                    pdl_id = person.get('id')
                    
                    if pdl_id and (work_email_raw is True or work_email_raw is None):
                        print(f"🔄 Search found {person.get('full_name')} but emails are masked. Enriching ID: {pdl_id}...")
                        return self._enrich_by_id(pdl_id)

                    # Return as is if data looks real
                    return self._format_person(person)
                else:
                     return {"success": False, "error": "No person match found"}
            else:
                return {"success": False, "error": "No match found"}
                
        except requests.exceptions.RequestException as e:
            return {"success": False, "error": str(e)}

    def _resolve_company(self, name, website, location):
        """
        Hits the Company Enrichment API to find the canonical profile.
        """
        params = {}
        if website:
            params["website"] = website
        if name:
            params["name"] = name
        if location:
            params["location"] = location # PDL auto-parses "OAKLAND, CA"
            
        # print(f"🔎 DEBUG: Enriching Company: {json.dumps(params)}")
        
        headers = {"X-Api-Key": self.api_key}
        
        try:
            response = requests.get(PDL_COMPANY_ENRICH_URL, headers=headers, params=params)
            
            # print(f"🔎 DEBUG: Response Code: {response.status_code}")
            
            if response.status_code == 200:
                # print(f"✅ DEBUG: Match Found: {response.json().get('name')}")
                return response.json()
            elif response.status_code == 404:
                return None
            else:
                print(f"⚠️ Company Enrich Error: {response.text}")
                return None
        except Exception as e:
            print(f"⚠️ Company Enrich Exception: {e}")
            return None

    def _enrich_by_id(self, pdl_id):
        params = {"pdl_id": pdl_id, "pretty": True}
        headers = {"X-Api-Key": self.api_key}
        
        try:
            response = requests.get(PDL_PERSON_ENRICH_URL, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data['status'] == 200 and data['data']:
                return self._format_person(data['data'])
            else:
                return {"success": False, "error": f"Enrichment failed for ID {pdl_id}"}
        except Exception as e:
            return {"success": False, "error": f"Enrichment error: {str(e)}"}

    def _format_person(self, person):
        # Safely extract emails handling potential list/bool weirdness if still present
        personal_emails = person.get('personal_emails', [])
        secondary_email = None
        if isinstance(personal_emails, list) and len(personal_emails) > 0:
            secondary_email = personal_emails[0]
            
        return {
            "success": True,
            "name": person.get('full_name'),
            "first_name": person.get('first_name'),
            "last_name": person.get('last_name'),
            "title": person.get('job_title'),
            "company": person.get('job_company_name'),
            "email": person.get('work_email'),
            "personal_email": secondary_email,
            "linkedin": person.get('linkedin_url'),
            "confidence": 1.0
        }

if __name__ == "__main__":
    # Quick module test
    print("Service initialized.")
