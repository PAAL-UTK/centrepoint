# centrepointAPI/api/studies.py

from httpx import Client
from centrepoint.auth import CentrePointAuth
from centrepoint.models.study import PaginatedStudies, Study

BASE_URL = "https://api.actigraphcorp.com/centrepoint/v3"

class StudiesAPI:
    def __init__(self, auth: CentrePointAuth):
        self.auth = auth
        self.client = Client(base_url=BASE_URL)

    def list_studies(self, limit: int = 100, offset: int = 0) -> PaginatedStudies:
        token = self.auth.get_token()
        headers = {"Authorization": f"Bearer {token}"}
        params = {"limit": limit, "offset": offset}
        
        response = self.client.get("/Studies", headers=headers, params=params)
        response.raise_for_status()
        return PaginatedStudies.model_validate(response.json())

    def get_study(self, study_id: int) -> Study:
        token = self.auth.get_token()
        headers = {"Authorization": f"Bearer {token}"}
        
        response = self.client.get(f"/Studies/{study_id}", headers=headers)
        response.raise_for_status()
        return Study.model_validate(response.json())
