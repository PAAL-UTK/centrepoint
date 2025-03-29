# centrepointAPI/api/subjects.py

from httpx import Client
from centrepoint.auth import CentrePointAuth
from centrepoint.models.subject import Subject, PaginatedSubjects

BASE_URL = "https://api.actigraphcorp.com/centrepoint/v3"

class SubjectsAPI:
    def __init__(self, auth: CentrePointAuth):
        self.auth = auth
        self.client = Client(base_url=BASE_URL)

    def list_subjects(self, study_id: int, limit: int = 100, offset: int = 0) -> PaginatedSubjects:
        token = self.auth.get_token()
        headers = {"Authorization": f"Bearer {token}"}
        params = {"limit": limit, "offset": offset}

        response = self.client.get(f"/Studies/{study_id}/Subjects", headers=headers, params=params)
        response.raise_for_status()
        return PaginatedSubjects.model_validate(response.json())

    def get_subject(self, study_id: int, subject_id: int) -> Subject:
        token = self.auth.get_token()
        headers = {"Authorization": f"Bearer {token}"}

        response = self.client.get(f"/Studies/{study_id}/Subjects/{subject_id}", headers=headers)
        response.raise_for_status()
        return Subject.model_validate(response.json())
