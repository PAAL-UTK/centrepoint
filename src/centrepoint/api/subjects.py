# centrepoint/api/subjects.py

from centrepoint.api.base import BaseAPI
from centrepoint.auth import CentrePointAuth
from centrepoint.models.subject import PaginatedSubjects, Subject

BASE_URL = "https://api.actigraphcorp.com/centrepoint/v3"

class SubjectsAPI(BaseAPI):
    def __init__(self, auth: CentrePointAuth):
        super().__init__(auth, base_url=BASE_URL)

    def list_subjects(self, study_id: int, limit: int = 100, offset: int = 0) -> PaginatedSubjects:
        response = self.get(f"/Studies/{study_id}/Subjects", params={"limit": str(limit), "offset": str(offset)})
        response.raise_for_status()
        return PaginatedSubjects.model_validate(response.json())

    def get_subject(self, study_id: int, subject_id: int) -> Subject:
        response = self.get(f"/Studies/{study_id}/Subjects/{subject_id}")
        response.raise_for_status()
        return Subject.model_validate(response.json())
