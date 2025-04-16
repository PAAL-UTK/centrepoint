# centrepoint/api/subjects.py

from centrepoint.api.base import BaseAPI
from centrepoint.auth import CentrePointAuth
from centrepoint.models.subject import PaginatedSubjects, Subject

BASE_URL = "https://api.actigraphcorp.com/centrepoint/v3"

class SubjectsAPI(BaseAPI):
    """Client for interacting with CentrePoint subject data."""

    def __init__(self, auth: CentrePointAuth):
        """Initializes the SubjectsAPI client.

        Args:
            auth (CentrePointAuth): An instance that handles authentication and token retrieval.
        """
        super().__init__(auth, base_url=BASE_URL)

    def list_subjects(self, study_id: int, limit: int = 100, offset: int = 0) -> PaginatedSubjects:
        """Retrieves a paginated list of subjects associated with a study.

        Args:
            study_id (int): The ID of the study.
            limit (int, optional): Number of results per page. Defaults to 100.
            offset (int, optional): Offset for pagination. Defaults to 0.

        Returns:
            PaginatedSubjects: A paginated container of Subject objects.
        """
        response = self.get(f"/Studies/{study_id}/Subjects", params={"limit": str(limit), "offset": str(offset)})
        response.raise_for_status()
        return PaginatedSubjects.model_validate(response.json())

    def get_subject(self, study_id: int, subject_id: int) -> Subject:
        """Retrieves detailed metadata for a single subject in a study.

        Args:
            study_id (int): The ID of the study the subject is part of.
            subject_id (int): The ID of the subject.

        Returns:
            Subject: A Subject object populated with metadata.
        """
        response = self.get(f"/Studies/{study_id}/Subjects/{subject_id}")
        response.raise_for_status()
        return Subject.model_validate(response.json())

