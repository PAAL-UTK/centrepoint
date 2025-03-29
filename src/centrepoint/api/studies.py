# centrepoint/api/studies.py

from centrepoint.api.base import BaseAPI
from centrepoint.auth import CentrePointAuth
from centrepoint.models.study import PaginatedStudies

BASE_URL = "https://api.actigraphcorp.com/centrepoint/v3"

class StudiesAPI(BaseAPI):
    def __init__(self, auth: CentrePointAuth):
        super().__init__(auth, base_url=BASE_URL)

    def list_studies(self, limit: int = 100, offset: int = 0) -> PaginatedStudies:
        response = self.get("/Studies", params={"limit": str(limit), "offset": str(offset)})
        response.raise_for_status()
        return PaginatedStudies.model_validate(response.json())
