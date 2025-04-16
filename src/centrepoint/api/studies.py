# centrepoint/api/studies.py

from centrepoint.api.base import BaseAPI
from centrepoint.auth import CentrePointAuth
from centrepoint.models.study import PaginatedStudies

BASE_URL = "https://api.actigraphcorp.com/centrepoint/v3"

class StudiesAPI(BaseAPI):
    """Client for interacting with CentrePoint studies."""

    def __init__(self, auth: CentrePointAuth):
        """Initializes the StudiesAPI client.

        Args:
            auth (CentrePointAuth): An instance that handles authentication and token retrieval.
        """
        super().__init__(auth, base_url=BASE_URL)

    def list_studies(self, limit: int = 100, offset: int = 0) -> PaginatedStudies:
        """Retrieves a paginated list of all available studies.

        Args:
            limit (int, optional): Maximum number of studies to return. Defaults to 100.
            offset (int, optional): Offset for pagination. Defaults to 0.

        Returns:
            PaginatedStudies: A paginated container of Study objects.
        """
        response = self.get("/Studies", params={"limit": str(limit), "offset": str(offset)})
        response.raise_for_status()
        return PaginatedStudies.model_validate(response.json())
