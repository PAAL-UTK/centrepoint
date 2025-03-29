# centrepointAPI/api/data_access.py

from httpx import Client
from typing import Optional
from centrepoint.auth import CentrePointAuth
from centrepoint.models.data_access import PaginatedDataAccessFiles
from datetime import datetime

BASE_URL = "https://api.actigraphcorp.com/dataaccess/v3"

class DataAccessAPI:
    def __init__(self, auth: CentrePointAuth):
        self.auth = auth
        self.client = Client(base_url=BASE_URL)

    def list_files(
        self,
        study_id: int,
        subject_id: int,
        data_category: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        sort_order: Optional[str] = None,
        file_format: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> PaginatedDataAccessFiles:
        token = self.auth.get_token()
        headers = {"Authorization": f"Bearer {token}"}

        
        params: dict[str, str] = {
            "limit": str(limit),
            "offset": str(offset),
        }

        if start_date:
            params["startDate"] = start_date.isoformat()
        if end_date:
            params["endDate"] = end_date.isoformat()
        if sort_order:
            params["sortOrder"] = sort_order
        if file_format:
            params["fileFormat"] = file_format

        url = f"/files/studies/{study_id}/subjects/{subject_id}/{data_category}"
        response = self.client.get(url, headers=headers, params=params)
        response.raise_for_status()
        return PaginatedDataAccessFiles.model_validate(response.json())
