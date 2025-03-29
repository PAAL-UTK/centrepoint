# centrepoint/api/data_access.py

from datetime import datetime
from typing import Optional

from centrepoint.api.base import BaseAPI
from centrepoint.auth import CentrePointAuth
from centrepoint.models.data_access import PaginatedDataAccessFiles

BASE_URL = "https://api.actigraphcorp.com/dataaccess/v3"

class DataAccessAPI(BaseAPI):
    def __init__(self, auth: CentrePointAuth):
        super().__init__(auth, base_url=BASE_URL)

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
        offset: int = 0,
    ) -> PaginatedDataAccessFiles:
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
        response = self.get(url, params=params)
        response.raise_for_status()
        return PaginatedDataAccessFiles.model_validate(response.json())
