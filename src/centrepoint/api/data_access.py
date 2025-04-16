# centrepoint/api/data_access.py

from datetime import datetime
from typing import Optional

from centrepoint.api.base import BaseAPI
from centrepoint.auth import CentrePointAuth
from centrepoint.models.data_access import PaginatedDataAccessFiles

BASE_URL = "https://api.actigraphcorp.com/dataaccess/v3"

class DataAccessAPI(BaseAPI):
    """Client for retrieving downloadable sensor data files from CentrePoint."""

    def __init__(self, auth: CentrePointAuth):
        """Initializes the DataAccessAPI client.

        Args:
            auth (CentrePointAuth): An instance that handles authentication and token retrieval.
        """
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
        """Lists sensor data files for a given subject in a study.

        Args:
            study_id (int): The ID of the study.
            subject_id (int): The ID of the subject.
            data_category (str): The type of data (e.g., 'imu', 'raw-accelerometer').
            start_date (datetime, optional): Filter files after this date.
            end_date (datetime, optional): Filter files before this date.
            sort_order (str, optional): Sort direction (e.g., 'asc', 'desc').
            file_format (str, optional): Desired file format ('avro', 'csv').
            limit (int, optional): Max number of files to return. Defaults to 100.
            offset (int, optional): Offset for pagination. Defaults to 0.

        Returns:
            PaginatedDataAccessFiles: A paginated list of available data files.
        """
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
