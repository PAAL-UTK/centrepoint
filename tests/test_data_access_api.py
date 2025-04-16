# tests/test_data_access_api.py

import pytest
import httpx
from centrepoint.api.data_access import DataAccessAPI
from centrepoint.models.data_access import PaginatedDataAccessFiles, DataAccessFile
from centrepoint.auth import CentrePointAuth
from datetime import datetime

class FakeAuth(CentrePointAuth):
    def get_token(self):
        return "fake-token"


@pytest.fixture
def mock_file_list_response():
    return {
        "items": [
            {
                "studyId": 1414,
                "subjectId": 123,
                "dataCategory": "imu",
                "date": "2025-03-10T00:00:00Z",
                "fileName": "PIL_JAM_LW_imu_2025-03-10.avro",
                "fileFormat": "avro",
                "modifiedDate": "2025-03-10T01:00:00Z",
                "downloadUrl": "https://example.com/file.avro",
                "downloadUrlExpiresOn": "2025-03-11T00:00:00Z"
            }
        ],
        "totalCount": 1,
        "limit": 100,
        "offset": 0
    }


def test_list_files(monkeypatch, mock_file_list_response):
    def mock_get(self, url, **kwargs):
        assert url == "/files/studies/1414/subjects/123/imu"
        assert kwargs["params"]["limit"] == "100"
        request = httpx.Request("GET", url)
        return httpx.Response(200, json=mock_file_list_response, request=request)

    monkeypatch.setattr("centrepoint.api.base.BaseAPI.get", mock_get)

    api = DataAccessAPI(FakeAuth())
    files = api.list_files(
        study_id=1414,
        subject_id=123,
        data_category="imu",
        start_date=datetime(2025, 3, 10),
        end_date=datetime(2025, 3, 11),
        file_format="avro"
    )

    assert isinstance(files, PaginatedDataAccessFiles)
    assert len(files.items) == 1
    assert files.items[0].fileName.endswith(".avro")
