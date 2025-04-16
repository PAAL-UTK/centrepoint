import pytest
import httpx
from centrepoint.api.studies import StudiesAPI
from centrepoint.models.study import PaginatedStudies, Study
from centrepoint.auth import CentrePointAuth
from datetime import datetime

class FakeAuth(CentrePointAuth):
    def get_token(self):
        return "fake-token"

@pytest.fixture
def mock_study_response():
    return {
        "items": [
            {
                "id": 1,
                "name": "Study One",
                "organizationName": "Org",
                "createdDateTime": "2024-01-01T00:00:00Z",
                "studyStatus": "active",
                "defaultWearPosition": "wrist",
                "wearPositions": ["wrist", "hip"],
                "monitorDataCollectionMode": "continuous"
            }
        ],
        "totalCount": 1,
        "limit": 100,
        "offset": 0
    }

def test_list_studies(monkeypatch, mock_study_response):
    def mock_get(self, url, **kwargs):
        assert url == "/Studies"
        request = httpx.Request("GET", url)
        return httpx.Response(200, json=mock_study_response, request=request)

    monkeypatch.setattr("centrepoint.api.base.BaseAPI.get", mock_get)

    api = StudiesAPI(FakeAuth())
    result = api.list_studies()

    assert isinstance(result, PaginatedStudies)
    assert len(result.items) == 1
    assert result.items[0].name == "Study One"
    assert result.items[0].organizationName == "Org"

