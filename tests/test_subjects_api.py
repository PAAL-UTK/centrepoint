# tests/test_subjects_api.py

import pytest
import httpx
from centrepoint.api.subjects import SubjectsAPI
from centrepoint.models.subject import PaginatedSubjects, Subject
from centrepoint.auth import CentrePointAuth

class FakeAuth(CentrePointAuth):
    def get_token(self):
        return "fake-token"


@pytest.fixture
def mock_subjects_response():
    return {
        "items": [
            {
                "id": 1,
                "studyId": 1414,
                "siteId": 11,
                "subjectIdentifier": "ABC123",
                "dob": None,
                "gender": "M",
                "timezone": "UTC",
                "weight": 75.0,
                "weightUnit": "kg",
                "height": 180.0,
                "heightUnit": "cm",
                "assignmentStatus": "active",
                "assignmentId": 999
            }
        ],
        "totalCount": 1,
        "limit": 100,
        "offset": 0
    }


@pytest.fixture
def mock_single_subject_response():
    return {
        "id": 42,
        "studyId": 1414,
        "siteId": 9,
        "subjectIdentifier": "XYZ999",
        "dob": None,
        "gender": "F",
        "timezone": "UTC",
        "weight": 65.0,
        "weightUnit": "kg",
        "height": 165.0,
        "heightUnit": "cm",
        "assignmentStatus": "inactive",
        "assignmentId": 888
    }


def test_list_subjects(monkeypatch, mock_subjects_response):
    def mock_get(self, url, **kwargs):
        assert url == "/Studies/1414/Subjects"
        assert kwargs["params"] == {"limit": "100", "offset": "0"}
        request = httpx.Request("GET", url)
        return httpx.Response(200, json=mock_subjects_response, request=request)

    monkeypatch.setattr("centrepoint.api.base.BaseAPI.get", mock_get)

    api = SubjectsAPI(FakeAuth())
    result = api.list_subjects(study_id=1414)

    assert isinstance(result, PaginatedSubjects)
    assert len(result.items) == 1
    assert result.items[0].subjectIdentifier == "ABC123"


def test_get_subject(monkeypatch, mock_single_subject_response):
    def mock_get(self, url, **kwargs):
        assert url == "/Studies/1414/Subjects/42"
        request = httpx.Request("GET", url)
        return httpx.Response(200, json=mock_single_subject_response, request=request)

    monkeypatch.setattr("centrepoint.api.base.BaseAPI.get", mock_get)

    api = SubjectsAPI(FakeAuth())
    subject = api.get_subject(study_id=1414, subject_id=42)

    assert isinstance(subject, Subject)
    assert subject.id == 42
    assert subject.subjectIdentifier == "XYZ999"
    assert subject.gender == "F"
