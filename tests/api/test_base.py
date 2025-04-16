import pytest
import httpx
from centrepoint.api.base import BaseAPI
from centrepoint.auth import CentrePointAuth


class FakeAuth(CentrePointAuth):
    def get_token(self):
        return "fake-token"


@pytest.fixture
def dummy_api():
    return BaseAPI(FakeAuth(), base_url="https://example.com")


def test_get_request(monkeypatch, dummy_api):
    def fake_get(self, url, **kwargs):
        assert url == "/resource"
        assert kwargs["headers"]["Authorization"] == "Bearer fake-token"
        return httpx.Response(200, json={"message": "ok"}, request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx.Client, "get", fake_get)

    response = dummy_api.get("/resource")
    assert response.status_code == 200
    assert response.json()["message"] == "ok"


def test_post_request(monkeypatch, dummy_api):
    def fake_post(self, url, **kwargs):
        assert url == "/submit"
        assert kwargs["headers"]["Authorization"] == "Bearer fake-token"
        assert kwargs["json"] == {"foo": "bar"}
        return httpx.Response(201, json={"status": "created"}, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx.Client, "post", fake_post)

    response = dummy_api.post("/submit", json={"foo": "bar"})
    assert response.status_code == 201
    assert response.json()["status"] == "created"

