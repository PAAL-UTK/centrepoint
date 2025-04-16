import pytest
import httpx
from datetime import datetime, timedelta, timezone
from centrepoint.auth import CentrePointAuth

@pytest.fixture
def mock_token_response():
    return {
        "access_token": "mock-access-token",
        "expires_in": 3600
    }

def test_fetch_token(monkeypatch, mock_token_response):
    def fake_post(url, data):
        request = httpx.Request("POST", url)
        return httpx.Response(200, json=mock_token_response, request=request)


    monkeypatch.setattr(httpx, "post", fake_post)

    auth = CentrePointAuth()
    token = auth.get_token()

    assert token == "mock-access-token"
    assert auth.token_expires_at is not None
    assert auth.token_expires_at > datetime.now(timezone.utc)


def test_token_caching(monkeypatch):
    auth = CentrePointAuth()
    auth.token = "cached-token"
    auth.token_expires_at = datetime.now(timezone.utc) + timedelta(minutes=5)

    monkeypatch.setattr(httpx, "post", lambda *args, **kwargs: pytest.fail("Should not fetch new token"))

    token = auth.get_token()
    assert token == "cached-token"

def test_token_refresh_on_expiry(monkeypatch, mock_token_response):
    def fake_post(url, **kwargs):
        request = httpx.Request("POST", url)
        return httpx.Response(200, json=mock_token_response, request=request)


    auth = CentrePointAuth()
    auth.token = "expired-token"
    auth.token_expires_at = datetime.now(timezone.utc) - timedelta(minutes=1)

    monkeypatch.setattr(httpx, "post", fake_post)

    token = auth.get_token()
    assert token == "mock-access-token"

