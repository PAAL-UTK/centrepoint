# centrepointAPI/auth.py

import os
import httpx
from typing import Optional
from datetime import datetime, timedelta, timezone

AUTH_URL = "https://auth.actigraphcorp.com/connect/token"

class CentrePointAuth:
    def __init__(self):
        self.client_id = os.getenv("CP_CLIENT_ID")
        self.client_secret = os.getenv("CP_CLIENT_SECRET")
        self.scope = "CentrePoint DataAccess"
        self.token: Optional[str] = None
        self.token_expires_at: Optional[datetime] = None

    def is_token_expired(self) -> bool:
        now = datetime.now(timezone.utc)
        return not self.token or not self.token_expires_at or now >= self.token_expires_at

    def get_token(self) -> str:
        if self.is_token_expired():
            self._fetch_token()
        assert self.token is not None  # Silence the type checker
        return self.token

    def _fetch_token(self):
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": self.scope,
        }

        response = httpx.post(AUTH_URL, data=data)
        response.raise_for_status()
        token_data = response.json()

        self.token = token_data["access_token"]
        expires_in = int(token_data["expires_in"])
        self.token_expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in - 60)
