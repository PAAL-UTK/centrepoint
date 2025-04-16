# centrepointAPI/auth.py

import os
import httpx
from typing import Optional
from datetime import datetime, timedelta, timezone

AUTH_URL = "https://auth.actigraphcorp.com/connect/token"

class CentrePointAuth:
    """Handles bearer token authentication for the CentrePoint API.

    Automatically refreshes tokens using the client credentials grant flow.
    Relies on the following environment variables:
      - CP_CLIENT_ID
      - CP_CLIENT_SECRET
    """

    def __init__(self):
        """Initializes the CentrePointAuth with client credentials from environment."""
        self.client_id = os.getenv("CP_CLIENT_ID")
        self.client_secret = os.getenv("CP_CLIENT_SECRET")
        self.scope = "CentrePoint DataAccess"
        self.token: Optional[str] = None
        self.token_expires_at: Optional[datetime] = None

    def is_token_expired(self) -> bool:
        """Determines whether the current token is missing or expired.

        Returns:
            bool: True if the token is missing or expired, False otherwise.
        """
        now = datetime.now(timezone.utc)
        return not self.token or not self.token_expires_at or now >= self.token_expires_at

    def get_token(self) -> str:
        """Returns a valid bearer token, refreshing it if expired.

        Returns:
            str: A valid access token.
        """
        if self.is_token_expired():
            self._fetch_token()
        assert self.token is not None  # Silence the type checker
        return self.token

    def _fetch_token(self):
        """Fetches a new bearer token from the authentication endpoint."""
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

