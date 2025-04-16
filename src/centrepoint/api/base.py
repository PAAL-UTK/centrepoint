# centrepoint/api/base.py

import httpx
from centrepoint.auth import CentrePointAuth

class BaseAPI:
    """Base class for CentrePoint API clients that handles HTTP requests and authentication."""

    def __init__(self, auth: CentrePointAuth, base_url: str):
        """Initializes the BaseAPI with authentication and base URL.

        Args:
            auth (CentrePointAuth): Auth handler that provides access tokens.
            base_url (str): The base URL of the API being targeted.
        """
        self.auth = auth
        self.client = httpx.Client(base_url=base_url)

    def get(self, url: str, **kwargs) -> httpx.Response:
        """Sends an authenticated HTTP GET request.

        Args:
            url (str): The relative URL path.
            **kwargs: Additional arguments to pass to the request (e.g., params, headers).

        Returns:
            httpx.Response: The HTTP response object.
        """
        token = self.auth.get_token()
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {token}"
        return self.client.get(url, headers=headers, **kwargs)

    def post(self, url: str, **kwargs) -> httpx.Response:
        """Sends an authenticated HTTP POST request.

        Args:
            url (str): The relative URL path.
            **kwargs: Additional arguments to pass to the request (e.g., json, headers).

        Returns:
            httpx.Response: The HTTP response object.
        """
        token = self.auth.get_token()
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {token}"
        return self.client.post(url, headers=headers, **kwargs)
