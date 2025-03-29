# centrepoint/api/base.py

import httpx
from centrepoint.auth import CentrePointAuth

class BaseAPI:
    def __init__(self, auth: CentrePointAuth, base_url: str):
        self.auth = auth
        self.client = httpx.Client(base_url=base_url)

    def get(self, url: str, **kwargs) -> httpx.Response:
        token = self.auth.get_token()
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {token}"
        return self.client.get(url, headers=headers, **kwargs)

    def post(self, url: str, **kwargs) -> httpx.Response:
        token = self.auth.get_token()
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {token}"
        return self.client.post(url, headers=headers, **kwargs)

