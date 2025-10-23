# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, Optional

import requests


class ApiHandler:
    """
    Handles API authentication and requests with automatic token refresh.

    - Performs login on initialization.
    - Automatically refreshes expired tokens (401).
    - Supports GET, POST, PUT.
    """

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        login_route: str = "users/login",
        token_type: str = "Bearer",
        timeout: int = 30,
    ):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.login_route = login_route.lstrip("/")
        self.token_type = token_type
        self.timeout = timeout

        self.token: Optional[str] = None
        self.headers: Dict[str, str] = {"Content-Type": "application/json"}

        self._login()

    # -------------------------
    # Internal methods
    # -------------------------

    def _login(self) -> None:
        """Authenticate and store token."""
        login_url = f"{self.base_url}/{self.login_route}"
        data = {"username": self.username, "password": self.password}

        response = requests.post(login_url, json=data, timeout=self.timeout)
        response.raise_for_status()
        body = response.json()

        token = (
            body.get("token")
            or body.get("access_token")
            or body.get("authToken")
            or body.get("jwt")
            or body.get("data", {}).get("item", {}).get("token")
        )

        if not token:
            raise ValueError("No token found in login response")

        self.token = token
        self.headers["Authorization"] = f"{self.token_type} {token}"

    def _refresh_token_if_needed(self, response: requests.Response) -> bool:
        """If 401, refresh token and return True."""
        if response.status_code == 401:
            self._login()
            return True
        return False

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        """Perform an HTTP request with auth and auto-refresh."""
        url = f"{self.base_url}/{path.lstrip('/')}"
        kwargs.setdefault("headers", self.headers)
        kwargs.setdefault("timeout", self.timeout)

        response = requests.request(method, url, **kwargs)

        if self._refresh_token_if_needed(response):
            response = requests.request(method, url, **kwargs)

        return response

    # -------------------------
    # Public interface
    # -------------------------

    def get(self, path: str, params: Optional[Dict[str, Any]] = None, **kwargs) -> requests.Response:
        return self._request("GET", path, params=params, **kwargs)

    def post(
        self,
        path: str,
        json: Optional[Dict[str, Any]] = None,
        data: Any = None,
        **kwargs,
    ) -> requests.Response:
        return self._request("POST", path, json=json, data=data, **kwargs)

    def put(
        self,
        path: str,
        json: Optional[Dict[str, Any]] = None,
        data: Any = None,
        **kwargs,
    ) -> requests.Response:
        return self._request("PUT", path, json=json, data=data, **kwargs)
