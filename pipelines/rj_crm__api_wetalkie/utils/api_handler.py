# -*- coding: utf-8 -*-
"""
API Handler for managing authenticated API requests with automatic token management
Migrated from pipelines_rj_crm_registry for prefect_rj_iplanrio
"""

from typing import Any, Dict, Optional

import requests
from iplanrio.pipelines_utils.logging import log


class ApiHandler:
    """
    Handles API authentication and request management with automatic token refresh.
    """

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        login_route: str = "users/login",
        token_type: str = "Bearer",
    ):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.login_route = login_route.lstrip("/")
        self.token_type = token_type
        self.token = None
        self.headers = {"Content-Type": "application/json"}

        # Perform initial login
        self._login()

    def _login(self):
        """Perform login and extract token from response"""
        login_url = f"{self.base_url}/{self.login_route}"
        login_data = {"username": self.username, "password": self.password}

        try:
            response = requests.post(login_url, json=login_data, timeout=30)
            response.raise_for_status()

            response_data = response.json()
            log(f"Login successful to {login_url}")

            # Try to extract token from various common response patterns
            token = None
            if "token" in response_data:
                token = response_data["token"]
            elif "access_token" in response_data:
                token = response_data["access_token"]
            elif "authToken" in response_data:
                token = response_data["authToken"]
            elif "jwt" in response_data:
                token = response_data["jwt"]
            elif (
                "data" in response_data and "item" in response_data["data"] and "token" in response_data["data"]["item"]
            ):
                token = response_data["data"]["item"]["token"]

            if token:
                self.token = token
                self.headers["Authorization"] = f"{self.token_type} {token}"
                log("Authentication token obtained successfully")
            else:
                log("Warning: No token found in login response")
                log(f"Response keys: {list(response_data.keys())}")

        except requests.RequestException as e:
            log(f"Login failed: {e}")
            raise Exception(f"Failed to authenticate with API: {e}")

    def _refresh_token_if_needed(self, response):
        """Check if token needs refresh based on response status"""
        if response.status_code == 401:
            log("Token expired, refreshing...")
            self._login()
            return True
        return False

    def get(self, path: str, params: Optional[Dict] = None, **kwargs) -> requests.Response:
        """Perform GET request with automatic token refresh"""
        url = f"{self.base_url}/{path.lstrip('/')}"

        response = requests.get(url, headers=self.headers, params=params, **kwargs)

        if self._refresh_token_if_needed(response):
            # Retry with new token
            response = requests.get(url, headers=self.headers, params=params, **kwargs)

        return response

    def post(
        self,
        path: str,
        json: Optional[Dict] = None,
        data: Optional[Any] = None,
        **kwargs,
    ) -> requests.Response:
        """Perform POST request with automatic token refresh"""
        url = f"{self.base_url}/{path.lstrip('/')}"

        response = requests.post(url, headers=self.headers, json=json, data=data, **kwargs)

        if self._refresh_token_if_needed(response):
            # Retry with new token
            response = requests.post(url, headers=self.headers, json=json, data=data, **kwargs)

        return response

    def put(
        self,
        path: str,
        json: Optional[Dict] = None,
        data: Optional[Any] = None,
        **kwargs,
    ) -> requests.Response:
        """Perform PUT request with automatic token refresh"""
        url = f"{self.base_url}/{path.lstrip('/')}"

        response = requests.put(url, headers=self.headers, json=json, data=data, **kwargs)

        if self._refresh_token_if_needed(response):
            # Retry with new token
            response = requests.put(url, headers=self.headers, json=json, data=data, **kwargs)

        return response

    def get_token(self) -> Optional[str]:
        """Return the current authentication token"""
        return self.token
