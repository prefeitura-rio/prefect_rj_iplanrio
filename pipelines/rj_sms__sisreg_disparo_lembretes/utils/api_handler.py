# -*- coding: utf-8 -*-
"""
API handler for Wetalkie API communication
Adapted from pipelines_rj_crm_registry for Prefect 3.x
"""

import re
from datetime import datetime, timedelta
from typing import Callable, Dict, Tuple

import requests
import simplejson
from iplanrio.pipelines_utils.logging import log


class ApiHandler:
    """
    Handle API calls to Wetalkie API with automatic token management
    """

    def __init__(
        self,
        base_url: str = None,
        username: str = None,
        password: str = None,
        header_type: str = None,
        login_route: str = "login",
        token_callback: Callable[[str, datetime], None] = lambda *_: None,
    ) -> None:
        if username is None or password is None:
            raise ValueError("Must be set refresh token or username with password")

        self._base_url = base_url
        self._username = username
        self._password = password
        self._login_route = login_route
        self._header_type = header_type
        self._token_callback = token_callback
        self._headers, self._token, self._expires_at = self._get_headers()

    def _find_token_path(self, data, path="", token_word="token"):
        """Return the path of the token in the json"""
        if isinstance(data, dict):
            for key, value in data.items():
                new_path = f"{path}.{key}" if path else key
                if token_word in key.lower():
                    return new_path
                result = self._find_token_path(value, new_path, token_word)
                if result:
                    return result
        elif isinstance(data, list):
            for index, item in enumerate(data):
                new_path = f"{path}[{index}]"
                result = self._find_token_path(item, new_path, token_word)
                if result:
                    return result
        return None

    def _get_token(self, data: dict) -> str:
        """Return the token from the json checking the path"""
        token_word = re.search(
            r"\b\w*token\w*\b", simplejson.dumps(data, ensure_ascii=False), re.IGNORECASE
        ).group()
        token_path = self._find_token_path(data=data, path="", token_word=token_word)
        keys = token_path.split(".")
        token_word = keys[-1]
        token = data
        for i in keys:
            token = token[i]
        return token

    def _get_headers(self) -> Tuple[Dict[str, str], str, datetime]:
        response = requests.post(
            f"{self._base_url}/{self._login_route}",
            headers={"accept": "application/json", "Content-Type": "application/json"},
            json={
                "username": self._username,
                "password": self._password,
            },
        )
        log(f"Status code: {response.status_code}")
        if response.status_code == 200:
            response_json = response.json()
            token = self._get_token(response_json)
            expires_word = [i for i in response_json.keys() if "expires" in i.lower()]
            expires_at = (
                datetime.now() + timedelta(seconds=30 * 60)
                if len(expires_word) == 0
                else datetime.now() + timedelta(seconds=int(response_json[expires_word[0]]))
            )
            log(f"Token {token[:2]}... expires at {expires_at}")
        else:
            raise Exception(f"Failed to get token: {response.status_code} - {response.text}")

        if self._header_type == "token":
            return {"token": f"{token}"}, token, expires_at
        return {"Authorization": f"Bearer {token}"}, token, expires_at

    def _refresh_token_if_needed(self) -> None:
        if self._expires_at <= datetime.now():
            self._headers, self._token, self._expires_at = self._get_headers()
            self._token_callback(self.get_token(), self.expires_at())

    def refresh_token(self):
        """
        refresh token
        """
        self._expires_at = datetime.now()
        self._refresh_token_if_needed()

    def get_token(self):
        """
        Return token
        """
        self._refresh_token_if_needed()
        if "Authorization" in self._headers.keys():
            return self._headers["Authorization"].split(" ")[1]
        return self._headers["token"]

    def expires_at(self):
        """
        Return de expire datetime
        """
        return self._expires_at

    def get(self, path: str, timeout: int = 120):
        """
        GET request
        """
        self._refresh_token_if_needed()
        response = requests.get(f"{self._base_url}{path}", headers=self._headers, timeout=timeout)
        response.raise_for_status()
        try:
            return response.json()
        except simplejson.JSONDecodeError:
            return response

    def put(self, path, json=None):
        """
        PUT request
        """
        self._refresh_token_if_needed()
        response = requests.put(f"{self._base_url}{path}", headers=self._headers, json=json)
        return response

    def post(self, path, data: dict = None, json: dict = None, files: dict = None):
        """
        POST request
        """
        self._refresh_token_if_needed()
        response = requests.post(
            url=f"{self._base_url}{path}",
            headers=self._headers,
            data=data,
            json=json,
            files=files,
        )
        return response