# -*- coding: utf-8 -*-
# flake8: noqa:E501
# pylint: disable='line-too-long'
"""
Whitelist utility functions for SMAS pipelines.
"""
from typing import Dict, List, Optional
import requests

from iplanrio.pipelines_utils.env import getenv_or_action  # pylint: disable=E0611, E0401


def get_environment_config(env: str = "staging") -> Dict[str, str]:
    """
    Returns the configuration for the specified environment.

    Args:
        env (str): Environment ("staging" or "production")

    Returns:
        Dict[str, str]: Environment configuration

    Raises:
        ValueError: If the environment is not valid
    """
    if env == "staging":
        return {
            "api_base_url": getenv_or_action("WHITELIST_API_BASE_URL_STAGING"),
            "issuer": getenv_or_action("WHITELIST_ISSUER_STAGING"),
            "client_id": getenv_or_action("WHITELIST_CLIENT_ID_STAGING"),
            "client_secret": getenv_or_action("WHITELIST_CLIENT_SECRET_STAGING"),
        }
    elif env == "production":
        return {
            "api_base_url": getenv_or_action("WHITELIST_API_BASE_URL_PROD"),
            "issuer": getenv_or_action("WHITELIST_ISSUER_PROD"),
            "client_id": getenv_or_action("WHITELIST_CLIENT_ID_PROD"),
            "client_secret": getenv_or_action("WHITELIST_CLIENT_SECRET_PROD"),
        }
    else:
        raise ValueError(f"Invalid environment: {env}. Use 'staging' or 'production'.")


def validate_environment_config(config: Dict[str, str]) -> None:
    """
    Validates if all necessary configurations are present.

    Args:
        config (Dict[str, str]): Environment configuration

    Raises:
        ValueError: If any configuration is missing
    """
    required_keys = ["api_base_url", "issuer", "client_id", "client_secret"]
    missing_keys = [key for key in required_keys if not config.get(key)]

    if missing_keys:
        raise ValueError(f"Missing configurations: {missing_keys}. Check your environment variables.")


class BetaGroupManager:
    """Manage Beta group"""
    def __init__(self, issuer: str, client_id: str, client_secret: str, api_base_url: str):
        self.issuer = issuer
        self.client_id = client_id
        self.client_secret = client_secret
        self.api_base_url = api_base_url
        self.access_token = None

    def authenticate(self) -> bool:
        """Authenticates with the API and gets the access token"""
        url = f"{self.issuer}/protocol/openid-connect/token"

        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
            "scope": "profile email",
        }

        try:
            response = requests.post(url, data=payload)
            response.raise_for_status()

            token_data = response.json()
            self.access_token = token_data.get("access_token")

            if not self.access_token:
                print("Error: access_token not found in response")
                return False

            print("Authentication successful!")
            return True

        except requests.exceptions.RequestException as err:
            print(f"Authentication error: {err}")
            return False

    def get_headers(self) -> Dict[str, str]:
        """Returns headers with authorization"""
        if not self.access_token:
            raise ValueError("Access token not available. Run authenticate() first.")

        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    def list_groups(self) -> Optional[List[Dict]]:
        """Lists all existing groups"""
        url = f"{self.api_base_url}/groups"

        try:
            response = requests.get(url, headers=self.get_headers())
            response.raise_for_status()

            data = response.json()
            groups = data.get("groups", [])

            if groups:
                print(f"Found {len(groups)} groups:")
                for group in groups:
                    print(f"  - {group['name']} (ID: {group['id']})")
            else:
                print("No groups found.")

            return groups

        except requests.exceptions.RequestException as err:
            print(f"Error listing groups: {err}")
            return None

    def find_group_by_name(self, group_name: str) -> Optional[Dict]:
        """Finds a group by name"""
        groups = self.list_groups()
        if not groups:
            return None

        for group in groups:
            if group["name"] == group_name:
                return group

        return None

    def create_group(self, group_name: str) -> Optional[Dict]:
        """Creates a new group"""
        url = f"{self.api_base_url}/groups"

        payload = {"name": group_name}

        try:
            response = requests.post(url, json=payload, headers=self.get_headers())
            response.raise_for_status()

            group_data = response.json()
            print(f"Group '{group_name}' created successfully! ID: {group_data['id']}")

            return group_data

        except requests.exceptions.RequestException as err:
            print(f"Error creating group: {err}")
            return None

    def get_whitelist(self) -> Optional[Dict]:
        """
        Gets the complete list of numbers on the whitelist with pagination
        """
        all_whitelisted = []
        page = 1
        per_page = 100
        total_count = None

        print(f"📥 Fetching complete whitelist (pages of {per_page} records)...")

        while True:
            url = f"{self.api_base_url}/whitelist"
            params = {"page": page, "per_page": per_page}

            try:
                response = requests.get(url, headers=self.get_headers(), params=params)
                response.raise_for_status()

                data = response.json()

                if total_count is None:
                    total_count = data.get("total_count", 0)
                    print(f"📊 Total records on whitelist: {total_count}")

                page_whitelisted = data.get("whitelisted", [])
                all_whitelisted.extend(page_whitelisted)

                print(f"📥 Page {page}: {len(page_whitelisted)} records (total accumulated: {len(all_whitelisted)})")

                if (
                    len(page_whitelisted) == 0
                    or len(page_whitelisted) < per_page
                    or (total_count and len(all_whitelisted) >= total_count)
                ):
                    break

                page += 1

            except requests.exceptions.RequestException as err:
                print(f"Error getting whitelist (page {page}): {err}")
                if page == 1:
                    return None

                print(f"⚠️ Continuing with {len(all_whitelisted)} records obtained so far...")
                break

        print(f"✅ Complete whitelist obtained: {len(all_whitelisted)} total records")

        return {
            "total_count": len(all_whitelisted),
            "whitelisted": all_whitelisted,
        }

    def get_existing_numbers_set(self) -> set:
        """
        Returns a set with all numbers already registered on the whitelist
        """
        existing_numbers = set()
        whitelist_data = self.get_whitelist()

        if not whitelist_data:
            return existing_numbers

        for entry in whitelist_data.get("whitelisted", []):
            phone_number = entry.get("phone_number")
            if phone_number:
                clean_number = "".join(filter(str.isdigit, str(phone_number)))
                if len(clean_number) >= 10:
                    for norm_num in normalize_numbers(clean_number):
                        existing_numbers.add(norm_num)

        return existing_numbers

    def add_numbers_to_group(self, group_id, phone_numbers):
        """
        Adds phone numbers to a specific group
        """
        url = f"{self.api_base_url}/whitelist/bulk-add"

        payload = {"group_id": group_id, "phone_numbers": phone_numbers}

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()

            print(f"Numbers added to group {group_id} successfully")
            return True

        except requests.exceptions.RequestException as err:
            print(f"Error adding numbers to group: {err}")
            if hasattr(err, "response") and err.response is not None:
                print(f"Server response: {err.response.text}")
            return False


def normalize_numbers(clean_number: str) -> List[str]:
    """
    Normalizes a Brazilian phone number to always have '55' + DDD + number,
    and ALWAYS generates two versions: with and without the '9' after the DDD.
    """

    if not clean_number.startswith("55"):
        clean_number = "55" + clean_number

    local_number = clean_number[2:]

    if len(local_number) >= 11 and local_number[2] == "9":
        with_nine = clean_number
        without_nine = "55" + local_number[:2] + local_number[3:]
    else:
        without_nine = clean_number
        with_nine = "55" + local_number[:2] + "9" + local_number[2:]

    return list({with_nine, without_nine})
