# -*- coding: utf-8 -*-
import os

from iplanrio.pipelines_utils.env import getenv_or_action

# if file .env exists, load it
if os.path.exists("pipelines/rj_segur__forca_municipal/.env"):  # noqa
    import dotenv

    dotenv.load_dotenv(
        dotenv_path="pipelines/rj_segur__forca_municipal/.env", override=True
    )

API_FORCA_MUNICIPAL__API_LOGIN = getenv_or_action(key="API_FORCA_MUNICIPAL__API_LOGIN")
API_FORCA_MUNICIPAL__API_PASSWORD = getenv_or_action(
    key="API_FORCA_MUNICIPAL__API_PASSWORD"
)
API_FORCA_MUNICIPAL__API_URL = getenv_or_action(key="API_FORCA_MUNICIPAL__API_URL")
API_FORCA_MUNICIPAL__USE_PROXY_URL = str(
    getenv_or_action(key="API_FORCA_MUNICIPAL__USE_PROXY_URL")
).strip().lower() in ("true", "1", "yes")
API_FORCA_MUNICIPAL__PROXY_URL = getenv_or_action(key="API_FORCA_MUNICIPAL__PROXY_URL")
