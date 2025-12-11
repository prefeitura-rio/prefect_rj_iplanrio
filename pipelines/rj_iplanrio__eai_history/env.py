# -*- coding: utf-8 -*-
import os

from iplanrio.pipelines_utils.env import getenv_or_action

# if file .env exists, load it
if os.path.exists("pipelines/rj_iplanrio__eai_history/.env"):  # noqa
    import dotenv

    dotenv.load_dotenv(dotenv_path="pipelines/rj_iplanrio__eai_history/.env")

PROJECT_ID = getenv_or_action("EAI__PROJECT_ID", action="ignore")
LOCATION = getenv_or_action("EAI__LOCATION", action="ignore")
INSTANCE = getenv_or_action("EAI__INSTANCE", action="ignore")
DATABASE = getenv_or_action("EAI__DATABASE", action="ignore")
DATABASE_USER = getenv_or_action("EAI__DATABASE_USER", action="ignore")
DATABASE_PASSWORD = getenv_or_action("EAI__DATABASE_PASSWORD", action="ignore")


PROJECT_ID_PROD = getenv_or_action("EAI__PROJECT_ID_PROD", action="ignore")
LOCATION_PROD = getenv_or_action("EAI__LOCATION_PROD", action="ignore")
INSTANCE_PROD = getenv_or_action("EAI__INSTANCE_PROD", action="ignore")
DATABASE_PROD = getenv_or_action("EAI__DATABASE_PROD", action="ignore")
DATABASE_USER_PROD = getenv_or_action("EAI__DATABASE_USER_PROD", action="ignore")
DATABASE_PASSWORD_PROD = getenv_or_action(
    "EAI__DATABASE_PASSWORD_PROD", action="ignore"
)
