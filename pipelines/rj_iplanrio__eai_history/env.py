# -*- coding: utf-8 -*-
import os

from iplanrio.pipelines_utils.env import getenv_or_action

# if file .env exists, load it
if os.path.exists("src/config/.env"):
    import dotenv

    dotenv.load_dotenv(dotenv_path="src/config/.env")

PROJECT_ID = getenv_or_action("EAI__PROJECT_ID")
LOCATION = getenv_or_action("EAI__LOCATION")
INSTANCE = getenv_or_action("EAI__INSTANCE")
DATABASE = getenv_or_action("EAI__DATABASE")
DATABASE_USER = getenv_or_action("EAI__DATABASE_USER")
DATABASE_PASSWORD = getenv_or_action("EAI__DATABASE_PASSWORD")
