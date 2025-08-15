# -*- coding: utf-8 -*-
import os

from iplanrio.pipelines_utils.env import getenv_or_action

# if file .env exists, load it
if os.path.exists("src/config/.env"):
    import dotenv

    dotenv.load_dotenv(dotenv_path="src/config/.env")

PG_URI = getenv_or_action("EAI__PG_URI")
DB_SSL = getenv_or_action("EAI__DB_SSL", default="true")
