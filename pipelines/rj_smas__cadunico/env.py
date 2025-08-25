# -*- coding: utf-8 -*-
import os

# if file .env exists, load it
if os.path.exists("pipelines/rj_smas__cadunico/.env"):  # noqa
    import dotenv

    dotenv.load_dotenv(dotenv_path="pipelines/rj_smas__cadunico/.env")
