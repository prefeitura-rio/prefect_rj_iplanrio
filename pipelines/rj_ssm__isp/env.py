import os

from iplanrio.pipelines_utils.env import getenv_or_action

# if file .env exists, load it
if os.path.exists("pipelines/rj_ssm__isp/.env"):  # noqa
    import dotenv

    dotenv.load_dotenv(dotenv_path="pipelines/rj_ssm__isp/.env", override=True)

ISPGEO_USER = getenv_or_action(key="ISPGEO_USER", action="ignore")
ISPGEO_PASS = getenv_or_action(key="ISPGEO_PASS", action="ignore")
ISPGEO_PORTAL_URL = getenv_or_action(key="ISPGEO_PORTAL_URL", action="ignore")
ISPGEO_LAYER_URL = getenv_or_action(key="ISPGEO_LAYER_URL", action="ignore")
ISPGEO_REFERER = getenv_or_action(key="ISPGEO_REFERER", action="ignore")
