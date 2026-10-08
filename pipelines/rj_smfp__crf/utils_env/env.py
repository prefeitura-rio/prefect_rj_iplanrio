import os

from iplanrio.pipelines_utils.env import getenv_or_action

# if file .env exists, load it
if os.path.exists("pipelines/rj_smfp__crf/utils_env/.env"):  # noqa
    import dotenv

    dotenv.load_dotenv(dotenv_path="pipelines/rj_smfp__crf/utils_env/.env", override=True)

CRF__BUCKET_NAME = getenv_or_action(key="CRF__BUCKET_NAME", action="ignore")
CRF__FOLDER_PREFIX_PERIODOS_EVENTOS = getenv_or_action(key="CRF__FOLDER_PREFIX_PERIODOS_EVENTOS", action="ignore")
