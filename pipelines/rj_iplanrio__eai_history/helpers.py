"""Helpers of rj_iplanrio__eai_history."""

from pydantic_settings import BaseSettings


class EaiSettings(BaseSettings):
    """The Cloud SQL instances of the agent, for staging and for prod, from the environment."""

    EAI__PROJECT_ID: str | None = None
    EAI__LOCATION: str | None = None
    EAI__INSTANCE: str | None = None
    EAI__DATABASE: str | None = None
    EAI__DATABASE_USER: str | None = None
    EAI__DATABASE_PASSWORD: str | None = None
    EAI__PROJECT_ID_PROD: str | None = None
    EAI__LOCATION_PROD: str | None = None
    EAI__INSTANCE_PROD: str | None = None
    EAI__DATABASE_PROD: str | None = None
    EAI__DATABASE_USER_PROD: str | None = None
    EAI__DATABASE_PASSWORD_PROD: str | None = None
