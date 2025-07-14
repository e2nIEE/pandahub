import os
from typing import Annotated

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import AfterValidator


def get_secret(secret: str) -> str:
    """If arg is a file path, read and return file content."""
    if secret and os.path.isfile(secret):
        with open(secret) as f:
            secret = f.read()
    return secret


SecretFromFile = Annotated[str, AfterValidator(get_secret)]


class PandaHubSettings(BaseSettings):
    """PandaHub settings"""

    model_config = SettingsConfigDict(env_ignore_empty=True)

    mongodb_url: str = "mongodb://localhost:27017"
    mongodb_user: SecretFromFile | None = None
    mongodb_password: SecretFromFile | None = None
    mongodb_global_database_url: str | None = None
    mongodb_global_database_user: str | None = None
    mongodb_global_database_password: str | None = None
    pandahub_global_db_client: bool = False
    create_indexes_with_project: bool = True


pandahub_settings = PandaHubSettings()
