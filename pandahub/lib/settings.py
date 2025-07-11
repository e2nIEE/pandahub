from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import SecretStr


class PandaHubSettings(BaseSettings):
    """PandaHub settings"""

    model_config = SettingsConfigDict(env_ignore_empty=True, secrets_dir="/run/secrets")

    mongodb_url: SecretStr = "mongodb://localhost:27017"
    mongodb_user: SecretStr | None = None
    mongodb_password: SecretStr | None = None
    mongodb_global_database_url: SecretStr | None = None
    mongodb_global_database_user: SecretStr | None = None
    mongodb_global_database_password:  SecretStr | None = None
