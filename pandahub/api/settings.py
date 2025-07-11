from pydantic_settings import SettingsConfigDict

from lib.settings import PandaHubSettings


class PandaHubAppSettings(PandaHubSettings):
    """PandaHub app settings"""

    model_config = SettingsConfigDict(env_ignore_empty=True,secrets_dir='/run/secrets')

    registration_enabled: bool = True
    registration_admin_approval: bool = False
    create_indexes_with_project: bool = True
    pandahub_server_url: str = "0.0.0.0"
    pandahub_server_port: int =8002
    workers: int = 2
    pandahub_global_db_client: bool = False
    debug: bool = False



