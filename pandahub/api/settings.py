from pydantic_settings import SettingsConfigDict

from pandahub.lib.settings import PandaHubSettings


class PandaHubAppSettings(PandaHubSettings):
    """PandaHub app settings"""

    model_config = SettingsConfigDict(env_ignore_empty=True)

    registration_enabled: bool = True
    registration_admin_approval: bool = False
    pandahub_server_url: str = "0.0.0.0"
    pandahub_server_port: int =8002
    workers: int = 2
    debug: bool = False



