import importlib.metadata

__version__ = importlib.metadata.version("pandahub")

from lib.settings import PandaHubSettings
from pandahub.lib.PandaHub import PandaHub, PandaHubError #noqa: F401

pandahub_settings = PandaHubSettings()
