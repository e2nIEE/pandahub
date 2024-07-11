import importlib.metadata

__version__ = importlib.metadata.version("pandahub")

from pandahub.lib.PandaHub import PandaHub, PandaHubError
