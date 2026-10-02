from typing import Annotated

from fastapi import Depends

from pandahub import PandaHub

from .internal.db import User
from .internal.users import fastapi_users

current_active_user = fastapi_users.current_user(active=True)


def pandahub(user: Annotated[User, Depends(current_active_user)]) -> PandaHub:
    """FastAPI dependency that returns a PandaHub instance for the current user."""
    return PandaHub(user_id=str(user.id))


PandahubDep = Annotated[PandaHub, Depends(pandahub)]
