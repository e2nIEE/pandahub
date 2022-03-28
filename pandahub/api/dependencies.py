from fastapi import Depends

from pandahub import PandaHub
from .internal.models import UserDB
from .internal.users import fastapi_users

current_active_user = fastapi_users.current_user(active=True)


def pandahub(user: UserDB = Depends(current_active_user)):
    return PandaHub(user_id=str(user.id))
