import uuid

from fastapi_users import schemas
from .. import pandahub_app_settings as ph_settings


class UserRead(schemas.BaseUser[uuid.UUID]):
    pass


class UserCreate(schemas.BaseUserCreate):
    is_active: bool = not ph_settings.registration_admin_approval


class UserUpdate(schemas.BaseUserUpdate):
    pass
