import uuid

from fastapi_users import schemas
from pandahub.api.internal.settings import REGISTRATION_ADMIN_APPROVAL


class UserRead(schemas.BaseUser[uuid.UUID]):
    pass


class UserCreate(schemas.BaseUserCreate):
    is_active: bool = not REGISTRATION_ADMIN_APPROVAL


class UserUpdate(schemas.BaseUserUpdate):
    pass
