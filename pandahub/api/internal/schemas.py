import uuid

from fastapi_users import schemas

from pandahub.api import pandahub_app_settings as ph_settings


class UserRead(schemas.BaseUser[uuid.UUID]):
    """Schema for reading a user."""



class UserCreate(schemas.BaseUserCreate):
    """Schema for creating a user."""

    is_active: bool = not ph_settings.registration_admin_approval


class UserUpdate(schemas.BaseUserUpdate):
    """Schema for updating a user."""

