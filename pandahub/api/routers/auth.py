from fastapi import APIRouter

from pandahub.api.internal.users import auth_backend, fastapi_users

router = APIRouter(
    prefix="/auth",
    tags=["auth"]
)

router.include_router(
    fastapi_users.get_auth_router(auth_backend)
)
router.include_router(
    fastapi_users.get_register_router()
)
router.include_router(
    fastapi_users.get_reset_password_router()
)
router.include_router(
    fastapi_users.get_verify_router()
)
