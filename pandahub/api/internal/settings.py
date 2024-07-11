from dotenv import load_dotenv
import os

def get_os_env(key, default=None):
    value = os.getenv(key, default)
    value = default if value == "" else value
    return value

def settings_bool(var_name, default=None):
    var = get_os_env(var_name)
    if var is None:
        return default
    if isinstance(var, str) and var.lower() == "true":
        return True
    elif isinstance(var, str) and var.lower() == "false":
        return False
    return False

def get_secret(key, default=None):
    secret = get_os_env(key, default)
    if secret and os.path.isfile(secret):
        with open(secret) as f:
            secret = f.read()
    return secret

# load variables from .env to environment variables
load_dotenv()

MONGODB_URL = get_secret("MONGODB_URL", "mongodb://localhost:27017")
MONGODB_USER = get_secret("MONGODB_USER")
MONGODB_PASSWORD = get_secret("MONGODB_PASSWORD")

MONGODB_GLOBAL_DATABASE_URL = get_secret("MONGODB_GLOBAL_DATABASE_URL")
MONGODB_GLOBAL_DATABASE_USER = get_secret("MONGODB_GLOBAL_DATABASE_USER")
MONGODB_GLOBAL_DATABASE_PASSWORD = get_secret("MONGODB_GLOBAL_DATABASE_PASSWORD")
if not MONGODB_GLOBAL_DATABASE_URL:
    MONGODB_GLOBAL_DATABASE_URL = get_os_env("MONGODB_URL_GLOBAL_DATABASE")

EMAIL_VERIFICATION_REQUIRED = settings_bool("EMAIL_VERIFICATION_REQUIRED", default=False)

MAIL_USERNAME = get_os_env("MAIL_USERNAME", "dummy@mail.de")
MAIL_PASSWORD = get_os_env("MAIL_PASSWORD", "")
MAIL_PORT = get_os_env("MAIL_PORT", 587)
MAIL_SMTP_SERVER = get_os_env("MAIL_SMTP_SERVER", "")
MAIL_STARTTLS = settings_bool("MAIL_STARTTLS", default=True)
MAIL_SSL_TLS = settings_bool("MAIL_SSL_TLS", default=False)

PASSWORD_RESET_URL = get_os_env("PASSWORD_RESET_URL", "")
EMAIL_VERIFY_URL = get_os_env("EMAIL_VERIFY_URL", "")
SECRET = get_secret("SECRET")

REGISTRATION_ENABLED = settings_bool("REGISTRATION_ENABLED", default=True)
REGISTRATION_ADMIN_APPROVAL = settings_bool("REGISTRATION_ADMIN_APPROVAL", default=False)

CREATE_INDEXES_WITH_PROJECT = settings_bool("CREATE_INDEXES_WITH_PROJECT", default=True)

DEBUG = settings_bool("DEBUG", default=False)
PANDAHUB_SERVER_URL = get_os_env("PANDAHUB_SERVER_URL", "0.0.0.0")
PANDAHUB_SERVER_PORT = int(get_os_env('PANDAHUB_SERVER_PORT', 8002))
WORKERS = int(get_os_env('WORKER', 2))
