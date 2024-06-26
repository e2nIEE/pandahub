from dotenv import load_dotenv
import os

def settings_bool(var_name, default=None):
    var = os.getenv(var_name)
    if var is None:
        return default
    if isinstance(var, str) and var.lower() == "true":
        return True
    elif isinstance(var, str) and var.lower() == "false":
        return False
    return False

def get_secret(key, default=None):
    secret = os.getenv(key, default)
    if secret and os.path.isfile(secret):
        with open(secret) as f:
            secret = f.read()
    return secret

# load variables from .env to environment variables
load_dotenv()

MONGODB_URL = get_secret("MONGODB_URL") or "mongodb://localhost:27017"
MONGODB_USER = get_secret("MONGODB_USER") or None
MONGODB_PASSWORD = get_secret("MONGODB_PASSWORD") or None

MONGODB_GLOBAL_DATABASE_URL = get_secret("MONGODB_GLOBAL_DATABASE_URL") or None
MONGODB_GLOBAL_DATABASE_USER = get_secret("MONGODB_GLOBAL_DATABASE_USER") or None
MONGODB_GLOBAL_DATABASE_PASSWORD = get_secret("MONGODB_GLOBAL_DATABASE_PASSWORD") or None
if not MONGODB_GLOBAL_DATABASE_URL:
    MONGODB_GLOBAL_DATABASE_URL = os.getenv("MONGODB_URL_GLOBAL_DATABASE") or None

EMAIL_VERIFICATION_REQUIRED = settings_bool("EMAIL_VERIFICATION_REQUIRED", default=False)

MAIL_USERNAME = os.getenv("MAIL_USERNAME") or "dummy@mail.de"
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD") or ""
MAIL_PORT = os.getenv("MAIL_PORT") or 587
MAIL_SMTP_SERVER = os.getenv("MAIL_SMTP_SERVER") or ""
MAIL_STARTTLS = settings_bool("MAIL_STARTTLS", default=True)
MAIL_SSL_TLS = settings_bool("MAIL_SSL_TLS", default=False)

PASSWORD_RESET_URL = os.getenv("PASSWORD_RESET_URL") or ""
EMAIL_VERIFY_URL = os.getenv("EMAIL_VERIFY_URL") or ""
SECRET = get_secret("SECRET") or None

REGISTRATION_ENABLED = settings_bool("REGISTRATION_ENABLED", default=True)
REGISTRATION_ADMIN_APPROVAL = settings_bool("REGISTRATION_ADMIN_APPROVAL", default=False)

CREATE_INDEXES_WITH_PROJECT = settings_bool("CREATE_INDEXES_WITH_PROJECT", default=True)

DEBUG = settings_bool("DEBUG", default=False)
PANDAHUB_SERVER_URL = os.getenv("PANDAHUB_SERVER_URL", "0.0.0.0")
PANDAHUB_SERVER_PORT = int(os.getenv('PANDAHUB_SERVER_PORT', 8002))
WORKERS = int(os.getenv('WORKER', 2))
