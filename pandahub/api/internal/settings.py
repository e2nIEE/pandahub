from dotenv import load_dotenv
import os

def settings_bool(var_name):
    var = os.getenv(var_name)
    if var is None:
        return None
    if isinstance(var, str) and var.lower() == "true":
        return True
    elif isinstance(var, str) and var.lower() == "false":
        return False
    return False

# load variables from .env to environment variables
load_dotenv()

MONGODB_URL = os.getenv("MONGODB_URL") or "mongodb://localhost:27017"

EMAIL_VERIFICATION_REQUIRED = settings_bool("EMAIL_VERIFICATION_REQUIRED")

MAIL_USERNAME = os.getenv("MAIL_USERNAME") or "dummy@mail.de"
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD") or ""
MAIL_PORT = os.getenv("MAIL_PORT") or 587
MAIL_SMTP_SERVER = os.getenv("MAIL_SMTP_SERVER") or ""
MAIL_TLS = os.getenv("MAIL_TLS") or True
MAIL_SSL = os.getenv("MAIL_SSL") or False

PASSWORD_RESET_URL = os.getenv("PASSWORD_RESET_URL") or ""
EMAIL_VERIFY_URL = os.getenv("EMAIL_VERIFY_URL") or ""
SECRET = os.getenv("SECRET")
