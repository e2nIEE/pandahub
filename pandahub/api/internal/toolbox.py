from fastapi_mail import FastMail, MessageSchema, ConnectionConfig

from pandahub.api.internal import settings

email_conf = ConnectionConfig(
    MAIL_USERNAME=settings.MAIL_USERNAME,
    MAIL_PASSWORD=settings.MAIL_PASSWORD,
    MAIL_FROM=settings.MAIL_USERNAME,
    MAIL_PORT=settings.MAIL_PORT,
    MAIL_SERVER=settings.MAIL_SMTP_SERVER,
    MAIL_SSL_TLS=settings.MAIL_SSL_TLS,
    MAIL_STARTTLS=settings.MAIL_STARTTLS,
)

fast_mail = FastMail(email_conf)


async def send_password_reset_mail(user, token):
    to_address = user.email
    subject = "retoflow Password Reset"
    reset_link = f"{settings.PASSWORD_RESET_URL}?reset_token={token}"
    template = f"""\
Hello {user.email.split('@')[0]}, \n\n
you requested a password reset. To reset your password please click
the following link: \n\n
{reset_link}"
\n\n
If you didn't request a reset you can ignore this email. But be aware that
someone is trying to reset your password!
    """

    message = MessageSchema(
        subject=subject,
        recipients=[to_address],
        body=template,
        subtype="plain"
    )

    await fast_mail.send_message(message)


async def send_verification_email(user, token):
    to_address = user.email
    subject = "retoflow Email Verification"
    verification_link = f"{settings.EMAIL_VERIFY_URL}?verify_token={token}"
    template = f"""\
Hello {user.email.split('@')[0]}, \n\n
to complete your retoflow registration and to verify your email address please click
the following link: \n\n
{verification_link}
    """

    message = MessageSchema(
        subject=subject,
        recipients=[to_address],
        body=template,
        subtype="plain"
    )

    await fast_mail.send_message(message)
