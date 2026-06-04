"""TorqMind transactional email service (SMTP).

PT-BR: Serviço de envio de e-mail por SMTP, pronto para qualquer provedor.
Tudo é dirigido por variáveis de ambiente (ver app.config). Se o SMTP não
estiver configurado, o envio é ignorado de forma segura (retorna False e loga),
sem quebrar o fluxo de recuperação de senha — útil em dev/homolog.

Para ir ao ar, basta configurar um provedor SMTP. Sugestão gratuita e fácil:
  - Brevo (ex-Sendinblue): 300 e-mails/dia grátis. SMTP host smtp-relay.brevo.com,
    porta 587 (STARTTLS), usuário/senha do painel.
  - Mailtrap / Resend (SMTP) também funcionam.
Defina no .env (ou /etc/torqmind/prod.app.env):
  SMTP_ENABLED=true
  SMTP_HOST=smtp-relay.brevo.com
  SMTP_PORT=587
  SMTP_USER=<login_smtp>
  SMTP_PASSWORD=<senha_smtp>
  SMTP_USE_TLS=true
  SMTP_FROM=master@torqmind.com
  SMTP_FROM_NAME=TorqMind

EN: Env-driven SMTP sender. Safe no-op when unconfigured.
"""

from __future__ import annotations

import logging
import smtplib
import ssl
from email.message import EmailMessage
from email.utils import formataddr
from html import escape

from app.config import settings

logger = logging.getLogger("torqmind.email")


def is_email_configured() -> bool:
    """True when SMTP is enabled and minimally configured to actually send."""
    return bool(settings.smtp_enabled and settings.smtp_host and settings.smtp_from)


def _send_via_smtp(message: EmailMessage) -> None:
    timeout = settings.smtp_timeout_seconds
    if settings.smtp_use_ssl:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(settings.smtp_host, settings.smtp_port, timeout=timeout, context=context) as server:
            if settings.smtp_user:
                server.login(settings.smtp_user, settings.smtp_password)
            server.send_message(message)
        return

    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=timeout) as server:
        server.ehlo()
        if settings.smtp_use_tls:
            server.starttls(context=ssl.create_default_context())
            server.ehlo()
        if settings.smtp_user:
            server.login(settings.smtp_user, settings.smtp_password)
        server.send_message(message)


def send_email(to_email: str, subject: str, html_body: str, text_body: str | None = None) -> bool:
    """Send a single email. Returns True on success, False on failure/unconfigured.

    Never raises — callers treat email as best-effort and must not leak whether
    a given address exists.
    """
    if not is_email_configured():
        logger.warning("SMTP not configured; skipping email to=%s subject=%r", to_email, subject)
        return False

    message = EmailMessage()
    message["From"] = formataddr((settings.smtp_from_name or "TorqMind", settings.smtp_from))
    message["To"] = to_email
    message["Subject"] = subject
    message.set_content(text_body or "Abra este e-mail em um cliente compatível com HTML.")
    message.add_alternative(html_body, subtype="html")

    try:
        _send_via_smtp(message)
        logger.info("Email sent to=%s subject=%r", to_email, subject)
        return True
    except Exception:  # noqa: BLE001 — best-effort; never propagate to the request
        logger.exception("Failed to send email to=%s subject=%r", to_email, subject)
        return False


# ── Password reset email ─────────────────────────────────────

PASSWORD_RESET_SUBJECT = "Recuperação de senha do TorqMind"


def _password_reset_html(reset_url: str, nome: str | None, ttl_minutes: int) -> str:
    greeting = f"Olá, {escape(nome)}," if nome else "Olá,"
    safe_url = escape(reset_url, quote=True)
    return f"""\
<!DOCTYPE html>
<html lang="pt-br">
  <body style="margin:0;padding:0;background:#0d1317;font-family:'Segoe UI',system-ui,-apple-system,Roboto,Helvetica,Arial,sans-serif;color:#e6edf3;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#0d1317;padding:32px 16px;">
      <tr>
        <td align="center">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:480px;background:#121a20;border:1px solid rgba(255,255,255,0.08);border-radius:16px;overflow:hidden;">
            <tr>
              <td style="padding:24px 28px;border-bottom:1px solid rgba(255,255,255,0.08);">
                <span style="font-size:18px;font-weight:700;color:#ffffff;">TorqMind</span>
                <span style="font-size:12px;color:#9fb0bd;margin-left:8px;">BI operacional</span>
              </td>
            </tr>
            <tr>
              <td style="padding:28px;">
                <h1 style="margin:0 0 12px;font-size:20px;color:#ffffff;">Recuperação de senha</h1>
                <p style="margin:0 0 16px;font-size:14px;line-height:1.6;color:#c4d0da;">{greeting}</p>
                <p style="margin:0 0 16px;font-size:14px;line-height:1.6;color:#c4d0da;">
                  Recebemos um pedido para redefinir a senha da sua conta no TorqMind.
                  Clique no botão abaixo para criar uma nova senha.
                </p>
                <table role="presentation" cellpadding="0" cellspacing="0" style="margin:24px 0;">
                  <tr>
                    <td align="center" style="border-radius:12px;background:#b87333;">
                      <a href="{safe_url}" style="display:inline-block;padding:14px 28px;font-size:15px;font-weight:600;color:#ffffff;text-decoration:none;border-radius:12px;">
                        Redefinir minha senha
                      </a>
                    </td>
                  </tr>
                </table>
                <p style="margin:0 0 12px;font-size:13px;line-height:1.6;color:#9fb0bd;">
                  Este link é válido por {ttl_minutes} minutos e só pode ser usado uma vez.
                </p>
                <p style="margin:0 0 12px;font-size:13px;line-height:1.6;color:#9fb0bd;">
                  Se você não solicitou esta alteração, ignore este e-mail — sua senha continua a mesma.
                </p>
                <p style="margin:16px 0 0;font-size:12px;line-height:1.6;color:#6b7d89;word-break:break-all;">
                  Se o botão não funcionar, copie e cole este endereço no navegador:<br />
                  <a href="{safe_url}" style="color:#b87333;">{safe_url}</a>
                </p>
              </td>
            </tr>
            <tr>
              <td style="padding:18px 28px;border-top:1px solid rgba(255,255,255,0.08);font-size:11px;color:#6b7d89;">
                TorqMind · Mensagem automática, não responda este e-mail.
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>"""


def _password_reset_text(reset_url: str, nome: str | None, ttl_minutes: int) -> str:
    greeting = f"Olá, {nome}," if nome else "Olá,"
    return (
        f"{greeting}\n\n"
        "Recebemos um pedido para redefinir a senha da sua conta no TorqMind.\n"
        "Acesse o link abaixo para criar uma nova senha:\n\n"
        f"{reset_url}\n\n"
        f"Este link é válido por {ttl_minutes} minutos e só pode ser usado uma vez.\n"
        "Se você não solicitou esta alteração, ignore este e-mail — sua senha continua a mesma.\n\n"
        "TorqMind · Mensagem automática, não responda este e-mail."
    )


def send_password_reset_email(to_email: str, reset_url: str, nome: str | None, ttl_minutes: int) -> bool:
    """Send the branded password-reset email. Best-effort (see send_email)."""
    return send_email(
        to_email,
        PASSWORD_RESET_SUBJECT,
        _password_reset_html(reset_url, nome, ttl_minutes),
        _password_reset_text(reset_url, nome, ttl_minutes),
    )
