"""TorqMind transactional email service (SMTP).

Prioridade de configuração:
  1) app.platform_email_profile (UI Plataforma → E-mail), quando smtp_host está preenchido
  2) variáveis SMTP_* do ambiente (/etc/torqmind/*.app.env) como bootstrap/fallback

A senha no banco fica criptografada (Fernet / TOTP_ENCRYPTION_KEY) e nunca é
exposta na API. Envio é best-effort: falha/ausência de SMTP não quebra auth.
"""

from __future__ import annotations

import logging
import smtplib
import ssl
from dataclasses import dataclass
from email.message import EmailMessage
from email.utils import formataddr
from html import escape
from typing import Any

from app.config import settings

logger = logging.getLogger("torqmind.email")


@dataclass(frozen=True)
class SmtpRuntime:
    enabled: bool
    host: str
    port: int
    user: str
    password: str
    use_ssl: bool
    use_tls: bool
    from_email: str
    from_name: str
    timeout_seconds: int
    source: str  # "database" | "environment"


def _warn_unowned_sender(sender: str) -> None:
    if sender.lower().endswith("@torqmind.com"):
        logger.warning(
            "SMTP sender %r uses torqmind.com — only use this once the domain is "
            "controlled with SPF/DKIM/DMARC; otherwise mail will fail deliverability.",
            sender,
        )


def _runtime_from_settings() -> SmtpRuntime:
    sender = (settings.smtp_from_email or settings.smtp_from or "").strip()
    if sender:
        _warn_unowned_sender(sender)
    return SmtpRuntime(
        enabled=bool(settings.smtp_enabled),
        host=(settings.smtp_host or "").strip(),
        port=int(settings.smtp_port or 587),
        user=(settings.smtp_user or "").strip(),
        password=settings.smtp_password or "",
        use_ssl=bool(settings.smtp_use_ssl),
        use_tls=bool(settings.smtp_use_tls),
        from_email=sender,
        from_name=(settings.smtp_from_name or "TorqMind").strip() or "TorqMind",
        timeout_seconds=int(settings.smtp_timeout_seconds or 20),
        source="environment",
    )


def _load_db_smtp_row() -> dict[str, Any] | None:
    """Lê o singleton de perfil SMTP. Falha de DB → None (cai no env)."""
    try:
        from app.db import get_conn

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            row = conn.execute(
                """
                SELECT
                  channel_name,
                  from_email,
                  smtp_enabled,
                  smtp_host,
                  smtp_port,
                  smtp_user,
                  smtp_password_encrypted,
                  smtp_use_ssl,
                  smtp_use_tls,
                  smtp_from_name,
                  smtp_timeout_seconds
                FROM app.platform_email_profile
                WHERE id = 1
                """
            ).fetchone()
        return dict(row) if row else None
    except Exception:  # noqa: BLE001 — bootstrap / migration ainda não aplicada
        logger.debug("platform_email_profile SMTP unavailable; using env", exc_info=True)
        return None


def _decrypt_db_password(encrypted: str | None) -> str:
    raw = (encrypted or "").strip()
    if not raw:
        return ""
    try:
        from app.totp import decrypt_secret

        return decrypt_secret(raw)
    except Exception:  # noqa: BLE001
        logger.exception("Failed to decrypt SMTP password from platform profile")
        return ""


def resolve_smtp_runtime() -> SmtpRuntime:
    """Config efetiva: DB (se host salvo) com senha DB→env; senão só env."""
    env = _runtime_from_settings()
    row = _load_db_smtp_row()
    if not row:
        return env

    host = (row.get("smtp_host") or "").strip()
    if not host:
        return env

    password = _decrypt_db_password(row.get("smtp_password_encrypted"))
    if not password:
        password = env.password

    sender = (row.get("from_email") or "").strip() or env.from_email
    if sender:
        _warn_unowned_sender(sender)
    from_name = (
        (row.get("smtp_from_name") or "").strip()
        or (row.get("channel_name") or "").strip()
        or env.from_name
    )
    port = int(row.get("smtp_port") or env.port or 587)
    timeout = int(row.get("smtp_timeout_seconds") or env.timeout_seconds or 20)

    return SmtpRuntime(
        enabled=bool(row.get("smtp_enabled")),
        host=host,
        port=port,
        user=(row.get("smtp_user") or "").strip(),
        password=password,
        use_ssl=bool(row.get("smtp_use_ssl")),
        use_tls=bool(row.get("smtp_use_tls")),
        from_email=sender,
        from_name=from_name or "TorqMind",
        timeout_seconds=timeout,
        source="database",
    )


def is_email_configured() -> bool:
    """True when SMTP is enabled and minimally configured to actually send."""
    cfg = resolve_smtp_runtime()
    return bool(cfg.enabled and cfg.host and cfg.from_email)


def _send_via_smtp(message: EmailMessage, cfg: SmtpRuntime) -> None:
    timeout = cfg.timeout_seconds
    if cfg.use_ssl:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(cfg.host, cfg.port, timeout=timeout, context=context) as server:
            if cfg.user:
                server.login(cfg.user, cfg.password)
            server.send_message(message)
        return

    with smtplib.SMTP(cfg.host, cfg.port, timeout=timeout) as server:
        server.ehlo()
        if cfg.use_tls:
            server.starttls(context=ssl.create_default_context())
            server.ehlo()
        if cfg.user:
            server.login(cfg.user, cfg.password)
        server.send_message(message)


def send_email(to_email: str, subject: str, html_body: str, text_body: str | None = None) -> bool:
    """Send a single email. Returns True on success, False on failure/unconfigured.

    Never raises — callers treat email as best-effort and must not leak whether
    a given address exists.
    """
    cfg = resolve_smtp_runtime()
    if not (cfg.enabled and cfg.host and cfg.from_email):
        logger.warning("SMTP not configured; skipping email to=%s subject=%r", to_email, subject)
        return False

    message = EmailMessage()
    message["From"] = formataddr((cfg.from_name or "TorqMind", cfg.from_email))
    message["To"] = to_email
    message["Subject"] = subject
    message.set_content(text_body or "Abra este e-mail em um cliente compatível com HTML.")
    message.add_alternative(html_body, subtype="html")

    try:
        _send_via_smtp(message, cfg)
        logger.info("Email sent to=%s subject=%r source=%s", to_email, subject, cfg.source)
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


def smtp_status_public() -> dict:
    """Status mascarado do SMTP para a UI da plataforma (sem senha).

    Valores efetivos (DB se host salvo, senão env) para o admin editar no formulário.
    """
    cfg = resolve_smtp_runtime()
    row = _load_db_smtp_row()
    password_in_db = bool((row or {}).get("smtp_password_encrypted"))
    return {
        "configured": is_email_configured(),
        "source": cfg.source,
        "enabled": cfg.enabled,
        "host": cfg.host or None,
        "port": cfg.port,
        "use_ssl": cfg.use_ssl,
        "use_tls": cfg.use_tls,
        "from_email": cfg.from_email or None,
        "from_name": cfg.from_name,
        "user": cfg.user or None,
        "user_configured": bool(cfg.user),
        "password_configured": bool(password_in_db or (cfg.source == "environment" and cfg.password)),
        "timeout_seconds": cfg.timeout_seconds,
    }


TEST_EMAIL_SUBJECT = "TorqMind — e-mail de teste"


def send_test_email(to_email: str, channel_name: str | None = None) -> bool:
    """E-mail de teste para o admin logado (platform)."""
    cfg = resolve_smtp_runtime()
    brand = escape((channel_name or cfg.from_name or "TorqMind").strip() or "TorqMind")
    html = f"""\
<!DOCTYPE html>
<html lang="pt-br">
  <body style="margin:0;padding:24px;background:#0d1317;font-family:system-ui,sans-serif;color:#e6edf3;">
    <div style="max-width:480px;margin:0 auto;background:#121a20;border-radius:16px;padding:28px;border:1px solid rgba(255,255,255,0.08);">
      <h1 style="margin:0 0 12px;font-size:20px;color:#fff;">{brand}</h1>
      <p style="margin:0 0 12px;font-size:14px;line-height:1.6;color:#c4d0da;">
        Este é um e-mail de teste do canal de notificações TorqMind.
        Se você recebeu esta mensagem, o SMTP está operacional.
      </p>
      <p style="margin:0;font-size:12px;color:#6b7d89;">Mensagem automática · não responda.</p>
    </div>
  </body>
</html>"""
    text = (
        f"{brand}\n\n"
        "Este é um e-mail de teste do canal de notificações TorqMind.\n"
        "Se você recebeu esta mensagem, o SMTP está operacional.\n"
    )
    return send_email(to_email, TEST_EMAIL_SUBJECT, html, text)
