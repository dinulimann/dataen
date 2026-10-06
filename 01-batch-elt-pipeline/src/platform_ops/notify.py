"""Notifikasi ke Telegram dan/atau Slack untuk kegagalan DAG.

Kanal aktif ditentukan oleh environment variable; bila tidak ada yang diisi,
pesan hanya ditulis ke log. Pengiriman notifikasi tidak pernah melempar
exception, supaya kegagalan alert tidak menutupi error aslinya.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping

import requests

log = logging.getLogger(__name__)

MAX_ERROR_CHARS = 600


def configured_channels(env: Mapping[str, str] = os.environ) -> list[str]:
    channels = []
    if env.get("TELEGRAM_BOT_TOKEN") and env.get("TELEGRAM_CHAT_ID"):
        channels.append("telegram")
    if env.get("SLACK_WEBHOOK_URL"):
        channels.append("slack")
    return channels


def send(
    text: str, env: Mapping[str, str] = os.environ, session: requests.Session | None = None
) -> list[str]:
    """Kirim ke semua kanal yang dikonfigurasi. Mengembalikan kanal yang berhasil."""
    http = session or requests.Session()
    sent = []
    for channel in configured_channels(env):
        try:
            if channel == "telegram":
                base = env.get("TELEGRAM_API_BASE", "https://api.telegram.org")
                resp = http.post(
                    f"{base}/bot{env['TELEGRAM_BOT_TOKEN']}/sendMessage",
                    json={
                        "chat_id": env["TELEGRAM_CHAT_ID"],
                        "text": text,
                        "disable_web_page_preview": True,
                    },
                    timeout=10,
                )
            else:
                resp = http.post(env["SLACK_WEBHOOK_URL"], json={"text": text}, timeout=10)
            resp.raise_for_status()
            sent.append(channel)
        except Exception:  # noqa: BLE001 - alert tidak boleh menggagalkan callback
            log.exception("gagal mengirim notifikasi ke %s", channel)
    if not sent:
        log.warning("notifikasi tidak terkirim ke kanal mana pun:\n%s", text)
    return sent


def _truncate(text: str, limit: int = MAX_ERROR_CHARS) -> str:
    text = text.strip()
    return text if len(text) <= limit else text[: limit - 1] + "…"


def failure_message(context: Mapping) -> str:
    ti = context["task_instance"]
    error = context.get("exception")
    lines = [
        "🔴 Airflow: task gagal",
        f"DAG: {ti.dag_id}",
        f"Task: {ti.task_id} (percobaan {ti.try_number})",
        f"Run: {context['run_id']}",
    ]
    if error is not None:
        lines.append(f"Error: {_truncate(str(error))}")
    lines.append(f"Log: {ti.log_url}")
    return "\n".join(lines)


def recovery_message(context: Mapping) -> str:
    ti = context["task_instance"]
    return "\n".join(
        [
            "🟢 Airflow: pulih kembali",
            f"DAG: {ti.dag_id}",
            f"Task: {ti.task_id} kembali sukses setelah sebelumnya gagal",
            f"Run: {context['run_id']}",
        ]
    )


def _previous_task_state(context: Mapping) -> str | None:
    """Status task yang sama pada DAG run sebelumnya (None bila belum ada)."""
    previous_ti = context["task_instance"].get_previous_ti()
    return previous_ti.state if previous_ti else None


def on_task_failure(context: Mapping) -> None:
    """Callback umum: kirim alert setiap kali task gagal (setelah retry habis)."""
    send(failure_message(context))


def on_failure_state_change(context: Mapping) -> None:
    """Untuk DAG yang sering berjalan: alert hanya saat berubah dari sehat ke gagal."""
    if _previous_task_state(context) == "failed":
        log.info("task sudah gagal di run sebelumnya; alert tidak dikirim ulang")
        return
    send(failure_message(context))


def on_recovery(context: Mapping) -> None:
    """Kirim pesan pulih bila run sebelumnya gagal."""
    if _previous_task_state(context) == "failed":
        send(recovery_message(context))
