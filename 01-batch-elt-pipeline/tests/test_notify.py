from types import SimpleNamespace

import pytest

from platform_ops import notify

TELEGRAM = {"TELEGRAM_BOT_TOKEN": "123:abc", "TELEGRAM_CHAT_ID": "42"}
SLACK = {"SLACK_WEBHOOK_URL": "https://hooks.slack.test/T/B/X"}


class FakeSession:
    def __init__(self, fail_on: str | None = None):
        self.posts = []
        self.fail_on = fail_on

    def post(self, url, json, timeout):
        self.posts.append((url, json))
        status = 500 if self.fail_on and self.fail_on in url else 200

        def raise_for_status():
            if status >= 400:
                raise RuntimeError(f"HTTP {status}")

        return SimpleNamespace(raise_for_status=raise_for_status)


def test_no_channels_configured_only_logs():
    session = FakeSession()
    assert notify.send("halo", env={}, session=session) == []
    assert session.posts == []


def test_sends_to_telegram_and_slack():
    session = FakeSession()
    sent = notify.send("halo", env={**TELEGRAM, **SLACK}, session=session)

    assert sent == ["telegram", "slack"]
    (tg_url, tg_body), (slack_url, slack_body) = session.posts
    assert tg_url == "https://api.telegram.org/bot123:abc/sendMessage"
    assert tg_body["chat_id"] == "42" and tg_body["text"] == "halo"
    assert slack_url == SLACK["SLACK_WEBHOOK_URL"] and slack_body == {"text": "halo"}


def test_telegram_needs_both_token_and_chat_id():
    assert notify.configured_channels({"TELEGRAM_BOT_TOKEN": "x"}) == []


def test_one_channel_failing_does_not_block_the_other():
    session = FakeSession(fail_on="api.telegram.org")
    assert notify.send("halo", env={**TELEGRAM, **SLACK}, session=session) == ["slack"]


def make_context(previous_state=None, exception=None):
    """previous_state: status task di run sebelumnya, atau "none" bila belum ada run."""
    prev_ti = None if previous_state == "none" else SimpleNamespace(state=previous_state)
    ti = SimpleNamespace(
        dag_id="lakehouse_monthly",
        task_id="silver",
        try_number=2,
        log_url="http://localhost:8080/log?x=1",
        get_previous_ti=lambda: prev_ti,
    )
    return {"task_instance": ti, "run_id": "manual__1", "exception": exception}


def test_failure_message_contains_key_facts_and_truncates_error():
    msg = notify.failure_message(make_context(exception=ValueError("x" * 2000)))

    assert "DAG: lakehouse_monthly" in msg
    assert "Task: silver (percobaan 2)" in msg
    assert "Log: http://localhost:8080/log?x=1" in msg
    error_line = next(line for line in msg.splitlines() if line.startswith("Error: "))
    assert len(error_line) <= len("Error: ") + notify.MAX_ERROR_CHARS


@pytest.fixture
def sent(monkeypatch):
    messages = []
    monkeypatch.setattr(notify, "send", lambda text, **kw: messages.append(text) or ["test"])
    return messages


@pytest.mark.parametrize(
    ("previous_state", "should_alert"),
    [("none", True), ("success", True), ("failed", False), (None, True)],
)
def test_state_change_alert_only_when_newly_failing(sent, previous_state, should_alert):
    notify.on_failure_state_change(make_context(previous_state))
    assert bool(sent) is should_alert


@pytest.mark.parametrize(
    ("previous_state", "should_alert"), [("failed", True), ("success", False), ("none", False)]
)
def test_recovery_sent_only_after_failure(sent, previous_state, should_alert):
    notify.on_recovery(make_context(previous_state))
    assert bool(sent) is should_alert
    if should_alert:
        assert sent[0].startswith("🟢")


def test_generic_failure_callback_always_alerts(sent):
    notify.on_task_failure(make_context("failed"))
    assert len(sent) == 1 and sent[0].startswith("🔴")
