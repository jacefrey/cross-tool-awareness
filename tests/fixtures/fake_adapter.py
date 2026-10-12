"""Fake WakeAdapter for dispatcher tests."""

from cross_tool_awareness.adapter import WakeAdapter, WakeNotice


class FakeAdapter(WakeAdapter):
    def __init__(self, *, wake_capability_value="codex_queue",
                 outcome="accepted_by_cli", enabled=True,
                 raise_exc=None):
        self._wake_capability = wake_capability_value
        self._outcome = outcome
        self._enabled = enabled
        self._raise_exc = raise_exc
        self.wake_calls = []

    @property
    def wake_capability(self) -> str:
        return self._wake_capability

    def is_enabled(self) -> bool:
        return self._enabled

    def wake(self, target_uuid, target_incarnation, notice: WakeNotice) -> str:
        self.wake_calls.append((target_uuid, target_incarnation, notice))
        if self._raise_exc:
            raise self._raise_exc
        return self._outcome
