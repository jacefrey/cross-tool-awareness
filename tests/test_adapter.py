"""Adapter contract tests for the additive v0.4 bounded Protocol."""

from __future__ import annotations

import inspect

import pytest

import cross_tool_awareness
from cross_tool_awareness import adapter as adapter_module


class _BoundedStub:
    def invoke(
        self,
        claim,
        registration,
        fixed_notice,
        *,
        timeout_seconds,
    ):
        return None


class _UnboundedStub:
    def invoke(self, claim, registration, fixed_notice):
        return None


def _binds_protocol_call(adapter) -> bool:
    try:
        inspect.signature(adapter.invoke).bind(
            object(), object(), "notice", timeout_seconds=5
        )
    except TypeError:
        return False
    return True


def _protocol_type():
    protocol = getattr(adapter_module, "BoundedAdapter", None)
    assert protocol is not None, "BoundedAdapter must exist"
    return protocol


def test_bounded_adapter_is_exported_with_exact_invoke_shape():
    BoundedAdapter = _protocol_type()
    assert cross_tool_awareness.BoundedAdapter is BoundedAdapter
    assert "BoundedAdapter" in cross_tool_awareness.__all__

    parameters = list(inspect.signature(BoundedAdapter.invoke).parameters.values())
    assert [(parameter.name, parameter.kind) for parameter in parameters] == [
        ("self", inspect.Parameter.POSITIONAL_OR_KEYWORD),
        ("claim", inspect.Parameter.POSITIONAL_OR_KEYWORD),
        ("registration", inspect.Parameter.POSITIONAL_OR_KEYWORD),
        ("fixed_notice", inspect.Parameter.POSITIONAL_OR_KEYWORD),
        ("timeout_seconds", inspect.Parameter.KEYWORD_ONLY),
    ]
    assert [parameter.annotation for parameter in parameters[1:]] == [
        "DeliveryClaim",
        "PresenceRecord",
        "str",
        "int",
    ]
    assert inspect.signature(BoundedAdapter.invoke).return_annotation == "QueueAttempt"


def test_bounded_stub_supports_required_protocol_call_shape():
    assert _binds_protocol_call(_BoundedStub())


def test_stub_without_timeout_does_not_support_protocol_call_shape():
    assert not _binds_protocol_call(_UnboundedStub())


def test_timeout_is_keyword_only_for_conforming_stub():
    with pytest.raises(TypeError):
        _BoundedStub().invoke(object(), object(), "notice", 5)
