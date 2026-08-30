from __future__ import annotations

import asyncio
import hashlib
import importlib
import inspect
import sys
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any

from ..errors import ConfigurationError, RuntimeError
from ..events import sanitize_event_data
from ..fingerprint import implementation_identity
from ..models import JsonValue
from .base import AgentExecutionContext, AgentRequest, AgentResult, AgentUsage

QueryFactory = Callable[..., AsyncIterator[Any]]
OptionsFactory = Callable[[dict[str, Any]], Any]


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class ClaudeCodeOptions:
    """Focused, durable configuration for the Claude Agent SDK adapter.

    Advanced SDK integrations can use ``options_factory``. The factory receives the
    PaC-derived option dictionary and must return ``ClaudeAgentOptions`` (or a compatible
    object). Provider credentials should be supplied by the SDK's normal environment or
    secret-provider integration, not embedded in this configuration.
    """

    model: str | None = None
    fallback_model: str | None = None
    system_prompt: str | None = None
    max_turns: int | None = None
    max_budget_usd: Decimal | None = None
    tools: Sequence[str] | None = None
    allowed_tools: tuple[str, ...] = ()
    disallowed_tools: tuple[str, ...] = ()
    permission_mode: str | None = None
    setting_sources: tuple[str, ...] = ()
    cli_path: str | None = None
    max_thinking_tokens: int | None = None
    effort: str | None = None
    env: Mapping[str, str] = field(default_factory=dict)
    options_factory: OptionsFactory | None = field(default=None, compare=False, repr=False)

    def __post_init__(self) -> None:
        if self.max_turns is not None and self.max_turns < 1:
            raise ConfigurationError("Claude Code max_turns must be >= 1")
        if self.max_budget_usd is not None and self.max_budget_usd < 0:
            raise ConfigurationError("Claude Code max_budget_usd cannot be negative")
        if self.max_thinking_tokens is not None and self.max_thinking_tokens < 0:
            raise ConfigurationError("Claude Code max_thinking_tokens cannot be negative")
        object.__setattr__(self, "tools", tuple(self.tools) if self.tools is not None else None)
        object.__setattr__(self, "allowed_tools", tuple(self.allowed_tools))
        object.__setattr__(self, "disallowed_tools", tuple(self.disallowed_tools))
        object.__setattr__(self, "setting_sources", tuple(self.setting_sources))
        object.__setattr__(self, "env", MappingProxyType(dict(self.env)))

    def fingerprint_config(self) -> dict[str, JsonValue]:
        factory = self.options_factory
        factory_identity = implementation_identity(factory) if factory is not None else None
        return {
            "model": self.model,
            "fallback_model": self.fallback_model,
            "system_prompt_sha256": _hash(self.system_prompt) if self.system_prompt else None,
            "max_turns": self.max_turns,
            "max_budget_usd": str(self.max_budget_usd) if self.max_budget_usd is not None else None,
            "tools": list(self.tools) if self.tools is not None else None,
            "allowed_tools": list(self.allowed_tools),
            "disallowed_tools": list(self.disallowed_tools),
            "permission_mode": self.permission_mode,
            "setting_sources": list(self.setting_sources),
            "cli_path": self.cli_path,
            "max_thinking_tokens": self.max_thinking_tokens,
            "effort": self.effort,
            # Environment values may contain credentials. Only stable variable names
            # participate in the fingerprint; resolved values never do.
            "env_keys": list(sorted(self.env)),
            "options_factory": factory_identity,
        }


class ClaudeCodeRuntime:
    """Execute PaC agent calls with Anthropic's Claude Agent SDK for Python."""

    runtime_name = "claude-code"

    def __init__(
        self,
        options: ClaudeCodeOptions | None = None,
        *,
        query_factory: QueryFactory | None = None,
    ) -> None:
        self.options = options or ClaudeCodeOptions()
        self._query_factory = query_factory

    def fingerprint_config(self) -> JsonValue:
        return self.options.fingerprint_config()

    @staticmethod
    def _sdk() -> Any:
        try:
            return importlib.import_module("claude_agent_sdk")
        except ImportError as exc:
            # A partially imported SDK can remain in sys.modules after an optional
            # dependency fails. Remove it so a later correctly configured worker can retry.
            sys.modules.pop("claude_agent_sdk", None)
            raise ConfigurationError(
                "ClaudeCodeRuntime requires the 'claude-code' extra "
                "(pip install 'process-as-code[claude-code]')"
            ) from exc

    def _option_values(
        self,
        request: AgentRequest,
        context: AgentExecutionContext,
        session_id: str | None,
    ) -> dict[str, Any]:
        configured = self.options
        values: dict[str, Any] = {
            "cwd": str(context.cwd),
            "model": request.model or configured.model,
            "fallback_model": configured.fallback_model,
            "system_prompt": configured.system_prompt,
            "max_turns": configured.max_turns,
            "max_budget_usd": (
                float(configured.max_budget_usd)
                if configured.max_budget_usd is not None
                else None
            ),
            "tools": list(configured.tools) if configured.tools is not None else None,
            "allowed_tools": list(configured.allowed_tools) or None,
            "disallowed_tools": list(configured.disallowed_tools) or None,
            "permission_mode": configured.permission_mode,
            "setting_sources": list(configured.setting_sources) or None,
            "cli_path": configured.cli_path,
            "max_thinking_tokens": configured.max_thinking_tokens,
            "effort": configured.effort,
            "env": dict(configured.env) or None,
            "resume": session_id,
            "output_format": (
                {"type": "json_schema", "schema": request.output_schema}
                if request.output_schema is not None
                else None
            ),
        }
        return {key: value for key, value in values.items() if value is not None}

    @staticmethod
    def _session_id(value: JsonValue | None) -> str | None:
        if not isinstance(value, dict):
            return None
        session_id = value.get("session_id")
        return session_id if isinstance(session_id, str) and session_id else None

    async def execute(
        self, request: AgentRequest, context: AgentExecutionContext
    ) -> AgentResult:
        sdk = self._sdk()
        session_id = self._session_id(context.load_session(self.runtime_name))
        option_values = self._option_values(request, context, session_id)
        options = (
            self.options.options_factory(option_values)
            if self.options.options_factory is not None
            else sdk.ClaudeAgentOptions(**option_values)
        )
        query = self._query_factory or sdk.query
        messages: list[Any] = []
        result_message: Any = None
        last_model: str | None = None
        stream: Any = None
        try:
            stream = query(prompt=request.prompt, options=options)
            if inspect.isawaitable(stream):
                stream = await stream
            async for message in stream:
                messages.append(message)
                if self._is_result_message(message, sdk):
                    result_message = message
                model = getattr(message, "model", None)
                if isinstance(model, str):
                    last_model = model
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            details = []
            exit_code = getattr(exc, "exit_code", None)
            if isinstance(exit_code, int):
                details.append(f"exit_code={exit_code}")
            suffix = f" ({', '.join(details)})" if details else ""
            raise RuntimeError(
                f"Claude Code SDK {type(exc).__name__}{suffix}"
            ) from exc
        finally:
            close = getattr(stream, "aclose", None)
            if close is not None:
                await close()

        if result_message is None:
            raise RuntimeError("Claude Code query ended without a terminal ResultMessage")

        subtype = getattr(result_message, "subtype", None)
        is_error = bool(getattr(result_message, "is_error", False))
        if is_error or (isinstance(subtype, str) and subtype not in {"success", "completed"}):
            status = subtype if isinstance(subtype, str) else "error"
            api_status = getattr(result_message, "api_error_status", None)
            suffix = f", api_status={api_status}" if api_status is not None else ""
            raise RuntimeError(f"Claude Code query failed: subtype={status}{suffix}")

        returned_session = getattr(result_message, "session_id", None)
        if isinstance(returned_session, str) and returned_session:
            context.save_session(self.runtime_name, {"session_id": returned_session})
        else:
            returned_session = session_id

        if request.output_schema is not None:
            output = getattr(result_message, "structured_output", None)
            if output is None:
                raise RuntimeError(
                    "Claude Code returned no structured output for the requested schema"
                )
        else:
            output = getattr(result_message, "result", None)
            if output is None:
                output = self._assistant_text(messages)

        usage, usage_metadata = self._usage(result_message)
        result_model = getattr(result_message, "model", None)
        if not isinstance(result_model, str):
            result_model = last_model or request.model or self.options.model
        invocation_id = getattr(result_message, "uuid", None)
        if invocation_id is not None:
            invocation_id = str(invocation_id)
        metadata: dict[str, JsonValue] = {
            "runtime": self.runtime_name,
            "subtype": subtype if isinstance(subtype, str) else None,
            "stop_reason": sanitize_event_data(getattr(result_message, "stop_reason", None)),
            "terminal_reason": sanitize_event_data(
                getattr(result_message, "terminal_reason", None)
            ),
            "duration_ms": self._integer(getattr(result_message, "duration_ms", None)),
            "duration_api_ms": self._integer(
                getattr(result_message, "duration_api_ms", None)
            ),
            "num_turns": self._integer(getattr(result_message, "num_turns", None)),
            "permission_denials": len(getattr(result_message, "permission_denials", ()) or ()),
            **usage_metadata,
        }
        return AgentResult(
            output=output,
            provider="anthropic",
            model=result_model,
            invocation_id=invocation_id,
            session_id=returned_session,
            usage=usage,
            metadata=metadata,
            raw=tuple(messages),
        )

    @staticmethod
    def _is_result_message(message: Any, sdk: Any) -> bool:
        result_type = getattr(sdk, "ResultMessage", None)
        return (
            isinstance(message, result_type)
            if isinstance(result_type, type)
            else type(message).__name__ == "ResultMessage"
        )

    @staticmethod
    def _assistant_text(messages: Sequence[Any]) -> str | None:
        chunks: list[str] = []
        for message in messages:
            for block in getattr(message, "content", ()) or ():
                text = getattr(block, "text", None)
                if isinstance(text, str):
                    chunks.append(text)
        return "".join(chunks) or None

    @staticmethod
    def _value(source: Any, *names: str) -> Any:
        for name in names:
            if isinstance(source, Mapping) and name in source:
                return source[name]
            value = getattr(source, name, None)
            if value is not None:
                return value
        return None

    @classmethod
    def _integer(cls, value: Any) -> int | None:
        return value if isinstance(value, int) and not isinstance(value, bool) else None

    @classmethod
    def _usage(cls, result: Any) -> tuple[AgentUsage, dict[str, JsonValue]]:
        raw = getattr(result, "usage", None) or {}
        input_tokens = cls._integer(cls._value(raw, "input_tokens", "inputTokens"))
        output_tokens = cls._integer(cls._value(raw, "output_tokens", "outputTokens"))
        cached_tokens = cls._integer(
            cls._value(raw, "cache_read_input_tokens", "cacheReadInputTokens", "cached_tokens")
        )
        cache_creation = cls._integer(
            cls._value(raw, "cache_creation_input_tokens", "cacheCreationInputTokens")
        )
        total_tokens = cls._integer(cls._value(raw, "total_tokens", "totalTokens"))
        if total_tokens is None and input_tokens is not None and output_tokens is not None:
            total_tokens = input_tokens + output_tokens
        cost_value = getattr(result, "total_cost_usd", None)
        try:
            cost = Decimal(str(cost_value)) if cost_value is not None else None
        except (InvalidOperation, ValueError):
            cost = None
        duration_api_ms = cls._integer(getattr(result, "duration_api_ms", None))
        model_usage = getattr(result, "model_usage", None)
        metadata: dict[str, JsonValue] = {
            "cache_creation_input_tokens": cache_creation,
            "model_usage": sanitize_event_data(model_usage) if model_usage is not None else None,
        }
        return (
            AgentUsage(
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cached_tokens=cached_tokens,
                total_tokens=total_tokens,
                cost=cost,
                currency="USD" if cost is not None else None,
                latency_seconds=(duration_api_ms / 1000 if duration_api_ms is not None else None),
            ),
            metadata,
        )
