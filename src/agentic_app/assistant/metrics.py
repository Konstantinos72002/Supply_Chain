"""Per-question metrics: every LLM call made while answering one question (count, time, tokens).

The SQL side is recorded by app.run_statement; this module only sees the LLM. The handler is attached through a
LangChain configure hook, so every chat-model call inside track_llm_calls() reports to it without passing callbacks
through the graph.
"""
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any
from uuid import UUID

from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, LLMResult
from langchain_core.tracers.context import register_configure_hook


class LlmCallRecorder(BaseCallbackHandler):
    """Records one entry per chat-model call: graph node, duration and token usage."""
    run_inline = True                      # called on the event loop, in order, not in a thread pool

    def __init__(self) -> None:
        super().__init__()
        self._lock = threading.Lock()
        self._started: dict[UUID, tuple[float, str]] = {}
        self.calls: list[dict[str, Any]] = []

    def on_chat_model_start(self, serialized: Any, messages: Any, *, run_id: UUID,
                            metadata: dict[str, Any] | None = None, **kwargs: Any) -> None:
        self._started[run_id] = (time.perf_counter(), (metadata or {}).get('langgraph_node', '-'))

    def on_llm_end(self, response: LLMResult, *, run_id: UUID, **kwargs: Any) -> None:
        usage = {}
        generation = response.generations[0][0] if response.generations and response.generations[0] else None
        if isinstance(generation, ChatGeneration) and isinstance(generation.message, AIMessage):
            usage = generation.message.usage_metadata or {}
        self._finish(run_id, 'ok', usage.get('input_tokens', 0), usage.get('output_tokens', 0))

    def on_llm_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        self._finish(run_id, 'error', 0, 0)

    def _finish(self, run_id: UUID, status: str, input_tokens: int, output_tokens: int) -> None:
        started, node = self._started.pop(run_id, (time.perf_counter(), '-'))
        with self._lock:
            self.calls.append({'node': node, 'status': status,
                               'duration_ms': round((time.perf_counter() - started) * 1000, 1),
                               'input_tokens': input_tokens, 'output_tokens': output_tokens})


_recorder: ContextVar[LlmCallRecorder | None] = ContextVar('llm_call_recorder', default=None)
register_configure_hook(_recorder, inheritable=True)


@contextmanager
def track_llm_calls() -> Iterator[LlmCallRecorder]:
    recorder = LlmCallRecorder()
    token = _recorder.set(recorder)
    try:
        yield recorder
    finally:
        _recorder.reset(token)
