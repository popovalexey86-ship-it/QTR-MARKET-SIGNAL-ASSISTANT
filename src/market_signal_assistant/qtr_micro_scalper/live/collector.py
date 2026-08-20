from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any, Protocol

LINEAR_PUBLIC_WS_URL = "wss://stream.bybit.com/v5/public/linear"


class LiveMarketDataError(RuntimeError):
    """Controlled failure in the shadow-only public market-data layer."""


class AsyncWebSocket(Protocol):
    async def send(self, message: str) -> None: ...
    async def recv(self) -> str | bytes: ...
    async def close(self) -> None: ...


WebSocketFactory = Callable[[str], Awaitable[AsyncWebSocket]]


class ManagedMarketStream(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...


class DynamicMarketStream(ManagedMarketStream, Protocol):
    async def update_symbols(self, symbols: Iterable[str]) -> None: ...

    @property
    def metrics(self) -> StreamMetrics: ...


@dataclass(frozen=True, slots=True)
class StreamMetrics:
    connections: int
    reconnects: int
    messages: int
    accepted_events: int
    malformed_events: int
    last_error: str | None
    active_topics: int
    subscribe_operations: int
    unsubscribe_operations: int
    subscription_errors: int


@dataclass(frozen=True, slots=True)
class UnifiedSubscriptionMetrics:
    active_topics: int
    subscribe_operations: int
    unsubscribe_operations: int
    subscription_errors: int



@dataclass(frozen=True, slots=True)
class UnifiedCollectorStatus:
    running: bool
    stream_errors: tuple[str, ...]


async def default_websocket_factory(url: str) -> AsyncWebSocket:
    """Open a socket lazily; importing or constructing collectors is offline."""
    try:
        from websockets.asyncio.client import connect  # type: ignore[import-not-found]
    except ImportError as exc:
        raise LiveMarketDataError(
            "Live Scalper V2 requires the optional websocket dependency."
        ) from exc
    return await connect(url)  # type: ignore[no-any-return]


class BybitPublicStream:
    """Reconnectable Bybit V5 public stream with an injected socket factory."""

    def __init__(
        self,
        *,
        topics: Iterable[str],
        websocket_factory: WebSocketFactory = default_websocket_factory,
        url: str = LINEAR_PUBLIC_WS_URL,
        heartbeat_seconds: float = 20.0,
        reconnect_seconds: float = 0.25,
    ) -> None:
        normalized = tuple(
            sorted(dict.fromkeys(topic.strip() for topic in topics if topic.strip()))
        )
        _validate_topic_args(normalized)
        if heartbeat_seconds <= 0 or reconnect_seconds < 0:
            raise ValueError(
                "Heartbeat must be positive and reconnect delay non-negative."
            )
        self._topics = normalized
        self._topic_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._factory = websocket_factory
        self._url = url
        self._heartbeat_seconds = heartbeat_seconds
        self._reconnect_seconds = reconnect_seconds
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._socket: AsyncWebSocket | None = None
        self._connections = 0
        self._reconnects = 0
        self._messages = 0
        self._accepted = 0
        self._malformed = 0
        self._last_error: str | None = None
        self._subscribe_operations = 0
        self._unsubscribe_operations = 0
        self._subscription_errors = 0

    @property
    def metrics(self) -> StreamMetrics:
        return StreamMetrics(
            self._connections,
            self._reconnects,
            self._messages,
            self._accepted,
            self._malformed,
            self._last_error,
            len(self._topics),
            self._subscribe_operations,
            self._unsubscribe_operations,
            self._subscription_errors,
        )

    async def set_topics(self, topics: Iterable[str]) -> None:
        normalized = tuple(
            sorted(dict.fromkeys(topic.strip() for topic in topics if topic.strip()))
        )
        _validate_topic_args(normalized)
        async with self._topic_lock:
            previous = set(self._topics)
            current = set(normalized)
            added = tuple(sorted(current - previous))
            removed = tuple(sorted(previous - current))
            if not added and not removed:
                return
            socket = self._socket
            if socket is not None:
                try:
                    if removed:
                        await self._send_operation(socket, "unsubscribe", removed)
                        self._unsubscribe_operations += 1
                    if added:
                        await self._send_operation(socket, "subscribe", added)
                        self._subscribe_operations += 1
                except Exception as exc:
                    self._subscription_errors += 1
                    self._last_error = f"{type(exc).__name__}: {exc}"
                    raise LiveMarketDataError(
                        "Dynamic Bybit subscription update failed."
                    ) from exc
            self._topics = normalized

    async def _send_operation(
        self,
        socket: AsyncWebSocket,
        operation: str,
        topics: tuple[str, ...] = (),
    ) -> None:
        payload: dict[str, object] = {"op": operation}
        if topics:
            payload["args"] = topics
        async with self._send_lock:
            await socket.send(json.dumps(payload))

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run())
        await asyncio.sleep(0)

    async def stop(self) -> None:
        self._stop.set()
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
        socket = self._socket
        self._socket = None
        if socket is not None:
            await socket.close()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)

    async def _run(self) -> None:
        first_connection = True
        while not self._stop.is_set():
            socket: AsyncWebSocket | None = None
            try:
                socket = await self._factory(self._url)
                async with self._topic_lock:
                    self._socket = socket
                    self._connections += 1
                    if not first_connection:
                        self._reconnects += 1
                    first_connection = False
                    topics = self._topics
                    if topics:
                        await self._send_operation(socket, "subscribe", topics)
                        self._subscribe_operations += 1
                heartbeat = asyncio.create_task(self._heartbeat(socket))
                try:
                    while not self._stop.is_set():
                        raw = await socket.recv()
                        self._messages += 1
                        try:
                            payload = json.loads(raw)
                            if not isinstance(payload, dict):
                                raise ValueError("WebSocket payload must be an object.")
                            self._accepted += self.handle_payload(payload)
                        except (
                            KeyError,
                            TypeError,
                            ValueError,
                            json.JSONDecodeError,
                        ) as exc:
                            self._malformed += 1
                            self._last_error = str(exc)
                finally:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._last_error = f"{type(exc).__name__}: {exc}"
            finally:
                if socket is not None:
                    await socket.close()
                async with self._topic_lock:
                    if self._socket is socket:
                        self._socket = None
            if not self._stop.is_set():
                await asyncio.sleep(self._reconnect_seconds)

    async def _heartbeat(self, socket: AsyncWebSocket) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self._heartbeat_seconds)
            await self._send_operation(socket, "ping")

    def handle_payload(self, payload: dict[str, Any]) -> int:
        raise NotImplementedError


class UnifiedMarketDataCollector:
    """Lifecycle coordinator; every stream reconnects and fails independently."""

    def __init__(
        self,
        streams: Iterable[ManagedMarketStream],
        *,
        dynamic_streams: Iterable[DynamicMarketStream] = (),
    ) -> None:
        self._streams = tuple(streams)
        if not self._streams:
            raise ValueError("Unified collector requires at least one stream.")
        self._dynamic_streams = tuple(dynamic_streams)
        self._running = False
        self._errors: tuple[str, ...] = ()
        self._subscription_errors = 0

    @property
    def status(self) -> UnifiedCollectorStatus:
        return UnifiedCollectorStatus(self._running, self._errors)
    @property
    def subscription_metrics(self) -> UnifiedSubscriptionMetrics:
        metrics = tuple(stream.metrics for stream in self._dynamic_streams)
        return UnifiedSubscriptionMetrics(
            active_topics=sum(item.active_topics for item in metrics),
            subscribe_operations=sum(item.subscribe_operations for item in metrics),
            unsubscribe_operations=sum(
                item.unsubscribe_operations for item in metrics
            ),
            subscription_errors=(
                self._subscription_errors
                + sum(item.subscription_errors for item in metrics)
            ),
        )

    async def update_symbols(self, symbols: Iterable[str]) -> None:
        normalized = tuple(
            sorted(
                dict.fromkeys(
                    item.strip().upper() for item in symbols if item.strip()
                )
            )
        )
        results = await asyncio.gather(
            *(stream.update_symbols(normalized) for stream in self._dynamic_streams),
            return_exceptions=True,
        )
        errors = tuple(
            result for result in results if isinstance(result, BaseException)
        )
        self._subscription_errors += len(errors)
        if errors:
            raise LiveMarketDataError(str(errors[0]))


    async def start(self) -> None:
        if self._running:
            return
        results = await asyncio.gather(
            *(stream.start() for stream in self._streams), return_exceptions=True
        )
        self._errors = tuple(
            f"{type(result).__name__}: {result}"
            for result in results
            if isinstance(result, BaseException)
        )
        self._running = True

    async def stop(self) -> None:
        if not self._running:
            return
        await asyncio.gather(
            *(stream.stop() for stream in self._streams), return_exceptions=True
        )
        self._running = False

def _validate_topic_args(topics: tuple[str, ...]) -> None:
    if sum(len(topic) for topic in topics) > 21_000:
        raise ValueError("Bybit public WebSocket topic args exceed 21000 characters.")
