from __future__ import annotations

import contextlib
import ssl
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from enum import Enum
from urllib.parse import ParseResult, urlparse

import anyio
from anyio import (
    CancelScope,
    ClosedResourceError,
    EndOfStream,
    Event as AioEvent,
    ResourceGuard,
    aclose_forcefully,
    create_memory_object_stream,
)
from anyio.abc import ByteStream
from wsproto import ConnectionType, WSConnection
from wsproto.events import (
    AcceptConnection,
    BytesMessage,
    CloseConnection,
    Event,
    Ping,
    Pong,
    RejectConnection,
    RejectData,
    Request,
    TextMessage,
)

from stickney.exc import (
    ConnectionRejectedError,
    WebsocketClosedError,
    WebsocketStateMachineError,
)
from stickney.frame import (
    BinaryMessage,
    CloseMessage,
    ConnectionOpenMessage,
    PingMessage,
    PongMessage,
    RejectMessage,
    TextualMessage,
    WsMessage,
)


class BufferType(Enum):
    """
    Enumeration of the possible messages that are being buffered.
    """

    #: The current message being buffered is the body of a rejected upgrade response.
    REJECT = 0

    #: The current message being buffered is a regular WebSocket text message.
    TEXTUAL = 1

    #: The current message being buffered is a regular WebSocket bytes message.
    BYTES = 2


class WebsocketClient:
    """
    A single client websocket connection.
    """

    BUFFER_SIZE = 2**16

    def __init__(
        self,
        sock: ByteStream,
        *,
        graceful_closes: bool = True,
        cancel_scope: CancelScope,
        buffer_size: float = 0,
    ):
        self._sock = sock
        self._gracefully_close = graceful_closes
        self._proto = WSConnection(ConnectionType.CLIENT)
        self._cancel_scope = cancel_scope

        self._lock = anyio.Lock()
        self._recv_lock = ResourceGuard("receiving websocket messages from")

        self._close_event = AioEvent()
        self._open_event = AioEvent()

        # Closing is *still* a bit messy.
        # ``_is_half_closed`` -> a close frame has been processed, but we're still waiting for
        # *somebody* to deal with it. sending new messages will fail.
        self._is_half_closed = False
        # we are definitely closed, and receiving new messages will fail.
        # this can never be true and half_closed be false at the same time.
        self._is_definitely_closed = False

        # ``client_initialised_close``: Don't bother responding to the server's close frame.
        self._client_initialised_close = False

        self._write_incoming, self._read_incoming = create_memory_object_stream[WsMessage](
            buffer_size
        )

        # used for buffering various message types, last buf message is shared across all of them
        # but rejected error code is only used for rejected messages.
        self._buffer_type: BufferType | None = None
        self._rejected_error_code: int = 0

        self._last_buffered_message: str | bytes = ""

        self._close_code: int = 0
        self._close_reason: str = ""

        self._pumping = False

    async def _send_message(self, event: Event) -> None:
        """
        Sends a single message over the websocket connection.
        """

        if self._is_half_closed:
            raise WebsocketClosedError(self._close_code, self._close_reason)

        async with self._lock:
            data = self._proto.send(event)
            await self._sock.send(data)

    async def _do_handshake(self, url: ParseResult) -> None:
        """
        Does the opening websocket handshake.
        """

        assert url.hostname

        path = str(url.path) if url.path else "/"

        if url.query:
            req = Request(host=url.hostname, target=path + "?" + str(url.query))
        else:
            req = Request(host=url.hostname, target=path)
        await self._sock.send(self._proto.send(req))

        incoming = await self.receive_single_message()
        if not isinstance(incoming, ConnectionOpenMessage):
            raise WebsocketStateMachineError(f"Expected a ConnectionOpenMessage, got a {incoming}")

    async def _process_single_inbound_event(self, event: Event) -> None:
        if isinstance(event, AcceptConnection):
            await self._write_incoming.send(ConnectionOpenMessage())

        elif isinstance(event, CloseConnection):
            self._close_event.set()

            self._is_closing = True
            if reason := event.reason:
                self._close_reason = reason
            else:
                self._close_reason = "Closed"
            self._close_code = event.code

            try:
                if not (self._is_definitely_closed or self._client_initialised_close):
                    with contextlib.suppress(
                        ClosedResourceError, EndOfStream, WebsocketClosedError
                    ):
                        await self._send_message(event.response())
            finally:
                self._is_half_closed = True
                await self._write_incoming.send(
                    CloseMessage(close_code=event.code, reason=self._close_reason)
                )
                self._write_incoming.close()

        elif isinstance(event, RejectConnection):
            assert self._buffer_type is None, "shouldnt get a reject during buffering, wtf wsproto?"
            self._buffer_type = BufferType.REJECT
            self._rejected_error_code = event.status_code
            self._last_buffered_message = b""
            self._is_closing = True

            if not event.has_body:
                try:
                    await self._write_incoming.send(
                        RejectMessage(status_code=self._rejected_error_code)
                    )
                finally:
                    self._is_closed = True
                    self._close_event.set()

                    await self._sock.aclose()

        elif isinstance(event, RejectData):
            assert self._buffer_type == BufferType.REJECT, "reject data before reject conn? wtf??"

            self._last_buffered_message += event.data  # type: ignore
            if event.body_finished:
                try:
                    await self._write_incoming.send(
                        RejectMessage(
                            status_code=self._rejected_error_code,
                            body=self._last_buffered_message,  # type: ignore
                        )
                    )
                finally:
                    self._is_closed = True
                    self._close_event.set()

                    await self._sock.aclose()

        elif isinstance(event, TextMessage):
            if self._buffer_type == BufferType.TEXTUAL:
                assert isinstance(self._last_buffered_message, str)
                self._last_buffered_message += event.data
            elif self._buffer_type is None:
                self._buffer_type = BufferType.TEXTUAL
                self._last_buffered_message = event.data
            else:
                raise WebsocketStateMachineError(
                    "Buffer type should either be textual or None when reading a textual "
                    f"message, not {self._buffer_type}!"
                )

            if event.message_finished:
                self._buffer_type = None
                text_body: str = self._last_buffered_message
                self._last_buffered_message = ""
                await self._write_incoming.send(TextualMessage(text_body))

        elif isinstance(event, BytesMessage):
            if self._buffer_type == BufferType.BYTES:
                assert isinstance(self._last_buffered_message, bytes)
                self._last_buffered_message += event.data
            elif self._buffer_type is None:
                self._buffer_type = BufferType.BYTES
                self._last_buffered_message = event.data
            else:
                raise WebsocketStateMachineError(
                    "Buffer type should either be bytes or None when reading a binary "
                    f"message, not {self._buffer_type}!"
                )

            if event.message_finished:
                self._buffer_type = None
                bytes_body: bytes = self._last_buffered_message
                self._last_buffered_message = b""
                await self._write_incoming.send(BinaryMessage(bytes_body))

        elif isinstance(event, Ping):
            if not self._is_definitely_closed:
                await self._send_message(event.response())

            await self._write_incoming.send(PingMessage(event.payload))

        elif isinstance(event, Pong):
            await self._write_incoming.send(PongMessage(event.payload))

    async def _pump_messages(self):
        while not self._is_half_closed:
            try:
                raw_data = await self._sock.receive(WebsocketClient.BUFFER_SIZE)
            except (ClosedResourceError, EndOfStream):
                # EOF means that we are *absolutely* closed and there's nothing to do anymore.
                self._is_half_closed = True
                await self._process_single_inbound_event(
                    CloseConnection(1006, "Socket unexpectedly closed")
                )
                self._write_incoming.close()
                return

            self._proto.receive_data(raw_data)

            for event in self._proto.events():
                await self._process_single_inbound_event(event)

    async def _finish(self) -> None:
        while True:
            try:
                await self._read_incoming.receive()
            except (EndOfStream, ClosedResourceError):
                break

        await self._sock.aclose()

    @property
    def definitely_closed(self) -> bool:
        return self._is_definitely_closed

    async def receive_single_message(self, *, raise_on_close: bool = True) -> WsMessage:
        """
        Receives a single message from the websocket connection.

        :param raise_on_close: If True, then an error will be raised on a server-sent close message.
                               Defaults to True.
        """

        if self._is_definitely_closed:
            if not raise_on_close:
                return CloseMessage(self._close_code, self._close_reason)

            raise WebsocketClosedError(self._close_code, self._close_reason)

        with self._recv_lock:
            msg = await self._read_incoming.receive()

            if isinstance(msg, RejectMessage):
                raise ConnectionRejectedError(msg.status_code, msg.body)

            if isinstance(msg, CloseMessage):
                self._is_definitely_closed = True

                if raise_on_close and not self._client_initialised_close:
                    raise WebsocketClosedError(msg.close_code, msg.reason)

            return msg

    async def close(
        self,
        *,
        code: int = 1000,
        reason: str = "Normal close",
    ) -> None:
        """
        Closes the websocket connection. If the websocket is already closed (or is closing),
        this method does nothing.

        :param code: The closing code to use. This should be in the range 1000 .. 1011 for most
                     applications.
        :param reason: The close reason to use. Arbitrary string.
        """

        if self._is_half_closed or self._client_initialised_close:
            # on trio, at least, this just calls checkpoint().
            return await anyio.sleep(0)

        self._close_code = code
        self._close_reason = reason

        self._client_initialised_close = True
        event = CloseConnection(code=code, reason=reason)

        try:  # noqa: SIM105
            await self._sock.send(self._proto.send(event))
        except ClosedResourceError:
            # whatever
            pass
        return None

    async def send_ping(self, data: bytes = b"Have you heard of the high elves?") -> None:
        """
        Sends a Ping frame to the remote server.

        :param data: Arbitrary application data. This will be returned from the server in a Pong
                     message.
        """

        await self._send_message(Ping(data))

    async def send_message(self, data: str | bytes, *, message_finished: bool = True) -> None:
        """
        Sends a message to the remote websocket server.

        :param data: The data to send. This may be either ``str`` or ``bytes``; the type will change
                     the underlying message frame type.
        :param message_finished: If this is a complete message or not.
        """

        event: TextMessage | BytesMessage
        if isinstance(data, str):
            event = TextMessage(data, message_finished=message_finished)
        else:
            event = BytesMessage(data, message_finished=message_finished)

        await self._send_message(event)


@asynccontextmanager
async def open_ws_connection(
    url: str, *, gracefully_close: bool = True, tls_context: ssl.SSLContext | None = None
) -> AsyncGenerator[WebsocketClient]:
    """
    Opens a new websocket connection to the provided URL, and returns a new asynchronous context
    manager holding the websocket.

    If ``tls_context`` is unset, but the URL provided is ``https`` or ``wss``, then a default
    one will be created instead.

    :param url: The URL of the websocket endpoint to connect to.
    :param gracefully_close: If True, then a graceful close will be attempted.
    :param tls_context: The TLS context to use for opening WSS connections. Optional.
    """

    parsed_url = urlparse(url)
    if parsed_url.hostname is None:
        raise ValueError("why does the url have no hostname?")

    port: int | None = parsed_url.port

    if port is None:
        match parsed_url.scheme:
            case "ws" | "http":
                port = 80

            case "wss" | "https":
                port = 443

            case _:
                port = 80

    if (parsed_url.scheme == "wss" or parsed_url.scheme == "https") and not tls_context:
        tls_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)

    async with (
        await anyio.connect_tcp(
            parsed_url.hostname,
            port,
            ssl_context=tls_context,  # type: ignore
            tls_standard_compatible=False,
        ) as socket,
        anyio.create_task_group() as nursery,
    ):
        conn = WebsocketClient(
            socket,
            graceful_closes=gracefully_close,
            cancel_scope=nursery.cancel_scope,
        )
        nursery.start_soon(conn._pump_messages)
        await conn._do_handshake(parsed_url)

        try:
            yield conn
        finally:
            with anyio.move_on_after(5, shield=True):
                await conn.close(code=1000)
                await conn._finish()

            await aclose_forcefully(socket)
