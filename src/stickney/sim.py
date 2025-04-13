import math
import socket
from collections import deque
from typing import override

from anyio import CancelScope
from anyio.abc import SocketStream
from wsproto.events import Event

from stickney.client import WebsocketClient
from stickney.exc import WebsocketClosedError


class FakeSocket(SocketStream):
    async def receive(self, max_bytes: int = 65536) -> bytes:  # type: ignore
        pass

    @override
    async def send(self, item: bytes) -> None:
        pass

    @property
    def _raw_socket(self) -> socket.socket:  # type: ignore
        pass

    @override
    async def send_eof(self) -> None:
        pass

    @override
    async def aclose(self) -> None:
        pass


class SimulatedWebsocket(WebsocketClient):
    """
    A websocket that allows for simulating incoming traffic.
    """

    def __init__(self):
        super().__init__(FakeSocket(), cancel_scope=CancelScope(), buffer_size=math.inf)

        #: A deque of outbound messages that have been "sent" by this websocket.
        self.outbound_messages: deque[Event] = deque()

    @override
    async def _send_message(self, event: Event) -> None:
        if self._is_definitely_closed:
            raise WebsocketClosedError(self._close_code, self._close_reason)

        self.outbound_messages.append(event)

    async def push_message(self, event: Event) -> None:
        """
        Pushes a single message down this websocket, updating the internal state machine.
        """

        await self._process_single_inbound_event(event)
