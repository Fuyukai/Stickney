from __future__ import annotations

import anyio

__all__ = (
    "ConnectionRejectedError",
    "WebsocketClosedError",
    "WebsocketStateMachineError",
)


class WebsocketStateMachineError(RuntimeError):
    """
    Raised when the websocket state machine is, in some form, corrupted.
    """


class WebsocketClosedError(anyio.BrokenResourceError):
    """
    Raised when a websocket has closed unexpectedly.
    """

    def __init__(self, code: int, reason: str):
        super().__init__(f"Websocket closed: {code} ({reason})")

        self.code = code
        self.reason = reason


class ConnectionRejectedError(anyio.BrokenResourceError):
    """
    Raised when a websocket server rejects the incoming connection.
    """

    def __init__(self, status_code: int, body: bytes):
        super().__init__(f"Server rejected WebSocket connection: {status_code} ({body!r})")

        self.status_code = status_code
        self.body = body
