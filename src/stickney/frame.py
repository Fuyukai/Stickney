from __future__ import annotations

import attr

__all__ = (
    "BinaryMessage",
    "CloseMessage",
    "ConnectionOpenMessage",
    "PingMessage",
    "PongMessage",
    "RejectMessage",
    "TextualMessage",
    "WsMessage",
)


class WsMessage:
    """
    Marker root class for inbound websocket frames.
    """


@attr.s(frozen=True, slots=True)
class ConnectionOpenMessage(WsMessage):
    """
    Returned when the websocket server accepts the Upgrade request.
    """


@attr.s(frozen=True, slots=True)
class RejectMessage(WsMessage):
    """
    Returned when the websocket server rejects the Upgrade request.
    """

    #: The HTTP status code that the server returned when rejecting the websocket upgrade request.
    status_code: int = attr.ib()

    #: The HTTP body that the server returned when rejecting the websocket upgrade request.
    body: bytes = attr.ib(default=b"")


@attr.s(frozen=True, slots=True)
class CloseMessage(WsMessage):
    """
    Used when either side wants to close the websocket connection.
    """

    #: The websocket close code. This should be a code that starts from 1000.
    close_code: int = attr.ib(default=1000)

    #: The websocket close reason. Human-readable, but otherwise unused.
    reason: str = attr.ib(default="Closed")


@attr.s(frozen=True, slots=True)
class TextualMessage(WsMessage):
    """
    A completed UTF-8 plaintext message.
    """

    #: The body of this message.
    body: str = attr.ib()


@attr.s(frozen=True, slots=True)
class BinaryMessage(WsMessage):
    """
    A completed binary message.
    """

    #: The body of this message.
    body: bytes = attr.ib()


@attr.s(frozen=True, slots=True)
class PingMessage(WsMessage):
    """
    A ping heartbeat.
    """

    data: bytes = attr.ib()


@attr.s(frozen=True, slots=True)
class PongMessage(WsMessage):
    """
    A pong heartbeat.
    """

    data: bytes = attr.ib()
