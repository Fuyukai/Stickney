from __future__ import annotations

from typing import final

import attrs

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


@attrs.define(frozen=True)
@final
class ConnectionOpenMessage(WsMessage):
    """
    Returned when the websocket server accepts the Upgrade request.
    """


@attrs.frozen()
@final
class RejectMessage(WsMessage):
    """
    Returned when the websocket server rejects the Upgrade request.
    """

    #: The HTTP status code that the server returned when rejecting the websocket upgrade request.
    status_code: int = attrs.field()

    #: The HTTP body that the server returned when rejecting the websocket upgrade request.
    body: bytes = attrs.field(default=b"")


@attrs.frozen()
@final
class CloseMessage(WsMessage):
    """
    Used when either side wants to close the websocket connection.
    """

    #: The websocket close code. This should be a code that starts from 1000.
    close_code: int = attrs.field(default=1000)

    #: The websocket close reason. Human-readable, but otherwise unused.
    reason: str = attrs.field(default="Closed")


@attrs.frozen()
@final
class TextualMessage(WsMessage):
    """
    A completed UTF-8 plaintext message.
    """

    #: The body of this message.
    body: str = attrs.field()


@attrs.frozen()
@final
class BinaryMessage(WsMessage):
    """
    A completed binary message.
    """

    #: The body of this message.
    body: bytes = attrs.field()


@attrs.frozen()
@final
class PingMessage(WsMessage):
    """
    A ping heartbeat.
    """

    data: bytes = attrs.field()


@attrs.frozen()
@final
class PongMessage(WsMessage):
    """
    A pong heartbeat.
    """

    data: bytes = attrs.field()
