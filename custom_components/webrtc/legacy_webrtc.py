"""Compatibility support for legacy go2rtc Home Assistant camera sources."""

from __future__ import annotations

from dataclasses import dataclass
from functools import partial
import logging
import re
from typing import Any

import voluptuous as vol

from homeassistant.components import websocket_api
from homeassistant.components.camera import (
    Camera,
    WebRTCAnswer,
    WebRTCCandidate,
    WebRTCError,
    WebRTCMessage,
)
from homeassistant.components.camera.webrtc import require_webrtc_support
from homeassistant.const import EVENT_HOMEASSISTANT_STOP
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv
from homeassistant.util.ulid import ulid

_LOGGER = logging.getLogger(__name__)

LEGACY_WEBRTC_OFFER = "camera/web_rtc_offer"
LEGACY_SESSION_TTL = 10 * 60
DATA_LEGACY_SESSIONS = "webrtc_legacy_sessions"


def _sdp_media_summary(sdp: str) -> str:
    """Return non-sensitive SDP media details for diagnostics."""
    prefixes = (
        "m=",
        "a=mid:",
        "a=sendrecv",
        "a=sendonly",
        "a=recvonly",
        "a=inactive",
        "a=rtpmap:",
        "a=fmtp:",
    )
    return " | ".join(line for line in sdp.splitlines() if line.startswith(prefixes))


def _simplify_legacy_video_codecs(sdp: str) -> str:
    """Offer one H264 payload to avoid malformed Nest SDP answers.

    Older go2rtc releases offer several H264 payloads with equivalent codec
    parameters. The current Nest API can answer those offers with duplicate
    payload IDs, which prevents go2rtc from receiving video packets. Keep the
    first H264 payload while preserving unrelated media sections and generic
    video attributes.
    """
    newline = "\r\n" if "\r\n" in sdp else "\n"
    lines = sdp.splitlines()
    video_start = next(
        (index for index, line in enumerate(lines) if line.startswith("m=video ")),
        None,
    )
    if video_start is None:
        return sdp

    video_end = next(
        (
            index
            for index in range(video_start + 1, len(lines))
            if lines[index].startswith("m=")
        ),
        len(lines),
    )
    payload = next(
        (
            match.group(1)
            for line in lines[video_start + 1 : video_end]
            if (match := re.match(r"a=rtpmap:(\d+) H264/", line, re.IGNORECASE))
        ),
        None,
    )
    if payload is None:
        return sdp

    media = lines[video_start].split()
    lines[video_start] = " ".join([*media[:3], payload])
    payload_attribute = re.compile(r"a=(?:rtpmap|fmtp|rtcp-fb):(\d+)\b")
    lines[video_start + 1 : video_end] = [
        line
        for line in lines[video_start + 1 : video_end]
        if not (match := payload_attribute.match(line)) or match.group(1) == payload
    ]
    return newline.join(lines) + (newline if sdp.endswith(("\r\n", "\n")) else "")


@dataclass
class LegacySession:
    """Track a modern Home Assistant WebRTC session for a legacy client."""

    close: Any
    cancel_expiration: Any


@callback
def _close_session(hass: HomeAssistant, entity_id: str) -> None:
    """Close a retained legacy WebRTC session."""
    sessions: dict[str, LegacySession] = hass.data[DATA_LEGACY_SESSIONS]
    if session := sessions.pop(entity_id, None):
        session.cancel_expiration.cancel()
        session.close()


@callback
def _retain_session(
    hass: HomeAssistant, entity_id: str, camera: Camera, session_id: str
) -> None:
    """Retain one bounded legacy session per camera entity."""
    _close_session(hass, entity_id)

    cancel_expiration = hass.loop.call_later(
        LEGACY_SESSION_TTL,
        _close_session,
        hass,
        entity_id,
    )
    hass.data[DATA_LEGACY_SESSIONS][entity_id] = LegacySession(
        close=partial(camera.close_webrtc_session, session_id),
        cancel_expiration=cancel_expiration,
    )


@websocket_api.websocket_command(
    {
        vol.Required("type"): LEGACY_WEBRTC_OFFER,
        vol.Required("entity_id"): cv.entity_id,
        vol.Required("offer"): str,
    }
)
@websocket_api.async_response
@require_webrtc_support("web_rtc_offer_failed")
async def ws_legacy_webrtc_offer(
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
    camera: Camera,
) -> None:
    """Translate the legacy synchronous offer API to the current async API.

    go2rtc releases through 1.9.x call ``camera/web_rtc_offer`` and expect the
    SDP answer in the command result. Home Assistant 2024.11 and newer use an
    event-based WebRTC API instead. This adapter collects the final answer and
    returns the response shape expected by go2rtc.

    The go2rtc client closes its Home Assistant WebSocket after negotiation, so
    the modern camera session cannot be owned by that socket subscription. A
    bounded lease retains the session long enough for normal on-demand viewing.
    A later offer for the same entity replaces the previous session.
    """
    answer: str | None = None
    error: WebRTCError | None = None
    session_id = ulid()

    offer = _simplify_legacy_video_codecs(msg["offer"])
    _LOGGER.debug("Legacy WebRTC offer: %s", _sdp_media_summary(offer))

    @callback
    def capture_message(message: WebRTCMessage) -> None:
        nonlocal answer, error
        if isinstance(message, WebRTCAnswer):
            answer = message.answer
        elif isinstance(message, WebRTCError):
            error = message
        elif isinstance(message, WebRTCCandidate):
            # The legacy API cannot exchange trickle ICE candidates. Legacy
            # clients send a complete offer and require a complete SDP answer.
            _LOGGER.debug("Ignoring trickle ICE candidate for legacy client")

    try:
        await camera.async_handle_async_webrtc_offer(
            offer, session_id, capture_message
        )
    except (HomeAssistantError, ValueError) as ex:
        _LOGGER.error("Error handling legacy WebRTC offer: %s", ex)
        camera.close_webrtc_session(session_id)
        connection.send_error(msg["id"], "web_rtc_offer_failed", str(ex))
        return
    except TimeoutError:
        _LOGGER.error("Timeout handling legacy WebRTC offer")
        camera.close_webrtc_session(session_id)
        connection.send_error(
            msg["id"],
            "web_rtc_offer_failed",
            "Timeout handling WebRTC offer",
        )
        return

    if error is not None:
        camera.close_webrtc_session(session_id)
        connection.send_error(msg["id"], error.code, error.message)
        return

    if answer is None:
        camera.close_webrtc_session(session_id)
        connection.send_error(
            msg["id"],
            "web_rtc_offer_failed",
            "Camera did not return a WebRTC answer",
        )
        return

    _LOGGER.debug("Legacy WebRTC answer: %s", _sdp_media_summary(answer))

    _retain_session(camera.hass, msg["entity_id"], camera, session_id)
    connection.send_result(msg["id"], {"answer": answer})


@callback
def async_register_legacy_webrtc_command(hass: HomeAssistant) -> None:
    """Register the WebSocket command expected by legacy go2rtc releases."""
    hass.data[DATA_LEGACY_SESSIONS] = {}
    websocket_api.async_register_command(hass, ws_legacy_webrtc_offer)

    @callback
    def close_all_sessions(_event: Any) -> None:
        for entity_id in list(hass.data[DATA_LEGACY_SESSIONS]):
            _close_session(hass, entity_id)

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, close_all_sessions)
