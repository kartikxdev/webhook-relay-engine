"""HMAC-SHA256 signature generation and header preparation for outbound webhooks."""

import hashlib
import hmac
import logging

from src.engine.exceptions import SignatureComputationError

logger = logging.getLogger("relayforge.signature")


def sign_payload(payload_bytes: bytes, secret: str) -> str:
    """
    Compute an HMAC-SHA256 hex digest for given payload bytes and secret key.

    Returns:
        Hexadecimal digest string.
    """
    try:
        key = secret.encode("utf-8")
        h = hmac.new(key, msg=payload_bytes, digestmod=hashlib.sha256)
        return h.hexdigest()
    except Exception as exc:
        raise SignatureComputationError(f"Failed to sign webhook payload: {exc}") from exc


def prepare_relay_headers(
    base_headers: dict[str, str],
    payload_str: str,
    secret_token: str | None,
    delivery_id: str,
    attempt_number: int,
) -> dict[str, str]:
    """
    Format outbound HTTP headers including signature, delivery ID, and content type.
    """
    headers = dict(base_headers)
    headers.setdefault("Content-Type", "application/json")
    headers.setdefault("User-Agent", "RelayForge-Worker/0.1.0")
    headers["X-RelayForge-Delivery-Id"] = delivery_id
    headers["X-RelayForge-Attempt"] = str(attempt_number)

    if secret_token:
        signature = sign_payload(payload_str.encode("utf-8"), secret_token)
        headers["X-RelayForge-Signature"] = f"sha256={signature}"

    return headers
