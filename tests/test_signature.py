"""Unit tests for payload signing and header creation."""

import hashlib
import hmac

from src.engine.signature import prepare_relay_headers, sign_payload


def test_sign_payload_matches_hmac_sha256() -> None:
    """Verify sign_payload computes valid HMAC-SHA256 hex string."""
    payload = b'{"event":"test.ping"}'
    secret = "super_secret_token_123"

    signature = sign_payload(payload, secret)
    expected = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()

    assert signature == expected
    assert len(signature) == 64


def test_sign_payload_distinct_keys_or_payloads() -> None:
    """Verify distinct secrets produce distinct signatures."""
    payload = b'{"hello":"world"}'
    sig1 = sign_payload(payload, "secret_a")
    sig2 = sign_payload(payload, "secret_b")
    assert sig1 != sig2


def test_prepare_relay_headers_with_secret() -> None:
    """Verify header composition with secret token."""
    headers = prepare_relay_headers(
        base_headers={"Authorization": "Bearer token123"},
        payload_str='{"msg":"hello"}',
        secret_token="key456",
        delivery_id="del_123",
        attempt_number=2,
    )

    assert headers["Authorization"] == "Bearer token123"
    assert headers["Content-Type"] == "application/json"
    assert headers["X-RelayForge-Delivery-Id"] == "del_123"
    assert headers["X-RelayForge-Attempt"] == "2"
    assert headers["X-RelayForge-Signature"].startswith("sha256=")


def test_prepare_relay_headers_without_secret() -> None:
    """Verify header composition when no secret token is configured."""
    headers = prepare_relay_headers(
        base_headers={},
        payload_str='{"msg":"hello"}',
        secret_token=None,
        delivery_id="del_999",
        attempt_number=1,
    )

    assert "X-RelayForge-Signature" not in headers
    assert headers["X-RelayForge-Delivery-Id"] == "del_999"
    assert headers["X-RelayForge-Attempt"] == "1"
