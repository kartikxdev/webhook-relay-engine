"""Domain enumerations for webhook delivery lifecycle."""

from enum import StrEnum


class WebhookStatus(StrEnum):
    """Lifecycle states of a webhook delivery."""

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    DELIVERED = "DELIVERED"
    FAILED = "FAILED"
    DEAD_LETTER = "DEAD_LETTER"


class HttpMethod(StrEnum):
    """Allowed HTTP forward methods."""

    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"


class AttemptOutcome(StrEnum):
    """Outcome category for individual delivery attempt."""

    SUCCESS = "SUCCESS"
    HTTP_ERROR = "HTTP_ERROR"
    NETWORK_ERROR = "NETWORK_ERROR"
    TIMEOUT = "TIMEOUT"
