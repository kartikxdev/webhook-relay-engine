"""Custom exception hierarchy for RelayForge engine."""


class RelayForgeException(Exception):
    """Base exception for all RelayForge errors."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class DeliveryError(RelayForgeException):
    """Base error occurred during a webhook forward delivery attempt."""

    def __init__(self, message: str, delivery_id: str | None = None) -> None:
        super().__init__(message)
        self.delivery_id = delivery_id


class HttpDeliveryError(DeliveryError):
    """Destination endpoint returned an HTTP error status code (4xx or 5xx)."""

    def __init__(
        self,
        message: str,
        status_code: int,
        response_body: str | None = None,
        delivery_id: str | None = None,
    ) -> None:
        super().__init__(message, delivery_id=delivery_id)
        self.status_code = status_code
        self.response_body = response_body


class NetworkDeliveryError(DeliveryError):
    """Underlying network connection failure, DNS resolution error, or socket error."""


class TimeoutDeliveryError(DeliveryError):
    """HTTP client connection or read operation timed out."""


class MaxRetriesExceededError(DeliveryError):
    """Webhook reached the maximum allowable retry attempts without succeeding."""


class SignatureComputationError(RelayForgeException):
    """Failure while generating cryptographic signature for payload."""


class DeadLetterReplayError(RelayForgeException):
    """Error encountered during dead-letter queue replay operation."""
