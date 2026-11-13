"""Exponential backoff and decorrelated jitter calculations."""

import random
from datetime import datetime, timedelta

from src.config import RetryConfig
from src.models.delivery import utc_now


def calculate_backoff_delay(
    attempt: int,
    config: RetryConfig,
    random_generator: random.Random | None = None,
) -> float:
    """
    Calculate backoff sleep duration in seconds for a given retry attempt.

    Uses exponential scaling capped by max_backoff_seconds with full decorrelated jitter:
        nominal_delay = min(max_backoff, base_backoff * (backoff_factor ** (attempt - 1)))
        jittered_delay = nominal_delay * uniform(0.5, 1.5)

    Args:
        attempt: Sequential retry attempt index (1, 2, 3...).
        config: Retry configuration parameters.
        random_generator: Optional seeded Random instance for deterministic testing.

    Returns:
        Delay in seconds, bounded between base_backoff * 0.5 and max_backoff_seconds.
    """
    rng = random_generator or random

    if attempt <= 0:
        attempt = 1

    # Guard against exponent overflow for large attempt counts
    exponent = min(attempt - 1, 12)
    nominal_delay = config.base_backoff_seconds * (config.backoff_factor**exponent)
    capped_delay = min(nominal_delay, config.max_backoff_seconds)

    if not config.jitter:
        return capped_delay

    # Decorrelated full jitter between 50% and 150% of the nominal delay
    jitter_factor = rng.uniform(0.5, 1.5)
    jittered_delay = capped_delay * jitter_factor

    # Floor at 50% of base backoff and cap at max_backoff_seconds
    min_delay = max(0.1, config.base_backoff_seconds * 0.5)
    return round(min(config.max_backoff_seconds, max(min_delay, jittered_delay)), 4)


def calculate_next_retry_time(
    attempt: int,
    config: RetryConfig,
    from_time: datetime | None = None,
    random_generator: random.Random | None = None,
) -> datetime:
    """
    Compute scheduled UTC timestamp for the next retry attempt.

    Args:
        attempt: The upcoming retry attempt count (e.g. 1 after first failure).
        config: Retry configuration parameters.
        from_time: Base timestamp (defaults to current UTC time).
        random_generator: Optional seeded Random instance.

    Returns:
        Timezone-aware UTC datetime.
    """
    base_time = from_time or utc_now()
    delay_seconds = calculate_backoff_delay(attempt, config, random_generator)
    return base_time + timedelta(seconds=delay_seconds)
