"""Unit tests for exponential backoff and jitter logic."""

import random
from datetime import UTC, datetime

from src.config import RetryConfig
from src.engine.backoff import calculate_backoff_delay, calculate_next_retry_time


def test_exponential_growth_without_jitter() -> None:
    """Verify strictly deterministic exponential growth when jitter is disabled."""
    config = RetryConfig(
        base_backoff_seconds=1.0,
        backoff_factor=2.0,
        max_backoff_seconds=60.0,
        jitter=False,
    )

    # attempt 1: 1.0 * (2^0) = 1.0
    assert calculate_backoff_delay(1, config) == 1.0
    # attempt 2: 1.0 * (2^1) = 2.0
    assert calculate_backoff_delay(2, config) == 2.0
    # attempt 3: 1.0 * (2^2) = 4.0
    assert calculate_backoff_delay(3, config) == 4.0
    # attempt 4: 1.0 * (2^3) = 8.0
    assert calculate_backoff_delay(4, config) == 8.0


def test_max_backoff_ceiling() -> None:
    """Verify that backoff delays never exceed max_backoff_seconds."""
    config = RetryConfig(
        base_backoff_seconds=1.0,
        backoff_factor=2.0,
        max_backoff_seconds=10.0,
        jitter=False,
    )

    assert calculate_backoff_delay(10, config) == 10.0
    assert calculate_backoff_delay(20, config) == 10.0


def test_jitter_bounds() -> None:
    """Verify that jittered delays remain within [0.5 * nominal, 1.5 * nominal]."""
    config = RetryConfig(
        base_backoff_seconds=2.0,
        backoff_factor=2.0,
        max_backoff_seconds=100.0,
        jitter=True,
    )

    nominal_attempt_2 = 2.0 * (2.0**1)  # 4.0
    min_expected = nominal_attempt_2 * 0.5
    max_expected = nominal_attempt_2 * 1.5

    for _ in range(50):
        delay = calculate_backoff_delay(2, config)
        assert min_expected <= delay <= max_expected


def test_deterministic_random_generator() -> None:
    """Verify seeded Random produces reproducible results."""
    config = RetryConfig(base_backoff_seconds=1.0, jitter=True)
    rng1 = random.Random(42)
    rng2 = random.Random(42)

    val1 = calculate_backoff_delay(2, config, random_generator=rng1)
    val2 = calculate_backoff_delay(2, config, random_generator=rng2)

    assert val1 == val2


def test_edge_cases_attempt_indices() -> None:
    """Verify behavior with zero or negative attempt counts."""
    config = RetryConfig(base_backoff_seconds=1.0, jitter=False)
    assert calculate_backoff_delay(0, config) == 1.0
    assert calculate_backoff_delay(-5, config) == 1.0


def test_calculate_next_retry_time() -> None:
    """Verify calculated datetime is future UTC timestamp."""
    config = RetryConfig(base_backoff_seconds=2.0, jitter=False)
    fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)

    next_time = calculate_next_retry_time(1, config, from_time=fixed_now)
    assert next_time == datetime(2026, 1, 1, 12, 0, 2, tzinfo=UTC)
