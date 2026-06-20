# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Polling helper shared by the HA-docker integration tests.

Replaces blanket ``time.sleep(N)`` "wait for the proxy to settle" pauses
with predicate-driven polling. If the condition becomes true in 100 ms,
the test takes 100 ms; if it never becomes true, the test fails at the
same deadline the old hard sleep would have walked past silently.
"""

import time
from typing import Any, Callable, TypeVar

T = TypeVar("T")


def wait_until(
    probe: Callable[[], T],
    predicate: Callable[[T], bool],
    *,
    timeout: float,
    interval: float = 0.2,
) -> T:
    """Poll ``probe()`` until ``predicate(result)`` is True or ``timeout``
    seconds elapse.

    Returns the result that satisfied the predicate. Raises
    ``AssertionError`` on timeout, including the last observed result
    so the test failure is diagnosable. ``interval`` is the polling
    cadence (200 ms by default — fast enough for a browser-side state
    transition, slow enough that the JS bridge isn't hammered).
    """
    deadline = time.monotonic() + timeout
    last: T = probe()
    while True:
        if predicate(last):
            return last
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"wait_until did not converge within {timeout}s; last result: {last!r}"
            )
        time.sleep(interval)
        last = probe()


def wait_for_session_ready(chrome: Any, *, timeout: float = 6.0) -> None:
    """Wait until the browser's ``hass.connection`` is live and
    ``hass.states`` has been populated by the proxy's mirror snapshot.

    Replaces a blanket ``time.sleep(N)`` "let the session settle" pause
    at the top of integration tests that drive subscription routing
    behaviour from the browser.
    """
    wait_until(
        lambda: chrome.execute_script(
            "const ha = document.querySelector('home-assistant');"
            "return !!(ha && ha.hass && ha.hass.connection && "
            "Object.keys(ha.hass.states || {}).length > 0);"
        ),
        lambda ready: ready,
        timeout=timeout,
    )
