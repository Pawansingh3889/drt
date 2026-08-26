"""Tests for drt.state._file_lock: the OS-level cross-process lock (#963)."""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from drt.state._file_lock import FileLockTimeout, cross_process_lock


def test_acquires_and_releases(tmp_path: Path) -> None:
    lock_path = tmp_path / "x.lock"
    with cross_process_lock(lock_path):
        pass
    # Released cleanly: a second, sequential acquire doesn't hang.
    with cross_process_lock(lock_path):
        pass


def test_creates_lock_file_and_parent_dirs(tmp_path: Path) -> None:
    lock_path = tmp_path / "nested" / "dir" / "x.lock"
    with cross_process_lock(lock_path):
        assert lock_path.exists()


def test_second_holder_blocks_until_first_releases(tmp_path: Path) -> None:
    """Two independent file descriptors on the same path, even within one
    process, genuinely serialise under ``flock``/``msvcrt.locking``. That is
    what lets this test prove blocking without spawning a real process.
    """
    lock_path = tmp_path / "x.lock"
    first_holds = threading.Event()
    second_acquired_at: list[float] = []
    first_released_at: list[float] = []

    def hold_then_release() -> None:
        with cross_process_lock(lock_path):
            first_holds.set()
            time.sleep(0.2)
        first_released_at.append(time.monotonic())

    def wait_then_acquire() -> None:
        first_holds.wait(timeout=2)
        with cross_process_lock(lock_path, timeout=5):
            second_acquired_at.append(time.monotonic())

    t1 = threading.Thread(target=hold_then_release)
    t2 = threading.Thread(target=wait_then_acquire)
    t1.start()
    t2.start()
    t1.join(timeout=5)
    t2.join(timeout=5)

    assert first_released_at and second_acquired_at
    # The second thread could only acquire at or after the first released.
    assert second_acquired_at[0] >= first_released_at[0]


def test_timeout_raises_when_lock_held(tmp_path: Path) -> None:
    lock_path = tmp_path / "x.lock"
    holder_ready = threading.Event()
    release = threading.Event()

    def hold_until_released() -> None:
        with cross_process_lock(lock_path):
            holder_ready.set()
            release.wait(timeout=5)

    t = threading.Thread(target=hold_until_released)
    t.start()
    holder_ready.wait(timeout=2)
    try:
        with pytest.raises(FileLockTimeout):
            with cross_process_lock(lock_path, timeout=0.2):
                pass  # pragma: no cover - never reached
    finally:
        release.set()
        t.join(timeout=5)


def test_lock_file_survives_and_is_reusable(tmp_path: Path) -> None:
    """The lock file is never deleted after use (see the module docstring on
    why): a third acquire against the same still-existing file must work.
    """
    lock_path = tmp_path / "x.lock"
    for _ in range(3):
        with cross_process_lock(lock_path):
            pass
    assert lock_path.exists()
