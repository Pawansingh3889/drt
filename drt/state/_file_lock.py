"""Cross-process file locking for the local (non-object-store) state backend (#963).

``LocalDlqStore`` / ``LocalStateManager`` / ``LocalHistoryManager`` each guard
their read-modify-write cycle with a ``threading.Lock``, which only
serialises threads sharing one process. Two separate ``drt`` CLI invocations
(``drt run`` and ``drt retry``/``drt status`` are always separate OS
processes, never threads) racing the same file is last-writer-wins with no
way to detect it (#955, #962). :func:`cross_process_lock` wraps the same
critical section in an OS-level advisory lock (``fcntl.flock`` on POSIX,
``msvcrt.locking`` on Windows), so a second process blocks until the first
one finishes instead of racing it.

This is a real mutex, not the object-store tier's optimistic
generation/ETag retry (:mod:`drt.state._objectstore`): a local file has no
version token to precondition a write against, so callers get exclusion
instead of conflict detection. Advisory OS locks are held by the kernel
against the open file description, not the file's content, so a crashed
holder releases the lock automatically when its process exits: there is no
stale-lock file to clean up the way a plain "lock file exists" convention
would need.
"""

from __future__ import annotations

import os
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

DEFAULT_TIMEOUT_SECONDS = 30.0
_POLL_INTERVAL_SECONDS = 0.05


class FileLockTimeout(TimeoutError):
    """Could not acquire the cross-process file lock within the timeout.

    No other ``drt`` process is expected to hold a state/DLQ/history lock
    for more than a handful of read-modify-write cycles. A caller stuck
    this long is either genuinely deadlocked behind another process (killed
    mid-hold on a platform where that doesn't release the lock, or a real
    hang), or advisory locking isn't actually supported on this filesystem.
    Both are conditions to surface, not to wait out silently.
    """


if sys.platform == "win32":
    import msvcrt

    def _try_lock(fd: int) -> bool:
        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            return False

    def _unlock(fd: int) -> None:
        os.lseek(fd, 0, 0)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)

else:
    import fcntl

    def _try_lock(fd: int) -> bool:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            return False

    def _unlock(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_UN)


@contextmanager
def cross_process_lock(
    lock_path: Path, *, timeout: float = DEFAULT_TIMEOUT_SECONDS
) -> Iterator[None]:
    """Hold an exclusive OS-level lock on ``lock_path`` for the block's duration.

    ``lock_path`` is a dedicated lock file, created if missing and never
    removed (deleting it after use would let a concurrent locker acquire a
    lock on a since-recreated inode while this process still holds the
    original one, defeating the exclusion). Callers pick a lock path scoped
    to the resource they're protecting (e.g. ``state.json.lock`` for the
    whole state file, ``<sync>.jsonl.lock`` per DLQ/history file) so
    unrelated resources never contend on the same lock.

    Raises:
        FileLockTimeout: see the exception's docstring.
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    # Owner-only, matching the rest of .drt/ (credentials hardened to 0o600
    # in #650) rather than the world-readable 0o644 CodeQL flags by default.
    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        deadline = time.monotonic() + timeout
        while not _try_lock(fd):
            if time.monotonic() >= deadline:
                raise FileLockTimeout(
                    f"could not acquire lock on {lock_path} within {timeout}s "
                    "(another drt process is likely holding it)"
                )
            time.sleep(_POLL_INTERVAL_SECONDS)
        try:
            yield
        finally:
            _unlock(fd)
    finally:
        os.close(fd)
