"""Sync execution history — append-only JSONL per sync.

Each sync writes one HistoryEntry per execution to ``.drt/history/<sync_name>.jsonl``.
The CLI exposes recent entries via ``drt status --history``; the MCP server exposes
them as ``drt_get_history`` so AI agents can query past runs.

Why JSONL per-sync:
- POSIX ``O_APPEND`` makes a single-line write atomic against *other appends*:
  two appends can never tear or interleave, with or without a lock. That is
  not the same as safe against ``prune()``'s wholesale rewrite (#963): a
  prune reading the file before an append lands and replacing it after would
  silently drop that append, so both now take the same OS-level lock
  (:mod:`drt.state._file_lock`) rather than relying on ``O_APPEND`` alone.
- Per-sync files keep retention prune trivial (rewrite the file once it crosses
  the cutoff) and let ``drt status --history <sync_name>`` read just one file.
- JSONL is grep/jq friendly without a database dependency.

Rust-migration note: pure JSON I/O, no rich types — straightforward to port.
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Protocol, runtime_checkable

from drt.state._file_lock import DEFAULT_TIMEOUT_SECONDS, FileLockTimeout, cross_process_lock

logger = logging.getLogger(__name__)


@dataclass
class HistoryEntry:
    """One execution of one sync."""

    sync_name: str
    started_at: str  # ISO-8601 UTC
    completed_at: str  # ISO-8601 UTC
    duration_seconds: float
    status: str  # "success" | "partial" | "failed"
    records_synced: int
    records_failed: int
    errors: list[str] = field(default_factory=list)  # truncated to first 5
    cursor_value_used: str | None = None  # for incremental syncs
    dry_run: bool = False  # always False on disk; reserved for future use
    # Correlation IDs (#762) — see drt._identifiers. Both None on entries
    # written before this field existed; the JSONL reader tolerates the
    # missing key via the dataclass default, so no migration is needed.
    run_id: str | None = None
    sync_run_id: str | None = None


@runtime_checkable
class HistoryStore(Protocol):
    """Append and read per-sync execution history (#756).

    Extracted so history can outlive the runner that produced it — a fresh CI
    checkout currently shows nothing, and the docs manifest's ``runs`` data
    (#698) is blank in any ephemeral container.
    """

    def append(self, entry: HistoryEntry) -> None:
        """Append one entry.

        Best-effort: a failure to persist must never fail the sync it's
        recording, so implementations log at WARNING and swallow the error
        rather than raise. Unlike ``StateStore.save_sync``, there is no
        contention error a caller needs to catch here.
        """
        ...

    def read(self, sync_name: str | None = None, limit: int = 20) -> list[HistoryEntry]:
        """Most recent ``limit`` entries, newest first; all syncs when ``sync_name`` is None."""
        ...

    def prune(self, sync_name: str, retention_days: int) -> int:
        """Drop entries older than ``retention_days``; return how many went."""
        ...


class LocalHistoryManager:
    """Append-only per-sync execution history.

    Files live under ``<project_dir>/.drt/history/<sync_name>.jsonl``. All
    writes append a single JSON object per line. Reads return the most recent
    N entries (newest first).

    Cross-process safety (#963): both ``append`` and ``prune`` take the same
    OS-level file lock (:mod:`drt.state._file_lock`). ``append``'s own write
    was already safe against *other appends* via POSIX ``O_APPEND``, but not
    against ``prune``'s wholesale rewrite: an append landing between
    prune's read and its replace would be silently dropped unless append
    also holds the lock prune waits on. Matching ``HistoryStore``'s
    best-effort contract, a lock timeout on either method is logged and
    swallowed rather than raised.
    """

    _MAX_ERRORS_PER_ENTRY = 5

    def __init__(
        self,
        project_dir: Path = Path("."),
        *,
        lock_timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self._dir = project_dir / ".drt" / "history"
        self._lock_timeout = lock_timeout
        self._lock = threading.Lock()  # serialises append/prune within one process

    def _file_for(self, sync_name: str) -> Path:
        return self._dir / f"{sync_name}.jsonl"

    def _lock_path(self, sync_name: str) -> Path:
        return self._dir / f"{sync_name}.jsonl.lock"

    def append(self, entry: HistoryEntry) -> None:
        """Append one entry. Best-effort — failures are logged at WARNING and
        never propagate (sync results must not depend on history persistence).

        Takes the cross-process lock (#963) even though the write itself is
        ``O_APPEND``-atomic on its own: without it, this write could still
        land inside a concurrent ``prune()``'s read-rewrite-replace window
        on another process and be silently discarded when that rewrite
        completes.
        """
        try:
            self._dir.mkdir(parents=True, exist_ok=True)
            # Truncate errors to bound disk growth on long-failing syncs.
            entry.errors = entry.errors[: self._MAX_ERRORS_PER_ENTRY]
            line = json.dumps(asdict(entry), default=str)
            with self._lock:
                with cross_process_lock(
                    self._lock_path(entry.sync_name), timeout=self._lock_timeout
                ):
                    with self._file_for(entry.sync_name).open("a") as f:
                        f.write(line + "\n")
        except OSError as exc:  # disk full, permission denied, etc.
            logger.warning("history append failed for sync=%s: %s", entry.sync_name, exc)
        except FileLockTimeout as exc:
            logger.warning("history append failed for sync=%s: %s", entry.sync_name, exc)

    def read(
        self,
        sync_name: str | None = None,
        limit: int = 20,
    ) -> list[HistoryEntry]:
        """Return up to ``limit`` most recent entries, newest first.

        If ``sync_name`` is given, only that sync's history is read; otherwise
        all syncs are merged and re-sorted by ``started_at``.
        """
        if not self._dir.exists():
            return []

        files: list[Path]
        if sync_name is not None:
            target = self._file_for(sync_name)
            files = [target] if target.exists() else []
        else:
            files = sorted(self._dir.glob("*.jsonl"))

        entries: list[HistoryEntry] = []
        for path in files:
            entries.extend(_read_jsonl(path))

        entries.sort(key=lambda e: e.started_at, reverse=True)
        return entries[:limit]

    def prune(self, sync_name: str, retention_days: int) -> int:
        """Drop entries older than ``retention_days`` for one sync.

        Returns the number of entries removed. No-op if the file doesn't exist.
        Rewrites the file in place under a process-local lock plus an
        OS-level cross-process lock (#963) so a concurrent ``append`` from a
        genuinely separate ``drt`` process (not just another thread) isn't
        lost in the gap between this method's read and its replace.
        """
        path = self._file_for(sync_name)
        if not path.exists():
            return 0

        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

        with self._lock:
            try:
                with cross_process_lock(self._lock_path(sync_name), timeout=self._lock_timeout):
                    kept: list[HistoryEntry] = []
                    removed = 0
                    for entry in _read_jsonl(path):
                        try:
                            started = datetime.fromisoformat(entry.started_at)
                        except ValueError:
                            # Malformed timestamp: keep so a human can inspect.
                            kept.append(entry)
                            continue
                        if started < cutoff:
                            removed += 1
                        else:
                            kept.append(entry)

                    if removed == 0:
                        return 0

                    # Rewrite (entries are already in the order we want,
                    # preserved from the original file).
                    tmp = path.with_suffix(".jsonl.tmp")
                    with tmp.open("w") as f:
                        for entry in kept:
                            f.write(json.dumps(asdict(entry), default=str) + "\n")
                    tmp.replace(path)
                    return removed
            except FileLockTimeout:
                # Best-effort, like HistoryStore.append: never fail the
                # caller over a retention housekeeping pass.
                logger.warning(
                    "history prune failed for sync=%s: lock timed out after %ss",
                    sync_name,
                    self._lock_timeout,
                )
                return 0


# Back-compat alias — see the note on ``StateManager`` in state/manager.py.
HistoryManager = LocalHistoryManager


def _read_jsonl(path: Path) -> list[HistoryEntry]:
    """Read all entries from one JSONL file. Skips malformed lines with a warning."""
    entries: list[HistoryEntry] = []
    try:
        with path.open() as f:
            for lineno, raw in enumerate(f, start=1):
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    data = json.loads(raw)
                    entries.append(HistoryEntry(**data))
                except (json.JSONDecodeError, TypeError) as exc:
                    logger.warning(
                        "history: skipping malformed line %s in %s: %s",
                        lineno,
                        path,
                        exc,
                    )
    except OSError as exc:
        logger.warning("history: cannot read %s: %s", path, exc)
    return entries
