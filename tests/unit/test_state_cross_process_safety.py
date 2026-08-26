"""Cross-process proof tests for the local state stores (#963).

These races only exist between genuinely separate OS processes racing a
read-modify-write on the same file. An in-process mock (a fake clock, a
patched method) cannot reproduce them, which is exactly why #955/#962 could
fix ``LocalDlqStore.reconcile()``'s in-memory logic but explicitly could not
close the cross-process gap this issue (#963) targets. Every test here
spawns real ``multiprocessing.Process`` workers against a shared ``tmp_path``
project directory, synchronised with a ``Barrier`` so their operations
actually overlap instead of running back-to-back.
"""

from __future__ import annotations

import json
import multiprocessing
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from multiprocessing.synchronize import Barrier
from pathlib import Path

from drt.state.dlq import DeadLetter, LocalDlqStore
from drt.state.history import HistoryEntry, LocalHistoryManager
from drt.state.manager import LocalStateManager, SyncState

# Short so a genuine deadlock/regression fails the test in seconds, not
# whatever the production default (30s) would otherwise wait.
_TEST_LOCK_TIMEOUT = 10.0
_JOIN_TIMEOUT = 15.0

# Explicit "spawn" rather than the bare module-level API (which defaults to
# "fork" on Linux): fork()ing a multi-threaded process (pytest itself runs
# background threads) is unsafe and Python warns on it; spawn is also the
# only choice that behaves the same on every platform this suite targets.
_mp = multiprocessing.get_context("spawn")


# -- LocalDlqStore --------------------------------------------------------


def _dlq_append_worker(project_dir: str, sync_name: str, idx: int, barrier: Barrier) -> None:
    store = LocalDlqStore(Path(project_dir), lock_timeout=_TEST_LOCK_TIMEOUT)
    entry = DeadLetter(record={"idx": idx}, error_message=f"boom {idx}")
    barrier.wait(timeout=_JOIN_TIMEOUT)
    store.append(sync_name, [entry])


def test_concurrent_appends_from_separate_processes_are_not_lost(tmp_path: Path) -> None:
    n = 8
    barrier = _mp.Barrier(n)
    procs = [
        _mp.Process(
            target=_dlq_append_worker, args=(str(tmp_path), "s", i, barrier)
        )
        for i in range(n)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=_JOIN_TIMEOUT)
        assert p.exitcode == 0

    store = LocalDlqStore(tmp_path)
    entries = store.read("s")
    assert store.depth("s") == n
    assert sorted(e.record["idx"] for e in entries) == list(range(n))


def _dlq_reconcile_worker(
    project_dir: str, sync_name: str, remove_id: str, barrier: Barrier
) -> None:
    store = LocalDlqStore(Path(project_dir), lock_timeout=_TEST_LOCK_TIMEOUT)
    barrier.wait(timeout=_JOIN_TIMEOUT)
    store.reconcile(sync_name, remove_ids={remove_id})


def _dlq_concurrent_append_worker(
    project_dir: str, sync_name: str, entry: DeadLetter, barrier: Barrier
) -> None:
    store = LocalDlqStore(Path(project_dir), lock_timeout=_TEST_LOCK_TIMEOUT)
    barrier.wait(timeout=_JOIN_TIMEOUT)
    store.append(sync_name, [entry])


def test_reconcile_does_not_drop_a_concurrent_append(tmp_path: Path) -> None:
    """The exact #955/#962 scenario: ``drt retry`` reconciling (removing one
    replayed entry) while a genuinely separate ``drt run`` process appends a
    new failure to the same queue. Neither operation may lose the other's.
    """
    store = LocalDlqStore(tmp_path)
    seed = [DeadLetter(record={"seed": i}, error_message="pre-existing") for i in range(3)]
    store.append("s", seed)
    to_remove = seed[0].id
    new_entry = DeadLetter(record={"new": True}, error_message="fresh failure")

    barrier = _mp.Barrier(2)
    p_reconcile = _mp.Process(
        target=_dlq_reconcile_worker, args=(str(tmp_path), "s", to_remove, barrier)
    )
    p_append = _mp.Process(
        target=_dlq_concurrent_append_worker, args=(str(tmp_path), "s", new_entry, barrier)
    )
    p_reconcile.start()
    p_append.start()
    p_reconcile.join(timeout=_JOIN_TIMEOUT)
    p_append.join(timeout=_JOIN_TIMEOUT)
    assert p_reconcile.exitcode == 0
    assert p_append.exitcode == 0

    final = store.read("s")
    final_ids = {e.id for e in final}
    # The removed seed entry is gone, the other two seeds survived, and the
    # concurrent append landed: nothing silently overwritten either way.
    assert to_remove not in final_ids
    assert seed[1].id in final_ids
    assert seed[2].id in final_ids
    assert new_entry.id in final_ids
    assert len(final) == 3  # 3 seeds - 1 removed + 1 appended


# -- LocalStateManager ------------------------------------------------------


def _state_save_worker(project_dir: str, sync_name: str, barrier: Barrier) -> None:
    mgr = LocalStateManager(Path(project_dir), lock_timeout=_TEST_LOCK_TIMEOUT)
    state = SyncState(
        sync_name=sync_name,
        last_run_at=mgr.now(),
        records_synced=1,
        status="success",
    )
    barrier.wait(timeout=_JOIN_TIMEOUT)
    mgr.save_sync(state)


def test_concurrent_save_sync_from_separate_processes_are_not_lost(tmp_path: Path) -> None:
    n = 8
    barrier = _mp.Barrier(n)
    sync_names = [f"sync_{i}" for i in range(n)]
    procs = [
        _mp.Process(target=_state_save_worker, args=(str(tmp_path), name, barrier))
        for name in sync_names
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=_JOIN_TIMEOUT)
        assert p.exitcode == 0

    mgr = LocalStateManager(tmp_path)
    all_states = mgr.get_all()
    assert set(all_states) == set(sync_names)


# -- LocalHistoryManager -----------------------------------------------------


def _history_append_burst_worker(
    project_dir: str, sync_name: str, count: int, barrier: Barrier
) -> None:
    mgr = LocalHistoryManager(Path(project_dir), lock_timeout=_TEST_LOCK_TIMEOUT)
    barrier.wait(timeout=_JOIN_TIMEOUT)
    now = datetime.now(timezone.utc)
    for i in range(count):
        mgr.append(
            HistoryEntry(
                sync_name=sync_name,
                started_at=(now + timedelta(microseconds=i)).isoformat(),
                completed_at=(now + timedelta(microseconds=i, milliseconds=1)).isoformat(),
                duration_seconds=0.001,
                status="success",
                records_synced=1,
                records_failed=0,
                run_id=f"run-{i}",
            )
        )


def _history_prune_worker(project_dir: str, sync_name: str, barrier: Barrier) -> None:
    mgr = LocalHistoryManager(Path(project_dir), lock_timeout=_TEST_LOCK_TIMEOUT)
    barrier.wait(timeout=_JOIN_TIMEOUT)
    for _ in range(5):
        mgr.prune(sync_name, retention_days=1)


def test_prune_does_not_drop_a_concurrent_append(tmp_path: Path) -> None:
    """The history analogue of #955/#962: a retention ``prune()`` rewriting
    the file while a separate process is mid-burst appending new entries
    must not silently discard any of those appends.
    """
    mgr = LocalHistoryManager(tmp_path)
    old = datetime(2000, 1, 1, tzinfo=timezone.utc)
    # Seed old entries directly so the first prune() has real work to do:
    # appending them via mgr.append() would use "now", not an old timestamp.
    history_dir = tmp_path / ".drt" / "history"
    history_dir.mkdir(parents=True)
    seed_lines = []
    for i in range(3):
        entry = HistoryEntry(
            sync_name="s",
            started_at=(old + timedelta(seconds=i)).isoformat(),
            completed_at=(old + timedelta(seconds=i, milliseconds=1)).isoformat(),
            duration_seconds=0.001,
            status="success",
            records_synced=1,
            records_failed=0,
        )
        seed_lines.append(json.dumps(asdict(entry), default=str))
    (history_dir / "s.jsonl").write_text("\n".join(seed_lines) + "\n")

    n_appends = 40
    barrier = _mp.Barrier(2)
    p_append = _mp.Process(
        target=_history_append_burst_worker, args=(str(tmp_path), "s", n_appends, barrier)
    )
    p_prune = _mp.Process(
        target=_history_prune_worker, args=(str(tmp_path), "s", barrier)
    )
    p_append.start()
    p_prune.start()
    p_append.join(timeout=_JOIN_TIMEOUT)
    p_prune.join(timeout=_JOIN_TIMEOUT)
    assert p_append.exitcode == 0
    assert p_prune.exitcode == 0

    kept = mgr.read("s", limit=1000)
    run_ids = {e.run_id for e in kept}
    # All 40 fresh appends survived (none silently dropped by a racing
    # prune), and the 3 seeded old entries were pruned away.
    assert run_ids == {f"run-{i}" for i in range(n_appends)}
    assert len(kept) == n_appends
