"""Single-process pipeline lock. One concurrent lakehouse run."""

from __future__ import annotations

import fcntl
from pathlib import Path


class SingleRunLock:
    """Exclusive non-blocking file lock.

    Lakeflow's max concurrent runs of one is mirrored locally. The lock is
    released when the context exits, including on failure.
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self._handle = None

    def __enter__(self) -> "SingleRunLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a+")
        try:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self._handle.close()
            self._handle = None
            raise RuntimeError(
                f"another pipeline run holds the single-run lock at {self.path}"
            ) from exc
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._handle is not None:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
            self._handle.close()
            self._handle = None
