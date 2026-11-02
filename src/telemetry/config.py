"""Paths and names for a local or later Databricks run."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


DEFAULT_NAMESPACE = "security_telemetry_demo"
VALID_IDENTIFIER = __import__("re").compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def require_identifier(value: str, label: str) -> str:
    if not VALID_IDENTIFIER.fullmatch(value):
        raise ValueError(f"{label} must be a simple identifier, got {value!r}")
    return value


@dataclass
class PipelineConfig:
    events_path: Path
    customers_path: Path
    warehouse: Path
    namespace: str = DEFAULT_NAMESPACE
    catalog: str = "local"
    bronze_checkpoint: Path | None = None
    silver_checkpoint: Path | None = None
    use_auto_loader: bool = False
    bronze_stream_id: str = "bronze_events"
    silver_stream_id: str = "silver_events"
    job_run_id: str = ""
    force_gold_assertion_failure: bool = False
    fail_after_sinks: list[str] = field(default_factory=list)
    shuffle_partitions: int = 1

    def __post_init__(self) -> None:
        self.events_path = Path(self.events_path)
        self.customers_path = Path(self.customers_path)
        self.warehouse = Path(self.warehouse)
        require_identifier(self.namespace, "namespace")
        if self.catalog not in {"local", "spark_catalog"}:
            require_identifier(self.catalog, "catalog")
        if self.bronze_checkpoint is None:
            self.bronze_checkpoint = self.warehouse.parent / "checkpoints" / "bronze"
        if self.silver_checkpoint is None:
            self.silver_checkpoint = self.warehouse.parent / "checkpoints" / "silver"
        self.bronze_checkpoint = Path(self.bronze_checkpoint)
        self.silver_checkpoint = Path(self.silver_checkpoint)
        if not self.job_run_id:
            # Local orchestrator identity. This is not a Databricks run id.
            from uuid import uuid4

            self.job_run_id = f"local-{uuid4().hex[:12]}"

    def qualified(self, table: str) -> str:
        require_identifier(table, "table")
        if self.catalog in {"local", "spark_catalog"}:
            return f"{self.namespace}.{table}"
        return f"{self.catalog}.{self.namespace}.{table}"

    def table_path(self, table: str) -> Path:
        require_identifier(table, "table")
        return self.warehouse / self.namespace / table

    def landing_dir_and_glob(self) -> tuple[Path, str]:
        events = self.events_path
        if events.is_file():
            return events.parent, events.name
        if events.is_dir():
            return events, "*.jsonl"
        raise FileNotFoundError(f"events path does not exist: {events}")
