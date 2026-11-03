"""Validated configuration for local Delta paths or Databricks managed tables.

Cloud object-store URIs are strings throughout. ``Path`` is deliberately used
only for the local backend: it corrupts URI schemes such as ``s3://``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import re
from urllib.parse import urlsplit

DEFAULT_NAMESPACE = "security_telemetry_demo"
VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
VALID_GENERATION = re.compile(r"^[A-Za-z0-9_-]+$")


def require_identifier(value: str, label: str) -> str:
    if not VALID_IDENTIFIER.fullmatch(value):
        raise ValueError(f"{label} must be a simple identifier, got {value!r}")
    return value


def storage_path(value: str | Path, label: str) -> str:
    """Keep a cloud URI or volume path intact, rejecting local/ambiguous paths."""
    result = str(value).rstrip("/")
    parsed = urlsplit(result)
    if parsed.query or parsed.fragment or ".." in parsed.path.split("/"):
        raise ValueError(f"{label} must be an unambiguous storage path")
    volume = result.startswith("/Volumes/") or result.startswith("dbfs:/Volumes/")
    object_store = parsed.scheme in {"s3", "s3a", "abfs", "abfss", "gs"} and bool(parsed.netloc)
    if not (volume or object_store):
        raise ValueError(f"{label} must use /Volumes/... or a complete cloud storage URI, got {result!r}")
    if volume and len(parsed.path.strip("/").split("/")) < 4:
        raise ValueError(f"{label} must include a catalog, schema, and volume")
    return result


def join_storage_path(root: str | Path, child: str) -> str:
    return str(root).rstrip("/") + "/" + child.lstrip("/")


def comparable_storage_path(value: str | Path) -> str:
    """Normalize the two supported spellings of a volume for containment checks."""
    result = str(value).rstrip("/")
    return result[5:] if result.startswith("dbfs:/Volumes/") else result


@dataclass
class PipelineConfig:
    events_path: str | Path
    customers_path: str | Path
    warehouse: str | Path = "warehouse"
    namespace: str = DEFAULT_NAMESPACE
    catalog: str = "local"
    bronze_checkpoint: str | Path | None = None
    silver_checkpoint: str | Path | None = None
    use_auto_loader: bool = False
    bronze_stream_id: str = "bronze_events"
    silver_stream_id: str = "silver_events"
    job_run_id: str = ""
    force_gold_assertion_failure: bool = False
    fail_after_sinks: list[str] = field(default_factory=list)
    shuffle_partitions: int = 1
    backend: str = "local"
    stream_generation: str = "v1"

    def __post_init__(self) -> None:
        if self.backend not in {"local", "databricks"}:
            raise ValueError("backend must be 'local' or 'databricks'")
        require_identifier(self.namespace, "namespace")
        require_identifier(self.catalog, "catalog")
        for label, value in (("stream_generation", self.stream_generation),
                             ("bronze_stream_id", self.bronze_stream_id),
                             ("silver_stream_id", self.silver_stream_id)):
            if not VALID_GENERATION.fullmatch(value):
                raise ValueError(f"{label} must contain only letters, digits, underscores, and hyphens")
        if self.shuffle_partitions < 1:
            raise ValueError("shuffle_partitions must be positive")
        if self.backend == "databricks":
            if self.catalog in {"local", "spark_catalog", "hive_metastore"}:
                raise ValueError("Databricks backend requires a Unity Catalog catalog")
            if not self.job_run_id.strip():
                raise ValueError("Databricks backend requires an explicit shared job_run_id")
            self.events_path = storage_path(self.events_path, "events_path")
            self.customers_path = storage_path(self.customers_path, "customers_path")
            if self.bronze_checkpoint is None or self.silver_checkpoint is None:
                raise ValueError("Databricks backend requires explicit persistent checkpoints")
            self.bronze_checkpoint = storage_path(self.bronze_checkpoint, "bronze_checkpoint")
            self.silver_checkpoint = storage_path(self.silver_checkpoint, "silver_checkpoint")
            # No output table path: managed-table storage is owned by Unity Catalog.
            self.warehouse = str(self.warehouse)
        else:
            if self.catalog not in {"local", "spark_catalog"}:
                raise ValueError("local backend supports only local or spark_catalog")
            for label in ("events_path", "customers_path", "warehouse", "bronze_checkpoint", "silver_checkpoint"):
                value = getattr(self, label)
                if value is not None and urlsplit(str(value)).scheme:
                    raise ValueError(f"{label} is a URI; select backend='databricks' for cloud paths")
            self.events_path = Path(self.events_path)
            self.customers_path = Path(self.customers_path)
            self.warehouse = Path(self.warehouse)
            self.bronze_checkpoint = Path(self.bronze_checkpoint) if self.bronze_checkpoint is not None else self.warehouse.parent / "checkpoints" / "bronze"
            self.silver_checkpoint = Path(self.silver_checkpoint) if self.silver_checkpoint is not None else self.warehouse.parent / "checkpoints" / "silver"
            if self.use_auto_loader:
                raise ValueError("Auto Loader requires backend='databricks'")
        bronze = self.canonical_checkpoint("bronze")
        silver = self.canonical_checkpoint("silver")
        if bronze == silver or bronze.startswith(silver + "/") or silver.startswith(bronze + "/"):
            raise ValueError("bronze and silver checkpoints must be distinct, non-nested directories")
        if not self.job_run_id:
            from uuid import uuid4
            self.job_run_id = f"{self.backend}-{uuid4().hex[:12]}"

    def qualified(self, table: str) -> str:
        require_identifier(table, "table")
        if self.catalog in {"local", "spark_catalog"}:
            return f"{self.namespace}.{table}"
        return f"{self.catalog}.{self.namespace}.{table}"

    def table_path(self, table: str) -> Path:
        require_identifier(table, "table")
        if self.backend != "local":
            raise ValueError("Databricks managed tables must be accessed by their qualified names")
        return self.warehouse / self.namespace / table

    def canonical_checkpoint(self, kind: str) -> str:
        if kind not in {"bronze", "silver"}:
            raise ValueError("stream kind must be bronze or silver")
        value = getattr(self, f"{kind}_checkpoint")
        if self.backend == "local":
            return Path(value).resolve().as_posix()
        return comparable_storage_path(value)

    def stream_identity(self, kind: str) -> str:
        if kind not in {"bronze", "silver"}:
            raise ValueError("stream kind must be bronze or silver")
        base = getattr(self, f"{kind}_stream_id")
        return f"{self.catalog}.{self.namespace}:{self.stream_generation}:{base}"

    def landing_dir_and_glob(self) -> tuple[Path | str, str]:
        if self.backend == "databricks":
            events = str(self.events_path)
            if events.endswith((".jsonl", ".json")):
                return tuple(events.rsplit("/", 1))
            return events.rstrip("/"), "*.jsonl"
        events = self.events_path
        if events.is_file():
            return events.parent, events.name
        if events.is_dir():
            return events, "*.jsonl"
        raise FileNotFoundError(f"events path does not exist: {events}")
