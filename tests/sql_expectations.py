"""Parse the tiny expected-result tables published in sql/analysis.sql."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = REPO_ROOT / "sql" / "analysis.sql"


def _cell(text: str):
    value = text.strip()
    if value.lower() in {"null", "none"}:
        return None
    if len(value) == 10 and value[4] == "-" and value[7] == "-":
        return date.fromisoformat(value)
    if "/" in value:
        numerator, denominator = value.split("/", 1)
        return float(numerator) / float(denominator)
    try:
        if any(token in value for token in (".", "e", "E")):
            return float(value)
        return int(value)
    except ValueError:
        return value


def load_queries(path: Path = SQL_PATH) -> list[dict]:
    text = path.read_text(encoding="utf-8")
    chunks = []
    current: dict | None = None
    statement: list[str] = []
    for line in text.splitlines():
        if line.startswith("-- QUERY:"):
            if current is not None:
                current["sql"] = "\n".join(statement).strip().rstrip(";")
                chunks.append(current)
            current = {
                "name": line.split(":", 1)[1].strip(),
                "question": "",
                "columns": [],
                "expected": [],
            }
            statement = []
            continue
        if current is None:
            continue
        if line.startswith("-- QUESTION:"):
            current["question"] = line.split(":", 1)[1].strip()
        elif line.startswith("-- EXPECTED_COLUMNS:"):
            current["columns"] = [part.strip() for part in line.split(":", 1)[1].split(",")]
        elif line.startswith("-- EXPECTED:"):
            current["expected"].append(tuple(_cell(part) for part in line.split(":", 1)[1].split("|")))
        elif not line.startswith("--"):
            statement.append(line)
    if current is not None:
        current["sql"] = "\n".join(statement).strip().rstrip(";")
        chunks.append(current)
    return chunks


def values_close(actual, expected, tolerance: float = 1e-6) -> bool:
    if isinstance(expected, float) or isinstance(actual, float):
        if actual is None or expected is None:
            return actual is expected
        return abs(float(actual) - float(expected)) <= tolerance
    if isinstance(actual, datetime) and isinstance(expected, date):
        return actual.date() == expected
    return actual == expected
