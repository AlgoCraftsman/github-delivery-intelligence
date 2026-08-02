"""Execute and contract-check every versioned dashboard query."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[1]
SQL_DIRECTORY = ROOT / "dashboards" / "sql"
CONTRACT_PATH = SQL_DIRECTORY / "query_contracts.json"


def _connection_string() -> str:
    return (
        f"host={os.getenv('DBT_POSTGRES_HOST', 'localhost')} "
        f"port={os.getenv('DBT_POSTGRES_PORT', '55432')} "
        f"dbname={os.getenv('DBT_POSTGRES_DB', 'github_analytics')} "
        f"user={os.getenv('DBT_POSTGRES_USER', 'github_analytics')} "
        f"password={os.getenv('DBT_POSTGRES_PASSWORD', 'local_only_change_me')}"
    )


def _serialized_rows(rows: list[dict[str, Any]]) -> str:
    return json.dumps(rows, default=str, ensure_ascii=False, separators=(",", ":"))


def _result_hash(rows: list[dict[str, Any]]) -> str:
    return hashlib.sha256(_serialized_rows(rows).encode()).hexdigest()


def _execute_query(connection: psycopg.Connection[Any], sql_path: Path) -> list[dict[str, Any]]:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(sql_path.read_text(encoding="utf-8"))
        return [dict(row) for row in cursor.fetchall()]


def _assert_contract(
    query_name: str,
    rows: list[dict[str, Any]],
    contract: dict[str, Any],
    *,
    verify_snapshot: bool,
) -> str:
    if len(rows) < contract["minimum_rows"]:
        raise AssertionError(
            f"{query_name}: expected at least {contract['minimum_rows']} rows, got {len(rows)}"
        )

    columns = set(rows[0])
    missing_columns = set(contract["required_columns"]) - columns
    if missing_columns:
        raise AssertionError(f"{query_name}: missing columns {sorted(missing_columns)}")

    grain_columns = contract["grain_columns"]
    grains = [tuple(row[column] for column in grain_columns) for row in rows]
    if len(grains) != len(set(grains)):
        raise AssertionError(f"{query_name}: duplicate result grain {grain_columns}")

    for column, expected_values in contract.get("required_values", {}).items():
        actual_values = {row[column] for row in rows}
        missing_values = set(expected_values) - actual_values
        if missing_values:
            raise AssertionError(
                f"{query_name}: {column} is missing fixture values {sorted(missing_values)}"
            )

    snapshot_hash = _result_hash(rows)
    if verify_snapshot and snapshot_hash != contract["fixture_sha256"]:
        raise AssertionError(
            f"{query_name}: fixture snapshot changed; expected {contract['fixture_sha256']}, "
            f"got {snapshot_hash}"
        )
    return snapshot_hash


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--print-snapshots",
        action="store_true",
        help="Print current fixture hashes instead of comparing checked-in hashes.",
    )
    args = parser.parse_args()

    contracts: dict[str, dict[str, Any]] = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    sql_files = sorted(SQL_DIRECTORY.glob("*.sql"))
    if {path.name for path in sql_files} != set(contracts):
        raise AssertionError(
            "query_contracts.json must describe every and only checked-in SQL query"
        )

    snapshots: dict[str, str] = {}
    with psycopg.connect(_connection_string(), autocommit=True) as connection:
        for sql_path in sql_files:
            first_rows = _execute_query(connection, sql_path)
            second_rows = _execute_query(connection, sql_path)
            if first_rows != second_rows:
                raise AssertionError(
                    f"{sql_path.name}: repeated execution returned different ordering"
                )
            snapshots[sql_path.name] = _assert_contract(
                sql_path.name,
                first_rows,
                contracts[sql_path.name],
                verify_snapshot=not args.print_snapshots,
            )
            print(f"PASS {sql_path.name}: {len(first_rows)} rows")

    if args.print_snapshots:
        print(json.dumps(snapshots, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
