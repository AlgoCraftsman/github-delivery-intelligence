"""Validate the image's Airflow metadata setup and bundled DAG discovery."""

import json
import subprocess
from typing import Any


def _json_output(command: list[str]) -> Any:
    result = subprocess.run(
        command,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    lines = result.stdout.splitlines()
    if not lines:
        raise RuntimeError(f"{command!r} returned no JSON output")
    return json.loads(lines[-1])


def main() -> int:
    """Build ephemeral metadata, reject import errors, and require the expected DAG."""

    subprocess.run(
        ["airflow", "db", "migrate"],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    subprocess.run(
        ["airflow", "dags", "reserialize"],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    errors = _json_output(["airflow", "dags", "list-import-errors", "--output=json"])
    if errors != []:
        raise RuntimeError(f"Airflow DAG import errors: {errors!r}")
    dags = _json_output(["airflow", "dags", "list", "--output=json"])
    if not isinstance(dags, list) or [
        item.get("dag_id") for item in dags if isinstance(item, dict)
    ] != ["github_backfill"]:
        raise RuntimeError(f"unexpected Airflow DAG inventory: {dags!r}")
    print('{"dags":["github_backfill"],"import_errors":[]}')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
