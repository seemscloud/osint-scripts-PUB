#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, Iterable, List

import requests
from requests.auth import HTTPBasicAuth


def build_session(username: str, password: str) -> requests.Session:
    session = requests.Session()
    session.auth = HTTPBasicAuth(username, password)
    session.headers.update({"Accept": "application/json"})
    return session


def api_get(
    session: requests.Session,
    base_url: str,
    path: str,
    params: Dict[str, Any],
    timeout: int = 30,
) -> requests.Response:
    url = f"{base_url.rstrip('/')}{path}"
    try:
        resp = session.get(url, params=params, timeout=timeout)
    except requests.RequestException as exc:
        raise RuntimeError(f"Request to {url} failed: {exc}") from exc
    if resp.status_code >= 400:
        raise RuntimeError(
            f"Request to {url} failed with {resp.status_code}: {resp.text[:500]}"
        )
    return resp


def list_all_dags(
    session: requests.Session,
    base_url: str,
    only_active: bool = True,
    page_size: int = 100,
) -> List[str]:
    dag_ids: List[str] = []
    offset = 0
    while True:
        resp = api_get(
            session,
            base_url,
            "/api/v1/dags",
            params={
                "only_active": str(only_active).lower(),
                "limit": page_size,
                "offset": offset,
            },
        )
        data = resp.json()
        dags: Iterable[Dict[str, Any]] = data.get("dags") or []
        batch = [dag["dag_id"] for dag in dags if dag.get("dag_id")]
        dag_ids.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return dag_ids


def list_tasks_for_dag(
    session: requests.Session, base_url: str, dag_id: str
) -> List[str]:
    resp = api_get(
        session,
        base_url,
        f"/api/v1/dags/{dag_id}/tasks",
        params={},
    )
    data = resp.json()
    tasks: Iterable[Dict[str, Any]] = data.get("tasks") or []
    return [task["task_id"] for task in tasks if task.get("task_id")]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Airflow diagnostics: list DAGs and their task_ids."
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Airflow host (default: %(default)s)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Airflow port (default: %(default)s)",
    )
    parser.add_argument(
        "--tls",
        action="store_true",
        help="Use HTTPS instead of HTTP when contacting Airflow",
    )
    parser.add_argument(
        "--username",
        default="airflow",
        help="Airflow username (default: %(default)s)",
    )
    parser.add_argument(
        "--password",
        default="airflow",
        help="Airflow password (default: %(default)s)",
    )
    parser.add_argument(
        "--dags",
        action="store_true",
        required=True,
        help="Required flag to emit DAG->tasks JSON lines.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    scheme = "https" if args.tls else "http"
    base_url = f"{scheme}://{args.host}:{args.port}"
    session = build_session(args.username, args.password)
    try:
        dag_ids = list_all_dags(session, base_url, only_active=True)
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"Failed to list DAGs: {exc}\n")
        return 1

    for dag_id in dag_ids:
        try:
            task_ids = list_tasks_for_dag(session, base_url, dag_id)
        except Exception as exc:  # noqa: BLE001
            sys.stderr.write(f"[{dag_id}] Failed to list tasks: {exc}\n")
            continue
        print(json.dumps({dag_id: {"tasks": task_ids}}, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

