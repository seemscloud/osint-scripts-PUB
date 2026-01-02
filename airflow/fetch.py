#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import threading
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple
from datetime import datetime
import ast

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
    params: Optional[Dict[str, Any]] = None,
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


def list_dags(
    session: requests.Session,
    base_url: str,
    only_active: bool = True,
    limit: int = 1000,
) -> Iterable[str]:
    resp = api_get(
        session,
        base_url,
        "/api/v1/dags",
        params={"only_active": str(only_active).lower(), "limit": limit},
    )
    data = resp.json()
    for dag in data.get("dags") or []:
        dag_id = dag.get("dag_id")
        if dag_id:
            yield dag_id


def get_latest_dag_run_id(
    session: requests.Session, base_url: str, dag_id: str
) -> str:
    resp = api_get(
        session,
        base_url,
        f"/api/v1/dags/{dag_id}/dagRuns",
        params={"order_by": "-execution_date", "limit": 1},
    )
    data = resp.json()
    runs = data.get("dag_runs") or []
    if not runs:
        raise RuntimeError(f"No dag runs found for {dag_id}")
    run = runs[0]
    dag_run_id = run.get("dag_run_id") or run.get("run_id")
    if not dag_run_id:
        raise RuntimeError(f"Unexpected dag run payload: {run}")
    return dag_run_id


def get_task_instances(
    session: requests.Session, base_url: str, dag_id: str, dag_run_id: str
) -> Iterable[Dict[str, Any]]:
    resp = api_get(
        session,
        base_url,
        f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
        params={"limit": 1000},
    )
    data = resp.json()
    return data.get("task_instances") or []


def list_successful_dag_runs(
    session: requests.Session, base_url: str, dag_id: str, limit: int = 10
) -> list[Dict[str, Any]]:
    resp = api_get(
        session,
        base_url,
        f"/api/v1/dags/{dag_id}/dagRuns",
        params={"state": "success", "order_by": "-execution_date", "limit": limit},
    )
    data = resp.json()
    return data.get("dag_runs") or []


def list_tasks(
    session: requests.Session, base_url: str, dag_id: str
) -> Iterable[str]:
    resp = api_get(
        session,
        base_url,
        f"/api/v1/dags/{dag_id}/tasks",
        params=None,
    )
    data = resp.json()
    for task in data.get("tasks") or []:
        task_id = task.get("task_id")
        if task_id:
            yield task_id


def get_task_detail(
    session: requests.Session, base_url: str, dag_id: str, task_id: str
) -> Dict[str, Any]:
    resp = api_get(
        session,
        base_url,
        f"/api/v1/dags/{dag_id}/tasks/{task_id}",
        params=None,
    )
    return resp.json()


def get_dag_file_token(
    session: requests.Session, base_url: str, dag_id: str
) -> Optional[str]:
    resp = api_get(
        session,
        base_url,
        f"/api/v1/dags/{dag_id}",
        params=None,
    )
    data = resp.json()
    return data.get("file_token")


def pick_try_number(task_instance: Dict[str, Any]) -> int:
    # Prefer reported try_number; fallback to next_try_number - 1; else 1.
    try_number = task_instance.get("try_number")
    if isinstance(try_number, int) and try_number > 0:
        return try_number
    next_try = task_instance.get("next_try_number")
    if isinstance(next_try, int) and next_try > 0:
        return max(1, next_try - 1)
    return 1


def fetch_log_text(
    session: requests.Session,
    base_url: str,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int,
) -> str:
    def _normalize_line(val: Any) -> str:
        # Convert to string and unescape common newline sequences
        s = str(val)
        return s.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\r", "\n")

    resp = api_get(
        session,
        base_url,
        f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}",
        params={"full_content": "true"},
    )
    content_type = resp.headers.get("Content-Type", "")
    if "application/json" in content_type:
        try:
            payload = resp.json()
        except ValueError:
            return resp.text
        content = payload.get("content")
        if isinstance(content, list):
            # Handle lists of strings or tuples/lists; join cleanly with newlines
            parts = []
            for part in content:
                if isinstance(part, (list, tuple)) and part:
                    candidate = part[-1]
                    parts.append(_normalize_line(candidate))
                else:
                    parts.append(_normalize_line(part))
            return "\n".join(parts)
        if isinstance(content, str):
            return _normalize_line(content)
        return _normalize_line(content)
    # Fallback: try to parse Python-like list/tuple repr (Airflow sometimes returns that)
    try:
        parsed = ast.literal_eval(resp.text)
        if isinstance(parsed, list):
            parts = []
            for part in parsed:
                if isinstance(part, (list, tuple)) and part:
                    candidate = part[-1]
                    parts.append(_normalize_line(candidate))
                else:
                    parts.append(_normalize_line(part))
            return "\n".join(parts)
    except Exception:
        pass
    return _normalize_line(resp.text)


def sanitize_name(name: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in name)


def save_code(text: str, output_dir: Path, dag_id: str, task_id: str) -> Path:
    base_code_root = Path(__file__).resolve().parent / "code"
    code_dir = base_code_root / sanitize_name(dag_id)
    code_dir.mkdir(parents=True, exist_ok=True)
    path = code_dir / f"{sanitize_name(task_id)}.py"
    path.write_text(text)
    return path


def fetch_dag_source(
    session: requests.Session,
    base_url: str,
    dag_id: str,
    dag_source_cache: Dict[str, str],
    dag_source_unavailable: set[str],
    dag_source_cache_lock: threading.Lock,
    dag_source_unavailable_lock: threading.Lock,
) -> Optional[str]:
    with dag_source_cache_lock:
        if dag_id in dag_source_cache:
            cached = dag_source_cache[dag_id]
            return cached or None
    try:
        resp = api_get(session, base_url, f"/api/v1/dags/{dag_id}/dagSource", params=None)
        ts = datetime.now().isoformat()
        print(f'{{"timestamp":"{ts}","dag":"{dag_id}","action":"fetch","status_code":{resp.status_code}}}')
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"[{dag_id}] Failed to fetch DAG source: {exc}\n")
        with dag_source_cache_lock:
            dag_source_cache[dag_id] = ""
        with dag_source_unavailable_lock:
            dag_source_unavailable.add(dag_id)
        return None

    content_type = resp.headers.get("Content-Type", "")
    text: Optional[str]
    if "application/json" in content_type:
        try:
            payload = resp.json()
        except ValueError:
            payload = {}
        text = payload.get("content") or resp.text
    else:
        text = resp.text

    with dag_source_cache_lock:
        dag_source_cache[dag_id] = text or ""
    if not text:
        with dag_source_unavailable_lock:
            dag_source_unavailable.add(dag_id)
    return text


def fetch_task_code(
    session: requests.Session,
    base_url: str,
    dag_id: str,
    task_id: str,
    file_cache: Dict[str, str],
    dag_source_cache: Dict[str, str],
    dag_source_unavailable: set[str],
    dag_file_token_cache: Dict[str, str],
    file_cache_lock: threading.Lock,
    dag_source_cache_lock: threading.Lock,
    dag_source_unavailable_lock: threading.Lock,
    dag_file_token_cache_lock: threading.Lock,
) -> Optional[Tuple[str, Optional[str]]]:
    try:
        detail = get_task_detail(session, base_url, dag_id, task_id)
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"[{dag_id}] Failed to get task detail for {task_id}: {exc}\n")
        return None

    file_token = detail.get("file_token")
    if file_token:
        with file_cache_lock:
            if file_token in file_cache:
                return file_cache[file_token], None
        try:
            resp = api_get(session, base_url, f"/api/v1/dagSources/{file_token}", params=None)
            ts = datetime.now().isoformat()
            print(f'{{"timestamp":"{ts}","dag":"{dag_id}","action":"fetch","status_code":{resp.status_code}}}')
        except Exception as exc:  # noqa: BLE001
            sys.stderr.write(f"[{dag_id}] Failed to fetch source for {task_id}: {exc}\n")
            return None

        content_type = resp.headers.get("Content-Type", "")
        if "application/json" in content_type:
            try:
                payload = resp.json()
            except ValueError:
                payload = {}
            text = payload.get("content") or resp.text
        else:
            text = resp.text

        with file_cache_lock:
            file_cache[file_token] = text
        return text, None

    # Fallback 1: use DAG-level file_token (same code file for all tasks)
    dag_file_token: Optional[str] = None
    with dag_file_token_cache_lock:
        dag_file_token = dag_file_token_cache.get(dag_id)
    if dag_file_token is None:
        try:
            dag_file_token = get_dag_file_token(session, base_url, dag_id) or ""
        except Exception as exc:  # noqa: BLE001
            sys.stderr.write(f"[{dag_id}] Failed to get DAG file_token: {exc}\n")
            dag_file_token = ""
        with dag_file_token_cache_lock:
            dag_file_token_cache[dag_id] = dag_file_token

    if dag_file_token:
        with file_cache_lock:
            if dag_file_token in file_cache:
                return file_cache[dag_file_token], f"dag_file_token:{dag_file_token}"
        try:
            resp = api_get(session, base_url, f"/api/v1/dagSources/{dag_file_token}", params=None)
            ts = datetime.now().isoformat()
            print(f'{{"timestamp":"{ts}","dag":"{dag_id}","action":"fetch","status_code":{resp.status_code}}}')
        except Exception as exc:  # noqa: BLE001
            sys.stderr.write(f"[{dag_id}] Failed to fetch DAG source via file_token: {exc}\n")
            resp = None
        if resp:
            content_type = resp.headers.get("Content-Type", "")
            if "application/json" in content_type:
                try:
                    payload = resp.json()
                except ValueError:
                    payload = {}
                text = payload.get("content") or resp.text
            else:
                text = resp.text

            with file_cache_lock:
                file_cache[dag_file_token] = text
            return text, f"dag_file_token:{dag_file_token}"

    # Fallback 2: fetch full DAG source via dagSource endpoint
    dag_src = fetch_dag_source(
        session,
        base_url,
        dag_id,
        dag_source_cache,
        dag_source_unavailable,
        dag_source_cache_lock,
        dag_source_unavailable_lock,
    )
    if dag_src:
        return dag_src, f"dag_source:{dag_id}"

    with dag_source_unavailable_lock:
        unavailable = dag_id in dag_source_unavailable
    if unavailable:
        return None

    hint = " (dagSource unavailable)"
    sys.stderr.write(
        f"[{dag_id}] No file_token for task {task_id}; skipping code fetch{hint}.\n"
    )
    return None


def save_log(text: str, output_dir: Path, dag_id: str, dag_run_id: str, task_id: str) -> Path:
    dag_dir = output_dir / sanitize_name(dag_id) / sanitize_name(dag_run_id)
    dag_dir.mkdir(parents=True, exist_ok=True)
    path = dag_dir / f"{sanitize_name(task_id)}.json"
    path.write_text(text)
    return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Airflow logs and/or code for DAGs."
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Airflow host (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Airflow port (default: 8080)",
    )
    parser.add_argument(
        "--tls",
        action="store_true",
        help="Use HTTPS instead of HTTP when contacting Airflow",
    )
    parser.add_argument(
        "--username",
        default="airflow",
        help="Airflow username (default: airflow)",
    )
    parser.add_argument(
        "--password",
        default="airflow",
        help="Airflow password (default: airflow)",
    )
    parser.add_argument(
        "--source",
        action="store_true",
        help="Fetch DAG source code into code/<dag>/source.py",
    )
    parser.add_argument(
        "--logs",
        action="store_true",
        help="Fetch logs for the latest DAG run (per DAG).",
    )
    parser.add_argument(
        "-c",
        "--concurrency",
        type=int,
        default=50,
        help="Number of worker threads for fetching logs/code (default: %(default)s)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.source and not args.logs:
        sys.stderr.write("Specify at least one of --source or --logs.\n")
        return 1

    scheme = "https" if args.tls else "http"
    base_url = f"{scheme}://{args.host}:{args.port}"
    username = args.username
    password = args.password
    default_output = None

    logs_enabled = bool(args.logs)
    code_enabled = bool(args.source)

    session = build_session(username, password)

    try:
        dag_ids = list(list_dags(session, base_url))
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"Failed to list DAGs: {exc}\n")
        return 1
    if not dag_ids:
        sys.stderr.write("No DAGs found.\n")
        return 1

    output_dir = Path(default_output) if default_output else Path(__file__).resolve().parent / "logs"
    total_saved_logs = 0
    total_saved_code = 0
    file_cache: Dict[str, str] = {}
    dag_source_cache: Dict[str, str] = {}
    dag_source_unavailable: set[str] = set()
    dag_file_token_cache: Dict[str, str] = {}
    shared_code_written: Dict[str, str] = {}
    file_cache_lock = threading.Lock()
    dag_source_cache_lock = threading.Lock()
    dag_source_unavailable_lock = threading.Lock()
    dag_file_token_cache_lock = threading.Lock()
    shared_code_written_lock = threading.Lock()
    max_workers = max(1, args.concurrency)
    executor_logs: Optional[concurrent.futures.ThreadPoolExecutor] = (
        concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) if logs_enabled else None
    )
    futures_logs: list[concurrent.futures.Future[Optional[str]]] = []
    futures_logs_lock = threading.Lock()
    executor_code: Optional[concurrent.futures.ThreadPoolExecutor] = (
        concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) if code_enabled else None
    )
    futures_code: list[concurrent.futures.Future[Optional[str]]] = []
    futures_code_lock = threading.Lock()

    dag_workers = max(1, min(len(dag_ids), max_workers))
    with concurrent.futures.ThreadPoolExecutor(max_workers=dag_workers) as dag_executor:
        def _process_dag(dag_id: str) -> None:
            task_ids_for_code: Optional[list[str]] = None

            if logs_enabled and executor_logs:
                try:
                    dag_runs = list_successful_dag_runs(session, base_url, dag_id, limit=10)
                except Exception as exc:  # noqa: BLE001
                    sys.stderr.write(f"[{dag_id}] Failed to list successful DAG runs: {exc}\n")
                    dag_runs = []

                if not dag_runs:
                    sys.stderr.write(f"[{dag_id}] No successful DAG runs found.\n")

                for dag_run in dag_runs:
                    dag_run_id = dag_run.get("dag_run_id") or dag_run.get("run_id")
                    if not dag_run_id:
                        continue
                    try:
                        task_instances = list(
                            get_task_instances(session, base_url, dag_id, dag_run_id)
                        )
                    except Exception as exc:  # noqa: BLE001
                        sys.stderr.write(f"[{dag_id}] Failed to list task instances for {dag_run_id}: {exc}\n")
                        task_instances = []

                    if task_instances:
                        def _log_worker(dag_id_local: str, dag_run_id_local: str, ti: Dict[str, Any]) -> Optional[str]:
                            task_id_local = ti.get("task_id") or "unknown_task"
                            try_number_local = pick_try_number(ti)
                            try:
                                log_text_local = fetch_log_text(
                                    session,
                                    base_url,
                                    dag_id_local,
                                    dag_run_id_local,
                                    task_id_local,
                                    try_number_local,
                                )
                                path_local = save_log(
                                    log_text_local, output_dir, dag_id_local, dag_run_id_local, task_id_local
                                )
                                ts_log = datetime.now().isoformat()
                                rel_path_log = path_local
                                try:
                                    rel_path_log = str(
                                        Path(path_local).relative_to(Path(__file__).resolve().parent)
                                    )
                                except ValueError:
                                    rel_path_log = path_local
                                print(
                                    f'{{"timestamp":"{ts_log}","dag":"{dag_id_local}","dag_run_id":"{dag_run_id_local}","path":"{rel_path_log}","action":"save_log"}}'
                                )
                                return str(path_local)
                            except Exception as exc:  # noqa: BLE001
                                sys.stderr.write(f"[{dag_id_local}] Failed to fetch log for {task_id_local} (run {dag_run_id_local}): {exc}\n")
                                return None

                        for ti in task_instances:
                            with futures_logs_lock:
                                futures_logs.append(executor_logs.submit(_log_worker, dag_id, dag_run_id, ti))
                    # Reuse last task ids for code if present (from most recent run in the loop)
                    task_ids_for_code = [ti.get("task_id") or "unknown_task" for ti in task_instances]

            if code_enabled and executor_code:
                if task_ids_for_code is None:
                    try:
                        task_ids_for_code = list(list_tasks(session, base_url, dag_id))
                    except Exception as exc:  # noqa: BLE001
                        sys.stderr.write(f"[{dag_id}] Failed to list tasks for code fetch: {exc}\n")
                        task_ids_for_code = []

                def _code_worker(dag_id_local: str, task_id_local: str) -> Optional[str]:
                    try:
                        code_res = fetch_task_code(
                            session,
                            base_url,
                            dag_id_local,
                            task_id_local,
                            file_cache,
                            dag_source_cache,
                            dag_source_unavailable,
                            dag_file_token_cache,
                            file_cache_lock,
                            dag_source_cache_lock,
                            dag_source_unavailable_lock,
                            dag_file_token_cache_lock,
                        )
                    except Exception as exc:  # noqa: BLE001
                        sys.stderr.write(f"[{dag_id_local}] Failed to fetch code for {task_id_local}: {exc}\n")
                        return None
                    if code_res is None:
                        return None
                    code_text, _shared_key = code_res
                    ts = datetime.now().isoformat()
                    rel_path = f"code/{sanitize_name(dag_id_local)}/source.py"
                    with shared_code_written_lock:
                        already_written = shared_code_written.get(dag_id_local, False)
                    if already_written:
                        print(
                            f'{{"timestamp":"{ts}","dag":"{dag_id_local}","path":"{rel_path}","action":"save"}}'
                        )
                        return None

                    # Save single code file per DAG: code/<dag>/source.py
                    code_path = save_code(code_text, output_dir, dag_id_local, "source")
                    with shared_code_written_lock:
                        shared_code_written[dag_id_local] = True
                    print(
                        f'{{"timestamp":"{ts}","dag":"{dag_id_local}","path":"{rel_path}","action":"save"}}'
                    )
                    return str(code_path)

                for tid in (task_ids_for_code or []):
                    with futures_code_lock:
                        futures_code.append(executor_code.submit(_code_worker, dag_id, tid))

        for dag_id in dag_ids:
            dag_executor.submit(_process_dag, dag_id)

    if executor_logs:
        for fut in concurrent.futures.as_completed(futures_logs):
            path_res = fut.result()
            if path_res:
                total_saved_logs += 1
        executor_logs.shutdown(wait=True)

    if executor_code:
        for fut in concurrent.futures.as_completed(futures_code):
            code_path_res = fut.result()
            if code_path_res:
                total_saved_code += 1
        executor_code.shutdown(wait=True)

    return 0 if (total_saved_logs or total_saved_code) else 1


if __name__ == "__main__":
    raise SystemExit(main())

