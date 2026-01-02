#!/usr/bin/env python3
"""
Simple Redis key lister (unauth by default, optional password).

Usage:
  python3 scripts/redis_list_keys.py -H 1.2.3.4 -P 6379 [-a PASSWORD]

Behavior:
  - Connects to Redis over TCP.
  - If password is provided, sends AUTH.
  - Iterates all keys via SCAN 0 (cursor) to avoid blocking.
  - Prints keys to stdout as UTF-8 (fallback repr on decode errors).
"""

import argparse
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple, Union, List

CONNECT_TIMEOUT = 5
READ_TIMEOUT = 30
SCAN_COUNT = 1000
SAMPLE_COUNT = 10  # max sample items for hash/set/zset/list
MIN_WORKERS = 50
DEFAULT_MAX_DBS = 16


def _encode_command(*args: str) -> bytes:
    parts = [f"*{len(args)}\r\n".encode()]
    for arg in args:
        arg_bytes = arg.encode("utf-8")
        parts.append(f"${len(arg_bytes)}\r\n".encode())
        parts.append(arg_bytes)
        parts.append(b"\r\n")
    return b"".join(parts)


def _read_line(sock: socket.socket) -> Optional[bytes]:
    chunks = []
    while True:
        b = sock.recv(1)
        if not b:
            return None
        if b == b"\r":
            nxt = sock.recv(1)
            if nxt == b"\n":
                return b"".join(chunks)
            chunks.append(b)
            chunks.append(nxt)
        else:
            chunks.append(b)


def _read_resp(sock: socket.socket) -> Tuple[str, Optional[Union[bytes, int, list]]]:
    first = sock.recv(1)
    if not first:
        return "unknown", None

    if first == b"+":
        line = _read_line(sock)
        return "simple", line
    if first == b"-":
        line = _read_line(sock)
        return "error", line
    if first == b":":
        line = _read_line(sock)
        try:
            return "integer", int(line.decode("utf-8", errors="ignore")) if line else None
        except Exception:
            return "unknown", None
    if first == b"$":
        len_line = _read_line(sock)
        if len_line is None:
            return "unknown", None
        try:
            length = int(len_line.decode("utf-8", errors="ignore"))
        except Exception:
            return "unknown", None
        if length == -1:
            return "bulk", None
        data = b""
        remaining = length
        while remaining > 0:
            chunk = sock.recv(remaining)
            if not chunk:
                return "unknown", None
            data += chunk
            remaining -= len(chunk)
        sock.recv(2)  # CRLF
        return "bulk", data
    if first == b"*":
        len_line = _read_line(sock)
        if len_line is None:
            return "unknown", None
        try:
            count = int(len_line.decode("utf-8", errors="ignore"))
        except Exception:
            return "unknown", None
        if count == -1:
            return "array", None
        arr = []
        for _ in range(count):
            t, v = _read_resp(sock)
            arr.append(v)
        return "array", arr

    return "unknown", None


def _safe_decode(data: bytes, limit: int) -> Tuple[str, bool]:
    truncated = False
    if len(data) > limit:
        data = data[:limit]
        truncated = True
    try:
        return data.decode("utf-8"), truncated
    except Exception:
        return repr(data), truncated


def _connect(host: str, port: int, password: Optional[str], db: Optional[int] = None) -> socket.socket:
    sock = socket.create_connection((host, port), timeout=CONNECT_TIMEOUT)
    sock.settimeout(READ_TIMEOUT)
    if password:
        sock.sendall(_encode_command("AUTH", password))
        tt, tv = _read_resp(sock)
        if tt == "error":
            msg = tv.decode("utf-8", errors="ignore") if isinstance(tv, (bytes, bytearray)) else repr(tv)
            raise RuntimeError(f"AUTH failed: {msg}")
    if db is not None and db != 0:
        sock.sendall(_encode_command("SELECT", str(db)))
        tt, tv = _read_resp(sock)
        if tt == "error":
            msg = tv.decode("utf-8", errors="ignore") if isinstance(tv, (bytes, bytearray)) else repr(tv)
            raise RuntimeError(f"SELECT {db} failed: {msg}")
    return sock


def _fetch_key_value(
    key: bytes,
    host: str,
    port: int,
    password: Optional[str],
    max_value_bytes: int,
    sample_count: int,
    db: int,
) -> str:
    key_str = key.decode("utf-8", errors="ignore")
    try:
        sock = _connect(host, port, password, db=db)
    except Exception as exc:
        return f"db{db}:{key_str} = <conn error: {exc}>"

    try:
        sock.sendall(_encode_command("TYPE", key_str))
        tt, tv = _read_resp(sock)
        key_type = (tv or b"unknown").decode("utf-8", errors="ignore") if isinstance(tv, (bytes, bytearray)) else "unknown"

        if key_type == "string":
            sock.sendall(_encode_command("GET", key_str))
            vt, vv = _read_resp(sock)
            if vt == "bulk" and isinstance(vv, (bytes, bytearray)):
                val_str, trunc = _safe_decode(bytes(vv), max_value_bytes)
                if trunc:
                    val_str += f" ... [truncated to {max_value_bytes} bytes]"
                return f"db{db}:{key_str} = {val_str}"
            if vt == "bulk" and vv is None:
                return f"db{db}:{key_str} = (nil)"
            msg = vv.decode("utf-8", errors="ignore") if isinstance(vv, bytes) else repr(vv)
            return f"db{db}:{key_str} = <string unreadable> {msg}"

        if key_type == "hash":
            sock.sendall(_encode_command("HSCAN", key_str, "0", "COUNT", str(sample_count)))
            ht, hv = _read_resp(sock)
            fields = []
            if ht == "array" and isinstance(hv, list) and len(hv) == 2:
                values = hv[1] or []
                if isinstance(values, list):
                    for i in range(0, len(values), 2):
                        if i + 1 >= len(values):
                            break
                        fname = values[i]
                        fval = values[i + 1]
                        fname_str = fname.decode("utf-8", errors="ignore") if isinstance(fname, (bytes, bytearray)) else repr(fname)
                        if isinstance(fval, (bytes, bytearray)):
                            fval_str, trunc = _safe_decode(bytes(fval), max_value_bytes)
                            if trunc:
                                fval_str += f" ... [truncated to {max_value_bytes} bytes]"
                        else:
                            fval_str = repr(fval)
                        fields.append(f"{fname_str}={fval_str}")
                        if len(fields) >= sample_count:
                            break
            return f"db{db}:{key_str} = <hash> " + (", ".join(fields) if fields else "(empty)")

        if key_type == "list":
            sock.sendall(_encode_command("LRANGE", key_str, "0", str(sample_count - 1)))
            lt, lv = _read_resp(sock)
            items: List[str] = []
            if lt == "array" and isinstance(lv, list):
                for item in lv[:sample_count]:
                    if isinstance(item, (bytes, bytearray)):
                        item_str, trunc = _safe_decode(bytes(item), max_value_bytes)
                        if trunc:
                            item_str += f" ... [truncated to {max_value_bytes} bytes]"
                    else:
                        item_str = repr(item)
                    items.append(item_str)
            return f"db{db}:{key_str} = <list> " + (", ".join(items) if items else "(empty)")

        if key_type == "set":
            sock.sendall(_encode_command("SSCAN", key_str, "0", "COUNT", str(sample_count)))
            st, sv = _read_resp(sock)
            members: List[str] = []
            if st == "array" and isinstance(sv, list) and len(sv) == 2:
                vals = sv[1] or []
                if isinstance(vals, list):
                    for item in vals[:sample_count]:
                        if isinstance(item, (bytes, bytearray)):
                            item_str, trunc = _safe_decode(bytes(item), max_value_bytes)
                            if trunc:
                                item_str += f" ... [truncated to {max_value_bytes} bytes]"
                        else:
                            item_str = repr(item)
                        members.append(item_str)
            return f"db{db}:{key_str} = <set> " + (", ".join(members) if members else "(empty)")

        if key_type == "zset":
            sock.sendall(_encode_command("ZSCAN", key_str, "0", "COUNT", str(sample_count)))
            zt, zv = _read_resp(sock)
            members: List[str] = []
            if zt == "array" and isinstance(zv, list) and len(zv) == 2:
                vals = zv[1] or []
                if isinstance(vals, list):
                    for i in range(0, len(vals), 2):
                        if i + 1 >= len(vals):
                            break
                        mem = vals[i]
                        score = vals[i + 1]
                        mem_str = mem.decode("utf-8", errors="ignore") if isinstance(mem, (bytes, bytearray)) else repr(mem)
                        score_str = score.decode("utf-8", errors="ignore") if isinstance(score, (bytes, bytearray)) else repr(score)
                        members.append(f"{mem_str}({score_str})")
                        if len(members) >= sample_count:
                            break
            return f"db{db}:{key_str} = <zset> " + (", ".join(members) if members else "(empty)")

        return f"db{db}:{key_str} = <{key_type}>"
    except Exception as exc:
        return f"db{db}:{key_str} = <error {exc}>"
    finally:
        try:
            sock.close()
        except Exception:
            pass


def list_keys(
    host: str,
    port: int,
    password: Optional[str],
    workers: int,
    db: Optional[int],
    max_dbs: int,
    max_value_bytes: int,
) -> int:
    addr = (host, port)
    try:
        # Determine DBs to scan
        if db is not None:
            dbs_to_scan = [db]
        else:
            dbs_to_scan = list(range(max_dbs))

        all_keys: list[tuple[int, bytes]] = []

        # Step 1: SCAN per DB (serially, one connection reused per db)
        for db_idx in dbs_to_scan:
            try:
                sock = _connect(host, port, password, db=db_idx if db_idx != 0 else None)
            except Exception as exc:
                print(f"db{db_idx}: <conn/select error: {exc}>")
                continue
            with sock:
                cursor = "0"
                while True:
                    sock.sendall(_encode_command("SCAN", cursor, "COUNT", str(SCAN_COUNT)))
                    t, v = _read_resp(sock)
                    if t == "error":
                        msg = v.decode("utf-8", errors="ignore") if isinstance(v, (bytes, bytearray)) else repr(v)
                        print(f"db{db_idx}: SCAN error: {msg}")
                        break
                    if t == "bulk" and isinstance(v, (bytes, bytearray)):
                        # Treat bulk response as a single key (non-standard SCAN response)
                        all_keys.append((db_idx, bytes(v)))
                        cursor = "0"
                        break
                    if t != "array" or not isinstance(v, list):
                        print(f"db{db_idx}: Unexpected SCAN response type: {t}")
                        break
                    if len(v) != 2:
                        print(f"db{db_idx}: Unexpected SCAN response length: {len(v)}")
                        break
                    cursor_raw = v[0]
                    batch_keys = v[1] or []
                    if not isinstance(batch_keys, list):
                        print(f"db{db_idx}: Unexpected SCAN keys type: {type(batch_keys)}")
                        break
                    cursor = (cursor_raw or b"0").decode("utf-8", errors="ignore")
                    for k in batch_keys:
                        if k is None:
                            continue
                        all_keys.append((db_idx, k))
                    if cursor == "0":
                        break

        total = len(all_keys)
        worker_count = max(MIN_WORKERS, workers, 1)
        sample_count = SAMPLE_COUNT
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [
                executor.submit(_fetch_key_value, k, host, port, password, max_value_bytes, sample_count, db_idx)
                for db_idx, k in all_keys
            ]
            for fut in as_completed(futures):
                try:
                    print(fut.result())
                except Exception as exc:
                    print(f"<worker error: {exc}>")

        print(f"-- total keys: {total}")
        return 0
    except Exception as exc:
        print(f"Connection/error: {exc}")
        return 1


def main():
    parser = argparse.ArgumentParser(
        description="List all Redis keys and values (GET), unauth by default.\n"
        "You can pass host/port positionally: python3 redis_list_keys.py 1.2.3.4 6379\n"
        "or with flags: -H 1.2.3.4 -P 6379\n"
        "(short -h is the host alias; use --help for help).",
        formatter_class=argparse.RawTextHelpFormatter,
        add_help=False,
    )
    parser.add_argument("pos_host", nargs="?", help="Redis host (positional)")
    parser.add_argument("pos_port", nargs="?", type=int, help="Redis port (positional, default 6379)")
    parser.add_argument("-h", "-H", "--host", dest="opt_host", help="Redis host")
    parser.add_argument("-P", "--port", dest="opt_port", type=int, help="Redis port (default 6379)")
    parser.add_argument("-a", "--auth", help="Redis password (optional)")
    parser.add_argument(
        "--workers",
        type=int,
        default=MIN_WORKERS,
        help=f"Number of concurrent workers (min {MIN_WORKERS}, default {MIN_WORKERS})",
    )
    parser.add_argument(
        "-d",
        "--db",
        type=int,
        help="Scan only this DB index. If not set, scan all DBs 0..15.",
    )
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=1024 * 1024,
        help="Max bytes to read per value (truncates longer values). Default 1048576.",
    )
    parser.add_argument("--help", action="help", help="Show this help message and exit")
    args = parser.parse_args()

    host = args.pos_host or args.opt_host
    port = args.pos_port or args.opt_port or 6379

    if not host:
        parser.error("host is required (positional or -H/--host)")

    exit_code = list_keys(
        host,
        port,
        args.auth,
        args.workers,
        args.db,
        DEFAULT_MAX_DBS,
        args.max_bytes,
    )
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()

