#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "rich",
#   "websockets",
#   "aiohttp",
#   "aiofiles",
#   "watchdog",
#   "psutil",
# ]
# ///

"""
AI Bridge Server v1.0
=====================
A local bridge server that connects your browser AI chat to your filesystem.

Features:
- JSON-based protocol for file operations (read, write, search, replace, patch)
- Execute scripts with stdout/stderr/exit code streaming to browser
- Auto-install dependencies in isolated environments
- Live TUI showing all operations, open files, replacements
- Annotation support (you annotate files, AI sees annotations)
- Automatic test-until-pass loop
- WebSocket for real-time bidirectional communication

Usage:
    uv run bridge.py [--port 8765] [--allowed-dirs ./project]

Or install once:
    uv run bridge.py --install

Protocol:
    All messages are JSON. See PROTOCOL.md for full spec.
"""

import os
import sys
import json
import time
import uuid
import shutil
import signal
import hashlib
import asyncio
import argparse
import tempfile
import traceback
import subprocess
import threading
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass, field, asdict
from collections import deque
from enum import Enum

# ════════════════════════════════════════════════════════════════════════════
# AUTO-INSTALL / UV BOOTSTRAP
# ════════════════════════════════════════════════════════════════════════════

def compute_exclude_newer_date(days_back=8):
    from datetime import timedelta
    return (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")

def should_set_exclude_newer():
    return not os.environ.get("UV_EXCLUDE_NEWER")

def restart_with_uv(script_path, args_list, env):
    try:
        os.execvpe("uv", ["uv", "run", "--quiet", script_path] + args_list, env)
    except FileNotFoundError:
        print("uv is not installed. Install it with:")
        print("  curl -LsSf https://astral.sh/uv/install.sh | sh")
        sys.exit(1)

def ensure_safe_env():
    if not should_set_exclude_newer():
        return
    past_date = compute_exclude_newer_date(8)
    os.environ["UV_EXCLUDE_NEWER"] = past_date
    restart_with_uv(sys.argv[0], sys.argv[1:], os.environ)

ensure_safe_env()

# ════════════════════════════════════════════════════════════════════════════
# NOW SAFE TO IMPORT HEAVY DEPS
# ════════════════════════════════════════════════════════════════════════════

import aiohttp
import aiohttp.web
import aiofiles
import websockets
import psutil
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.live import Live
from rich.text import Text
from rich.syntax import Syntax
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.columns import Columns
from rich.tree import Tree
from rich import box
from rich.markup import escape

console = Console()

# ════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════

DEFAULT_PORT = 8765
DEFAULT_HTTP_PORT = 8766
MAX_LOG_ENTRIES = 500
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
HEARTBEAT_INTERVAL = 10

# ════════════════════════════════════════════════════════════════════════════
# PROTOCOL DEFINITION
# ════════════════════════════════════════════════════════════════════════════

class MessageType(str, Enum):
    # Client -> Server
    FILE_READ = "file_read"
    FILE_WRITE = "file_write"
    FILE_PATCH = "file_patch"
    FILE_CREATE = "file_create"
    FILE_DELETE = "file_delete"
    FILE_LIST = "file_list"
    FILE_SEARCH = "file_search"
    FILE_REPLACE = "file_replace"
    FILE_MULTI_REPLACE = "file_multi_replace"
    FILE_ANNOTATE = "file_annotate"
    FILE_GET_ANNOTATIONS = "file_get_annotations"

    EXEC_RUN = "exec_run"
    EXEC_KILL = "exec_kill"
    EXEC_STATUS = "exec_status"

    ENV_INSTALL = "env_install"
    ENV_STATUS = "env_status"

    TEST_RUN = "test_run"
    TEST_LOOP = "test_loop"
    TEST_STOP = "test_stop"

    PING = "ping"
    STATUS = "status"
    CONFIG_GET = "config_get"
    CONFIG_SET = "config_set"

    # Server -> Client
    RESPONSE = "response"
    STREAM_STDOUT = "stream_stdout"
    STREAM_STDERR = "stream_stderr"
    STREAM_EXIT = "stream_exit"
    FILE_CHANGED = "file_changed"
    ERROR = "error"
    LOG = "log"
    TEST_RESULT = "test_result"
    TEST_ITERATION = "test_iteration"

# ════════════════════════════════════════════════════════════════════════════
# DATA MODELS
# ════════════════════════════════════════════════════════════════════════════

@dataclass
class FileAnnotation:
    file_path: str
    line_start: int
    line_end: int
    text: str
    author: str = "user"
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    annotation_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])

@dataclass
class ExecutionResult:
    exec_id: str
    command: str
    exit_code: Optional[int] = None
    stdout: str = ""
    stderr: str = ""
    start_time: float = 0.0
    end_time: float = 0.0
    duration: float = 0.0
    status: str = "running"  # running, completed, killed, error

@dataclass
class TestIteration:
    iteration: int
    exit_code: int
    stdout: str
    stderr: str
    duration: float
    passed: bool
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

@dataclass
class FileOperation:
    op_type: str
    file_path: str
    timestamp: str
    details: str = ""
    success: bool = True

# ════════════════════════════════════════════════════════════════════════════
# FILE WATCHER
# ════════════════════════════════════════════════════════════════════════════

class BridgeFileWatcher(FileSystemEventHandler):
    def __init__(self, bridge: 'BridgeServer'):
        self.bridge = bridge
        self._debounce: Dict[str, float] = {}
        self._debounce_interval = 0.5

    def on_modified(self, event):
        if event.is_directory:
            return
        self._handle_change(event.src_path, "modified")

    def on_created(self, event):
        if event.is_directory:
            return
        self._handle_change(event.src_path, "created")

    def on_deleted(self, event):
        if event.is_directory:
            return
        self._handle_change(event.src_path, "deleted")

    def _handle_change(self, path: str, change_type: str):
        now = time.time()
        if path in self._debounce and now - self._debounce[path] < self._debounce_interval:
            return
        self._debounce[path] = now

        asyncio.run_coroutine_threadsafe(
            self.bridge.notify_file_change(path, change_type),
            self.bridge.loop
        )

# ════════════════════════════════════════════════════════════════════════════
# BRIDGE SERVER
# ════════════════════════════════════════════════════════════════════════════

class BridgeServer:
    def __init__(self, port: int = DEFAULT_PORT, http_port: int = DEFAULT_HTTP_PORT,
                 allowed_dirs: Optional[List[str]] = None, auto_approve: bool = False):
        self.port = port
        self.http_port = http_port
        self.allowed_dirs: List[Path] = []
        if allowed_dirs:
            for d in allowed_dirs:
                p = Path(d).resolve()
                if p.exists():
                    self.allowed_dirs.append(p)
                else:
                    console.print(f"[yellow]Warning: Directory does not exist: {d}[/]")

        if not self.allowed_dirs:
            self.allowed_dirs.append(Path.cwd().resolve())

        self.auto_approve = auto_approve
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.clients: Dict[str, websockets.WebSocketServerProtocol] = {}
        self.annotations: Dict[str, List[FileAnnotation]] = {}
        self.executions: Dict[str, ExecutionResult] = {}
        self.processes: Dict[str, asyncio.subprocess.Process] = {}
        self.file_operations: deque = deque(maxlen=MAX_LOG_ENTRIES)
        self.log_entries: deque = deque(maxlen=MAX_LOG_ENTRIES)
        self.open_files: Dict[str, str] = {}  # path -> content hash
        self.test_running: bool = False
        self.test_stop_flag: bool = False
        self.test_iterations: List[TestIteration] = []
        self.config: Dict[str, Any] = {
            "max_test_iterations": 50,
            "test_timeout": 120,
            "exec_timeout": 300,
            "auto_install": True,
            "isolated_env": True,
            "env_dir": ".bridge_env",
        }

        # File watcher
        self.observer = Observer()
        self.file_handler = BridgeFileWatcher(self)
        for d in self.allowed_dirs:
            self.observer.schedule(self.file_handler, str(d), recursive=True)

        # TUI state
        self._tui_running = False
        self._last_activity = ""
        self._connected_clients = 0
        self._total_ops = 0
        self._start_time = time.time()

    # ════════════════════════════════════════════════════════════════════
    # SECURITY: PATH VALIDATION
    # ════════════════════════════════════════════════════════════════════

    def _validate_path(self, file_path: str) -> Tuple[bool, Path]:
        """Validate that a path is within allowed directories."""
        try:
            resolved = Path(file_path).resolve()
            for allowed in self.allowed_dirs:
                if resolved == allowed or allowed in resolved.parents:
                    return True, resolved
            return False, resolved
        except Exception:
            return False, Path(file_path)

    def _is_path_allowed(self, file_path: str) -> bool:
        allowed, _ = self._validate_path(file_path)
        return allowed

    # ════════════════════════════════════════════════════════════════════
    # LOGGING
    # ════════════════════════════════════════════════════════════════════

    def _log(self, level: str, message: str, details: str = ""):
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "message": message,
            "details": details,
        }
        self.log_entries.append(entry)
        self._last_activity = f"[{level.upper()}] {message}"

    def _log_op(self, op_type: str, file_path: str, details: str = "", success: bool = True):
        op = FileOperation(
            op_type=op_type,
            file_path=file_path,
            timestamp=datetime.now(timezone.utc).isoformat(),
            details=details,
            success=success,
        )
        self.file_operations.append(op)
        self._total_ops += 1

    # ════════════════════════════════════════════════════════════════════
    # MESSAGE HANDLING
    # ════════════════════════════════════════════════════════════════════

    async def handle_message(self, websocket, raw_message: str) -> Optional[dict]:
        """Parse and route incoming JSON messages."""
        try:
            msg = json.loads(raw_message)
        except json.JSONDecodeError as e:
            return self._error_response("invalid_json", f"Invalid JSON: {e}")

        msg_type = msg.get("type")
        msg_id = msg.get("id", str(uuid.uuid4())[:8])
        payload = msg.get("payload", {})

        self._log("info", f"Received: {msg_type}", json.dumps(payload)[:200])

        handler_map = {
            MessageType.FILE_READ: self._handle_file_read,
            MessageType.FILE_WRITE: self._handle_file_write,
            MessageType.FILE_PATCH: self._handle_file_patch,
            MessageType.FILE_CREATE: self._handle_file_create,
            MessageType.FILE_DELETE: self._handle_file_delete,
            MessageType.FILE_LIST: self._handle_file_list,
            MessageType.FILE_SEARCH: self._handle_file_search,
            MessageType.FILE_REPLACE: self._handle_file_replace,
            MessageType.FILE_MULTI_REPLACE: self._handle_file_multi_replace,
            MessageType.FILE_ANNOTATE: self._handle_file_annotate,
            MessageType.FILE_GET_ANNOTATIONS: self._handle_file_get_annotations,
            MessageType.EXEC_RUN: self._handle_exec_run,
            MessageType.EXEC_KILL: self._handle_exec_kill,
            MessageType.EXEC_STATUS: self._handle_exec_status,
            MessageType.ENV_INSTALL: self._handle_env_install,
            MessageType.ENV_STATUS: self._handle_env_status,
            MessageType.TEST_RUN: self._handle_test_run,
            MessageType.TEST_LOOP: self._handle_test_loop,
            MessageType.TEST_STOP: self._handle_test_stop,
            MessageType.PING: self._handle_ping,
            MessageType.STATUS: self._handle_status,
            MessageType.CONFIG_GET: self._handle_config_get,
            MessageType.CONFIG_SET: self._handle_config_set,
        }

        handler = handler_map.get(msg_type)
        if handler is None:
            return self._error_response("unknown_type", f"Unknown message type: {msg_type}", msg_id)

        try:
            result = await handler(payload, websocket)
            if result is not None:
                result["id"] = msg_id
                result["type"] = MessageType.RESPONSE
            return result
        except Exception as e:
            self._log("error", f"Handler error: {msg_type}", traceback.format_exc())
            return self._error_response("handler_error", str(e), msg_id)

    def _error_response(self, code: str, message: str, msg_id: str = "") -> dict:
        return {
            "type": MessageType.ERROR,
            "id": msg_id,
            "payload": {
                "code": code,
                "message": message,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        }

    def _success_response(self, data: Any = None) -> dict:
        return {
            "payload": {
                "success": True,
                "data": data,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        }

    # ════════════════════════════════════════════════════════════════════
    # FILE HANDLERS
    # ════════════════════════════════════════════════════════════════════

    async def _handle_file_read(self, payload: dict, ws) -> dict:
        file_path = payload.get("path", "")
        if not file_path:
            return self._error_response("missing_path", "No file path specified")

        if not self._is_path_allowed(file_path):
            return self._error_response("access_denied",
                f"Path not in allowed directories: {file_path}")

        try:
            resolved = Path(file_path).resolve()
            if not resolved.exists():
                return self._error_response("not_found", f"File not found: {file_path}")

            if resolved.stat().st_size > MAX_FILE_SIZE:
                return self._error_response("too_large",
                    f"File too large (>{MAX_FILE_SIZE} bytes)")

            async with aiofiles.open(str(resolved), 'r', encoding='utf-8', errors='replace') as f:
                content = await f.read()

            content_hash = hashlib.sha256(content.encode()).hexdigest()[:16]
            self.open_files[str(resolved)] = content_hash

            # Include annotations if any
            annotations = self.annotations.get(str(resolved), [])

            self._log_op("read", file_path)
            return self._success_response({
                "path": str(resolved),
                "content": content,
                "hash": content_hash,
                "size": len(content),
                "lines": content.count('\n') + 1,
                "annotations": [asdict(a) for a in annotations],
            })
        except Exception as e:
            self._log_op("read", file_path, str(e), success=False)
            return self._error_response("read_error", str(e))

    async def _handle_file_write(self, payload: dict, ws) -> dict:
        file_path = payload.get("path", "")
        content = payload.get("content", "")
        create_dirs = payload.get("create_dirs", True)

        if not file_path:
            return self._error_response("missing_path", "No file path specified")
        if not self._is_path_allowed(file_path):
            return self._error_response("access_denied",
                f"Path not in allowed directories: {file_path}")

        try:
            resolved = Path(file_path).resolve()
            if create_dirs:
                resolved.parent.mkdir(parents=True, exist_ok=True)

            async with aiofiles.open(str(resolved), 'w', encoding='utf-8') as f:
                await f.write(content)

            content_hash = hashlib.sha256(content.encode()).hexdigest()[:16]
            self.open_files[str(resolved)] = content_hash

            self._log_op("write", file_path, f"{len(content)} bytes")
            return self._success_response({
                "path": str(resolved),
                "hash": content_hash,
                "size": len(content),
                "lines": content.count('\n') + 1,
            })
        except Exception as e:
            self._log_op("write", file_path, str(e), success=False)
            return self._error_response("write_error", str(e))

    async def _handle_file_patch(self, payload: dict, ws) -> dict:
        """Apply a patch (list of operations) to a file."""
        file_path = payload.get("path", "")
        operations = payload.get("operations", [])

        if not file_path:
            return self._error_response("missing_path", "No file path specified")
        if not self._is_path_allowed(file_path):
            return self._error_response("access_denied", f"Path not allowed: {file_path}")

        try:
            resolved = Path(file_path).resolve()
            if not resolved.exists():
                return self._error_response("not_found", f"File not found: {file_path}")

            async with aiofiles.open(str(resolved), 'r', encoding='utf-8') as f:
                content = await f.read()

            lines = content.split('\n')
            applied = []

            # Sort operations by line number descending to avoid offset issues
            sorted_ops = sorted(operations, key=lambda o: o.get("line_start", 0), reverse=True)

            for op in sorted_ops:
                op_type = op.get("op", "replace")
                line_start = op.get("line_start", 1) - 1  # 1-indexed to 0-indexed
                line_end = op.get("line_end", line_start + 1) - 1
                new_content = op.get("content", "")

                if op_type == "replace":
                    new_lines = new_content.split('\n') if new_content else []
                    lines[line_start:line_end + 1] = new_lines
                    applied.append(f"replace L{line_start+1}-{line_end+1}")
                elif op_type == "insert":
                    new_lines = new_content.split('\n')
                    lines[line_start:line_start] = new_lines
                    applied.append(f"insert at L{line_start+1}")
                elif op_type == "delete":
                    del lines[line_start:line_end + 1]
                    applied.append(f"delete L{line_start+1}-{line_end+1}")

            new_content = '\n'.join(lines)
            async with aiofiles.open(str(resolved), 'w', encoding='utf-8') as f:
                await f.write(new_content)

            content_hash = hashlib.sha256(new_content.encode()).hexdigest()[:16]
            self.open_files[str(resolved)] = content_hash

            self._log_op("patch", file_path, f"{len(applied)} ops: {', '.join(applied)}")
            return self._success_response({
                "path": str(resolved),
                "hash": content_hash,
                "operations_applied": applied,
                "new_line_count": len(lines),
            })
        except Exception as e:
            self._log_op("patch", file_path, str(e), success=False)
            return self._error_response("patch_error", str(e))

    async def _handle_file_create(self, payload: dict, ws) -> dict:
        file_path = payload.get("path", "")
        content = payload.get("content", "")

        if not file_path:
            return self._error_response("missing_path", "No file path specified")
        if not self._is_path_allowed(file_path):
            return self._error_response("access_denied", f"Path not allowed: {file_path}")

        try:
            resolved = Path(file_path).resolve()
            if resolved.exists():
                return self._error_response("already_exists", f"File already exists: {file_path}")

            resolved.parent.mkdir(parents=True, exist_ok=True)
            async with aiofiles.open(str(resolved), 'w', encoding='utf-8') as f:
                await f.write(content)

            self._log_op("create", file_path, f"{len(content)} bytes")
            return self._success_response({"path": str(resolved), "created": True})
        except Exception as e:
            self._log_op("create", file_path, str(e), success=False)
            return self._error_response("create_error", str(e))

    async def _handle_file_delete(self, payload: dict, ws) -> dict:
        file_path = payload.get("path", "")

        if not file_path:
            return self._error_response("missing_path", "No file path specified")
        if not self._is_path_allowed(file_path):
            return self._error_response("access_denied", f"Path not allowed: {file_path}")

        try:
            resolved = Path(file_path).resolve()
            if not resolved.exists():
                return self._error_response("not_found", f"File not found: {file_path}")

            resolved.unlink()
            self.open_files.pop(str(resolved), None)

            self._log_op("delete", file_path)
            return self._success_response({"path": str(resolved), "deleted": True})
        except Exception as e:
            self._log_op("delete", file_path, str(e), success=False)
            return self._error_response("delete_error", str(e))

    async def _handle_file_list(self, payload: dict, ws) -> dict:
        dir_path = payload.get("path", ".")
        pattern = payload.get("pattern", "*")
        recursive = payload.get("recursive", True)

        if not self._is_path_allowed(dir_path):
            return self._error_response("access_denied", f"Path not allowed: {dir_path}")

        try:
            resolved = Path(dir_path).resolve()
            if not resolved.exists():
                return self._error_response("not_found", f"Directory not found: {dir_path}")

            files = []
            glob_method = resolved.rglob if recursive else resolved.glob
            for p in glob_method(pattern):
                if p.is_file():
                    try:
                        stat = p.stat()
                        files.append({
                            "path": str(p),
                            "relative": str(p.relative_to(resolved)),
                            "size": stat.st_size,
                            "modified": stat.st_mtime,
                        })
                    except (OSError, PermissionError):
                        continue

            files.sort(key=lambda f: f["relative"])
            return self._success_response({
                "directory": str(resolved),
                "files": files[:1000],  # Limit
                "total": len(files),
                "truncated": len(files) > 1000,
            })
        except Exception as e:
            return self._error_response("list_error", str(e))

    async def _handle_file_search(self, payload: dict, ws) -> dict:
        """Search for text/regex across files."""
        query = payload.get("query", "")
        dir_path = payload.get("path", ".")
        pattern = payload.get("file_pattern", "*")
        is_regex = payload.get("regex", False)
        case_sensitive = payload.get("case_sensitive", True)
        max_results = payload.get("max_results", 100)

        if not query:
            return self._error_response("missing_query", "No search query specified")
        if not self._is_path_allowed(dir_path):
            return self._error_response("access_denied", f"Path not allowed: {dir_path}")

        try:
            resolved = Path(dir_path).resolve()
            results = []

            flags = 0 if case_sensitive else re.IGNORECASE
            if is_regex:
                compiled = re.compile(query, flags)
            else:
                escaped = re.escape(query)
                compiled = re.compile(escaped, flags)

            for p in resolved.rglob(pattern):
                if not p.is_file() or p.stat().st_size > MAX_FILE_SIZE:
                    continue
                try:
                    with open(p, 'r', encoding='utf-8', errors='replace') as f:
                        for line_num, line in enumerate(f, 1):
                            matches = list(compiled.finditer(line))
                            if matches:
                                for m in matches:
                                    results.append({
                                        "file": str(p),
                                        "relative": str(p.relative_to(resolved)),
                                        "line": line_num,
                                        "column": m.start() + 1,
                                        "match": m.group(),
                                        "context": line.rstrip(),
                                    })
                                    if len(results) >= max_results:
                                        break
                        if len(results) >= max_results:
                            break
                except (OSError, PermissionError, UnicodeDecodeError):
                    continue
                if len(results) >= max_results:
                    break

            self._log_op("search", dir_path, f"query='{query}' found={len(results)}")
            return self._success_response({
                "query": query,
                "results": results,
                "total": len(results),
                "truncated": len(results) >= max_results,
            })
        except re.error as e:
            return self._error_response("regex_error", f"Invalid regex: {e}")
        except Exception as e:
            return self._error_response("search_error", str(e))

    async def _handle_file_replace(self, payload: dict, ws) -> dict:
        """Search and replace in a single file."""
        file_path = payload.get("path", "")
        search = payload.get("search", "")
        replace = payload.get("replace", "")
        is_regex = payload.get("regex", False)
        case_sensitive = payload.get("case_sensitive", True)
        max_replacements = payload.get("max_replacements", 0)  # 0 = all

        if not file_path or not search:
            return self._error_response("missing_params", "Need path and search")
        if not self._is_path_allowed(file_path):
            return self._error_response("access_denied", f"Path not allowed: {file_path}")

        try:
            resolved = Path(file_path).resolve()
            if not resolved.exists():
                return self._error_response("not_found", f"File not found: {file_path}")

            async with aiofiles.open(str(resolved), 'r', encoding='utf-8') as f:
                content = await f.read()

            flags = 0 if case_sensitive else re.IGNORECASE
            if is_regex:
                compiled = re.compile(search, flags)
            else:
                compiled = re.compile(re.escape(search), flags)

            count = max_replacements if max_replacements > 0 else 0
            if count > 0:
                new_content, n = compiled.subn(replace, content, count=count)
            else:
                new_content, n = compiled.subn(replace, content)

            if n > 0:
                async with aiofiles.open(str(resolved), 'w', encoding='utf-8') as f:
                    await f.write(new_content)

                content_hash = hashlib.sha256(new_content.encode()).hexdigest()[:16]
                self.open_files[str(resolved)] = content_hash

            self._log_op("replace", file_path, f"'{search}' -> '{replace}' ({n} replacements)")
            return self._success_response({
                "path": str(resolved),
                "replacements": n,
                "hash": content_hash if n > 0 else hashlib.sha256(content.encode()).hexdigest()[:16],
            })
        except Exception as e:
            self._log_op("replace", file_path, str(e), success=False)
            return self._error_response("replace_error", str(e))

    async def _handle_file_multi_replace(self, payload: dict, ws) -> dict:
        """Multiple search-and-replace operations across multiple files."""
        operations = payload.get("operations", [])
        if not operations:
            return self._error_response("missing_operations", "No operations specified")

        results = []
        for op in operations:
            file_path = op.get("path", "")
            search = op.get("search", "")
            replace = op.get("replace", "")
            is_regex = op.get("regex", False)
            case_sensitive = op.get("case_sensitive", True)

            if not file_path or not search:
                results.append({"path": file_path, "success": False, "error": "missing params"})
                continue
            if not self._is_path_allowed(file_path):
                results.append({"path": file_path, "success": False, "error": "access denied"})
                continue

            try:
                resolved = Path(file_path).resolve()
                if not resolved.exists():
                    results.append({"path": file_path, "success": False, "error": "not found"})
                    continue

                async with aiofiles.open(str(resolved), 'r', encoding='utf-8') as f:
                    content = await f.read()

                flags = 0 if case_sensitive else re.IGNORECASE
                if is_regex:
                    compiled = re.compile(search, flags)
                else:
                    compiled = re.compile(re.escape(search), flags)

                new_content, n = compiled.subn(replace, content)

                if n > 0:
                    async with aiofiles.open(str(resolved), 'w', encoding='utf-8') as f:
                        await f.write(new_content)
                    content_hash = hashlib.sha256(new_content.encode()).hexdigest()[:16]
                    self.open_files[str(resolved)] = content_hash

                results.append({
                    "path": str(resolved),
                    "success": True,
                    "replacements": n,
                })
                self._log_op("multi_replace", file_path, f"{n} replacements")
            except Exception as e:
                results.append({"path": file_path, "success": False, "error": str(e)})

        total_replacements = sum(r.get("replacements", 0) for r in results if r.get("success"))
        return self._success_response({
            "results": results,
            "total_replacements": total_replacements,
            "files_modified": sum(1 for r in results if r.get("success") and r.get("replacements", 0) > 0),
        })

    async def _handle_file_annotate(self, payload: dict, ws) -> dict:
        """Add an annotation to a file."""
        file_path = payload.get("path", "")
        line_start = payload.get("line_start", 1)
        line_end = payload.get("line_end", line_start)
        text = payload.get("text", "")
        author = payload.get("author", "user")

        if not file_path or not text:
            return self._error_response("missing_params", "Need path and text")
        if not self._is_path_allowed(file_path):
            return self._error_response("access_denied", f"Path not allowed: {file_path}")

        resolved = Path(file_path).resolve()
        key = str(resolved)

        annotation = FileAnnotation(
            file_path=key,
            line_start=line_start,
            line_end=line_end,
            text=text,
            author=author,
        )

        if key not in self.annotations:
            self.annotations[key] = []
        self.annotations[key].append(annotation)

        self._log_op("annotate", file_path, f"L{line_start}-{line_end}: {text[:50]}")
        return self._success_response({
            "annotation_id": annotation.annotation_id,
            "total_annotations": len(self.annotations[key]),
        })

    async def _handle_file_get_annotations(self, payload: dict, ws) -> dict:
        """Get all annotations for a file."""
        file_path = payload.get("path", "")
        if not file_path:
            return self._error_response("missing_path", "No file path specified")

        resolved = Path(file_path).resolve()
        key = str(resolved)
        annotations = self.annotations.get(key, [])

        return self._success_response({
            "path": key,
            "annotations": [asdict(a) for a in annotations],
            "count": len(annotations),
        })

    # ════════════════════════════════════════════════════════════════════
    # EXECUTION HANDLERS
    # ════════════════════════════════════════════════════════════════════

    async def _handle_exec_run(self, payload: dict, ws) -> dict:
        """Execute a command and stream output."""
        command = payload.get("command", "")
        cwd = payload.get("cwd", str(self.allowed_dirs[0]))
        env_vars = payload.get("env", {})
        timeout = payload.get("timeout", self.config["exec_timeout"])
        shell = payload.get("shell", True)

        if not command:
            return self._error_response("missing_command", "No command specified")

        # Validate cwd
        if not self._is_path_allowed(cwd):
            return self._error_response("access_denied", f"CWD not allowed: {cwd}")

        exec_id = str(uuid.uuid4())[:12]
        result = ExecutionResult(
            exec_id=exec_id,
            command=command,
            start_time=time.time(),
            status="running",
        )
        self.executions[exec_id] = result

        self._log("info", f"Exec [{exec_id}]: {command[:100]}")

        # Start execution in background
        asyncio.create_task(self._run_process(exec_id, command, cwd, env_vars, timeout, shell, ws))

        return self._success_response({
            "exec_id": exec_id,
            "status": "started",
            "command": command,
        })

    async def _run_process(self, exec_id: str, command: str, cwd: str,
                           env_vars: dict, timeout: int, shell: bool,
                           ws):
        """Run a process and stream output to the websocket."""
        result = self.executions[exec_id]
        env = os.environ.copy()
        env.update(env_vars)

        try:
            if shell:
                proc = await asyncio.create_subprocess_shell(
                    command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=cwd,
                    env=env,
                )
            else:
                args = command.split() if isinstance(command, str) else command
                proc = await asyncio.create_subprocess_exec(
                    *args,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=cwd,
                    env=env,
                )

            self.processes[exec_id] = proc

            async def stream_output(stream, stream_type):
                while True:
                    line = await stream.readline()
                    if not line:
                        break
                    decoded = line.decode('utf-8', errors='replace')
                    if stream_type == "stdout":
                        result.stdout += decoded
                    else:
                        result.stderr += decoded

                    # Send to websocket
                    msg = {
                        "type": stream_type == "stdout" and MessageType.STREAM_STDOUT or MessageType.STREAM_STDERR,
                        "payload": {
                            "exec_id": exec_id,
                            "data": decoded,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }
                    }
                    try:
                        await ws.send(json.dumps(msg))
                    except Exception:
                        pass

            # Stream both stdout and stderr concurrently
            stdout_task = asyncio.create_task(stream_output(proc.stdout, "stdout"))
            stderr_task = asyncio.create_task(stream_output(proc.stderr, "stderr"))

            try:
                await asyncio.wait_for(
                    asyncio.gather(stdout_task, stderr_task),
                    timeout=timeout
                )
                await proc.wait()
                result.exit_code = proc.returncode
                result.status = "completed"
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                result.exit_code = -1
                result.status = "timeout"
                result.stderr += f"\n[BRIDGE] Process killed after {timeout}s timeout\n"

        except Exception as e:
            result.status = "error"
            result.exit_code = -1
            result.stderr += f"\n[BRIDGE] Error: {str(e)}\n"

        result.end_time = time.time()
        result.duration = result.end_time - result.start_time
        self.processes.pop(exec_id, None)

        # Send exit message
        exit_msg = {
            "type": MessageType.STREAM_EXIT,
            "payload": {
                "exec_id": exec_id,
                "exit_code": result.exit_code,
                "duration": round(result.duration, 2),
                "status": result.status,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        }
        try:
            await ws.send(json.dumps(exit_msg))
        except Exception:
            pass

        self._log("info", f"Exec [{exec_id}] finished: exit={result.exit_code} duration={result.duration:.1f}s")

    async def _handle_exec_kill(self, payload: dict, ws) -> dict:
        """Kill a running process."""
        exec_id = payload.get("exec_id", "")
        if not exec_id:
            return self._error_response("missing_exec_id", "No exec_id specified")

        proc = self.processes.get(exec_id)
        if not proc:
            return self._error_response("not_found", f"Process not found: {exec_id}")

        try:
            proc.kill()
            result = self.executions.get(exec_id)
            if result:
                result.status = "killed"
            return self._success_response({"exec_id": exec_id, "killed": True})
        except Exception as e:
            return self._error_response("kill_error", str(e))

    async def _handle_exec_status(self, payload: dict, ws) -> dict:
        """Get status of an execution."""
        exec_id = payload.get("exec_id", "")
        if exec_id:
            result = self.executions.get(exec_id)
            if not result:
                return self._error_response("not_found", f"Execution not found: {exec_id}")
            return self._success_response({
                "exec_id": exec_id,
                "command": result.command,
                "status": result.status,
                "exit_code": result.exit_code,
                "duration": result.duration,
                "stdout_length": len(result.stdout),
                "stderr_length": len(result.stderr),
            })
        else:
            # Return all executions
            execs = []
            for eid, r in self.executions.items():
                execs.append({
                    "exec_id": eid,
                    "command": r.command[:80],
                    "status": r.status,
                    "exit_code": r.exit_code,
                    "duration": round(r.duration, 2),
                })
            return self._success_response({"executions": execs})

    # ════════════════════════════════════════════════════════════════════
    # ENVIRONMENT HANDLERS
    # ════════════════════════════════════════════════════════════════════

    async def _handle_env_install(self, payload: dict, ws) -> dict:
        """Install dependencies in an isolated environment."""
        dependencies = payload.get("dependencies", [])
        language = payload.get("language", "python")
        env_name = payload.get("env_name", self.config["env_dir"])
        cwd = payload.get("cwd", str(self.allowed_dirs[0]))
        auto_approve = payload.get("auto_approve", self.auto_approve)

        if not dependencies:
            return self._error_response("missing_deps", "No dependencies specified")

        if not auto_approve:
            # Send approval request to client
            approval_msg = {
                "type": MessageType.LOG,
                "payload": {
                    "level": "approval_required",
                    "message": f"Install {len(dependencies)} dependencies?",
                    "details": json.dumps(dependencies),
                    "action": "env_install",
                }
            }
            try:
                await ws.send(json.dumps(approval_msg))
            except Exception:
                pass
            # For now, proceed (in real implementation, wait for approval)

        env_path = Path(cwd) / env_name
        exec_id = str(uuid.uuid4())[:12]

        if language == "python":
            # Use uv to create venv and install
            commands = []
            if not env_path.exists():
                commands.append(f"uv venv {env_path}")

            pip_cmd = f"uv pip install --python {env_path}/bin/python " + " ".join(
                f'"{d}"' for d in dependencies
            )
            commands.append(pip_cmd)

            full_command = " && ".join(commands)
        elif language == "node":
            full_command = "npm install " + " ".join(dependencies)
        elif language == "rust":
            full_command = "cargo add " + " ".join(dependencies)
        else:
            full_command = f"echo 'Unsupported language: {language}'"

        # Execute installation
        result = ExecutionResult(
            exec_id=exec_id,
            command=full_command,
            start_time=time.time(),
            status="running",
        )
        self.executions[exec_id] = result

        asyncio.create_task(self._run_process(exec_id, full_command, cwd, {}, 120, True, ws))

        self._log("info", f"Installing deps [{language}]: {dependencies}")
        return self._success_response({
            "exec_id": exec_id,
            "env_path": str(env_path),
            "dependencies": dependencies,
            "language": language,
            "command": full_command,
        })

    async def _handle_env_status(self, payload: dict, ws) -> dict:
        """Check environment status."""
        cwd = payload.get("cwd", str(self.allowed_dirs[0]))
        env_name = payload.get("env_name", self.config["env_dir"])

        env_path = Path(cwd) / env_name
        exists = env_path.exists()

        info = {
            "env_path": str(env_path),
            "exists": exists,
        }

        if exists and (env_path / "bin" / "python").exists():
            # Get installed packages
            try:
                proc = await asyncio.create_subprocess_exec(
                    str(env_path / "bin" / "python"), "-m", "pip", "list", "--format=json",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, _ = await proc.communicate()
                packages = json.loads(stdout.decode())
                info["packages"] = packages
                info["package_count"] = len(packages)
            except Exception:
                info["packages"] = []
                info["package_count"] = 0

        return self._success_response(info)

    # ════════════════════════════════════════════════════════════════════
    # TEST HANDLERS
    # ════════════════════════════════════════════════════════════════════

    async def _handle_test_run(self, payload: dict, ws) -> dict:
        """Run a test command once."""
        command = payload.get("command", "")
        cwd = payload.get("cwd", str(self.allowed_dirs[0]))
        timeout = payload.get("timeout", self.config["test_timeout"])

        if not command:
            return self._error_response("missing_command", "No test command specified")

        # Just run it as a normal exec
        payload["timeout"] = timeout
        return await self._handle_exec_run(payload, ws)

    async def _handle_test_loop(self, payload: dict, ws) -> dict:
        """Run tests in a loop until they pass or max iterations reached."""
        command = payload.get("command", "")
        cwd = payload.get("cwd", str(self.allowed_dirs[0]))
        max_iterations = payload.get("max_iterations", self.config["max_test_iterations"])
        timeout = payload.get("timeout", self.config["test_timeout"])
        fix_command = payload.get("fix_command", "")  # Command to run to fix issues

        if not command:
            return self._error_response("missing_command", "No test command specified")

        if self.test_running:
            return self._error_response("already_running", "Test loop already running")

        self.test_running = True
        self.test_stop_flag = False
        self.test_iterations = []

        # Start test loop in background
        asyncio.create_task(self._test_loop(command, cwd, max_iterations, timeout, fix_command, ws))

        return self._success_response({
            "status": "started",
            "max_iterations": max_iterations,
            "command": command,
        })

    async def _test_loop(self, command: str, cwd: str, max_iterations: int,
                         timeout: int, fix_command: str, ws):
        """Background test loop."""
        for i in range(1, max_iterations + 1):
            if self.test_stop_flag:
                break

            self._log("info", f"Test iteration {i}/{max_iterations}")

            # Run test
            start = time.time()
            try:
                proc = await asyncio.create_subprocess_shell(
                    command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=cwd,
                )
                try:
                    stdout_bytes, stderr_bytes = await asyncio.wait_for(
                        proc.communicate(), timeout=timeout
                    )
                except asyncio.TimeoutError:
                    proc.kill()
                    await proc.wait()
                    stdout_bytes = b""
                    stderr_bytes = f"[BRIDGE] Test timed out after {timeout}s".encode()

                exit_code = proc.returncode or -1
                stdout = stdout_bytes.decode('utf-8', errors='replace')
                stderr = stderr_bytes.decode('utf-8', errors='replace')
            except Exception as e:
                exit_code = -1
                stdout = ""
                stderr = str(e)

            duration = time.time() - start
            passed = exit_code == 0

            iteration = TestIteration(
                iteration=i,
                exit_code=exit_code,
                stdout=stdout,
                stderr=stderr,
                duration=round(duration, 2),
                passed=passed,
            )
            self.test_iterations.append(iteration)

            # Send iteration result to client
            iter_msg = {
                "type": MessageType.TEST_ITERATION,
                "payload": {
                    "iteration": i,
                    "max_iterations": max_iterations,
                    "exit_code": exit_code,
                    "passed": passed,
                    "duration": round(duration, 2),
                    "stdout": stdout[-2000:],  # Last 2000 chars
                    "stderr": stderr[-2000:],
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            }
            try:
                await ws.send(json.dumps(iter_msg))
            except Exception:
                pass

            if passed:
                self._log("info", f"Tests PASSED on iteration {i}!")
                break

            # If there's a fix command, run it
            if fix_command and not self.test_stop_flag:
                self._log("info", f"Running fix command: {fix_command[:80]}")
                try:
                    fix_proc = await asyncio.create_subprocess_shell(
                        fix_command,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                        cwd=cwd,
                    )
                    await asyncio.wait_for(fix_proc.communicate(), timeout=timeout)
                except Exception:
                    pass

            # Small delay between iterations
            await asyncio.sleep(1)

        self.test_running = False

        # Send final result
        final_msg = {
            "type": MessageType.TEST_RESULT,
            "payload": {
                "completed": True,
                "total_iterations": len(self.test_iterations),
                "passed": len(self.test_iterations) > 0 and self.test_iterations[-1].passed,
                "stopped": self.test_stop_flag,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        }
        try:
            await ws.send(json.dumps(final_msg))
        except Exception:
            pass

    async def _handle_test_stop(self, payload: dict, ws) -> dict:
        """Stop the test loop."""
        self.test_stop_flag = True
        return self._success_response({"stopping": True})

    # ════════════════════════════════════════════════════════════════════
    # UTILITY HANDLERS
    # ════════════════════════════════════════════════════════════════════

    async def _handle_ping(self, payload: dict, ws) -> dict:
        return self._success_response({
            "pong": True,
            "server_time": datetime.now(timezone.utc).isoformat(),
            "uptime": round(time.time() - self._start_time, 1),
        })

    async def _handle_status(self, payload: dict, ws) -> dict:
        return self._success_response({
            "version": "1.0.0",
            "uptime": round(time.time() - self._start_time, 1),
            "connected_clients": len(self.clients),
            "allowed_dirs": [str(d) for d in self.allowed_dirs],
            "open_files": list(self.open_files.keys()),
            "active_processes": list(self.processes.keys()),
            "test_running": self.test_running,
            "total_operations": self._total_ops,
            "config": self.config,
            "memory_mb": round(psutil.Process().memory_info().rss / 1024 / 1024, 1),
        })

    async def _handle_config_get(self, payload: dict, ws) -> dict:
        key = payload.get("key")
        if key:
            return self._success_response({key: self.config.get(key)})
        return self._success_response(self.config)

    async def _handle_config_set(self, payload: dict, ws) -> dict:
        key = payload.get("key", "")
        value = payload.get("value")
        if not key:
            return self._error_response("missing_key", "No config key specified")
        if key not in self.config:
            return self._error_response("invalid_key", f"Unknown config key: {key}")
        self.config[key] = value
        self._log("info", f"Config set: {key} = {value}")
        return self._success_response({"key": key, "value": value})

    # ════════════════════════════════════════════════════════════════════
    # FILE CHANGE NOTIFICATION
    # ════════════════════════════════════════════════════════════════════

    async def notify_file_change(self, path: str, change_type: str):
        """Notify all connected clients about a file change."""
        msg = {
            "type": MessageType.FILE_CHANGED,
            "payload": {
                "path": path,
                "change_type": change_type,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        }
        msg_str = json.dumps(msg)
        for client_id, ws in list(self.clients.items()):
            try:
                await ws.send(msg_str)
            except Exception:
                pass

    # ════════════════════════════════════════════════════════════════════
    # WEBSOCKET SERVER
    # ════════════════════════════════════════════════════════════════════

    async def ws_handler(self, websocket):
        """Handle a WebSocket connection."""
        client_id = str(uuid.uuid4())[:8]
        self.clients[client_id] = websocket
        self._connected_clients = len(self.clients)
        self._log("info", f"Client connected: {client_id}")

        try:
            async for message in websocket:
                response = await self.handle_message(websocket, message)
                if response:
                    await websocket.send(json.dumps(response))
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            self._log("error", f"WebSocket error: {e}")
        finally:
            self.clients.pop(client_id, None)
            self._connected_clients = len(self.clients)
            self._log("info", f"Client disconnected: {client_id}")

    # ════════════════════════════════════════════════════════════════════
    # HTTP SERVER (for CORS-friendly health check & static)
    # ════════════════════════════════════════════════════════════════════

    async def http_handler(self, request: aiohttp.web.Request):
        """HTTP endpoint for health checks and info."""
        path = request.path

        # CORS headers
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        }

        if request.method == "OPTIONS":
            return aiohttp.web.Response(status=200, headers=headers)

        if path == "/health" or path == "/":
            data = {
                "status": "ok",
                "version": "1.0.0",
                "ws_port": self.port,
                "http_port": self.http_port,
                "uptime": round(time.time() - self._start_time, 1),
                "clients": len(self.clients),
                "allowed_dirs": [str(d) for d in self.allowed_dirs],
            }
            return aiohttp.web.json_response(data, headers=headers)

        if path == "/status":
            data = {
                "version": "1.0.0",
                "uptime": round(time.time() - self._start_time, 1),
                "connected_clients": len(self.clients),
                "allowed_dirs": [str(d) for d in self.allowed_dirs],
                "open_files": list(self.open_files.keys()),
                "active_processes": list(self.processes.keys()),
                "test_running": self.test_running,
                "total_operations": self._total_ops,
                "recent_ops": [
                    {"type": op.op_type, "file": op.file_path, "time": op.timestamp, "ok": op.success}
                    for op in list(self.file_operations)[-20:]
                ],
                "recent_logs": list(self.log_entries)[-30:],
            }
            return aiohttp.web.json_response(data, headers=headers)

        if path == "/protocol":
            protocol_doc = {
                "name": "AI Bridge Protocol v1.0",
                "transport": "WebSocket",
                "format": "JSON",
                "message_types": {
                    "client_to_server": [t.value for t in MessageType if not t.value.startswith("stream_") and t.value not in ("response", "error", "log", "file_changed", "test_result", "test_iteration")],
                    "server_to_client": [t.value for t in MessageType if t.value.startswith("stream_") or t.value in ("response", "error", "log", "file_changed", "test_result", "test_iteration")],
                },
                "message_format": {
                    "type": "string (MessageType)",
                    "id": "string (unique message ID for request/response correlation)",
                    "payload": "object (type-specific data)",
                },
                "examples": {
                    "file_read": {"type": "file_read", "id": "abc123", "payload": {"path": "./src/main.py"}},
                    "file_write": {"type": "file_write", "id": "def456", "payload": {"path": "./src/main.py", "content": "print('hello')"}},
                    "file_replace": {"type": "file_replace", "id": "ghi789", "payload": {"path": "./src/main.py", "search": "hello", "replace": "world"}},
                    "exec_run": {"type": "exec_run", "id": "jkl012", "payload": {"command": "python main.py", "cwd": "./src"}},
                    "test_loop": {"type": "test_loop", "id": "mno345", "payload": {"command": "pytest", "max_iterations": 10}},
                },
            }
            return aiohttp.web.json_response(protocol_doc, headers=headers)

        return aiohttp.web.json_response({"error": "not found"}, status=404, headers=headers)

    # ════════════════════════════════════════════════════════════════════
    # TUI (Rich Terminal UI)
    # ════════════════════════════════════════════════════════════════════

    def build_tui_layout(self) -> Layout:
        """Build the Rich TUI layout."""
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body"),
            Layout(name="footer", size=3),
        )
        layout["body"].split_row(
            Layout(name="left", ratio=2),
            Layout(name="right", ratio=3),
        )
        layout["left"].split_column(
            Layout(name="status", size=12),
            Layout(name="files"),
        )
        layout["right"].split_column(
            Layout(name="operations", ratio=1),
            Layout(name="logs", ratio=1),
        )
        return layout

    def render_header(self) -> Panel:
        """Render the header panel."""
        uptime = time.time() - self._start_time
        hours = int(uptime // 3600)
        minutes = int((uptime % 3600) // 60)
        seconds = int(uptime % 60)

        grid = Table.grid(expand=True)
        grid.add_column(justify="left", ratio=1)
        grid.add_column(justify="center", ratio=2)
        grid.add_column(justify="right", ratio=1)

        grid.add_row(
            f"[bold cyan]AI Bridge Server v1.0[/]",
            f"[bold white]WebSocket:[/] [green]ws://localhost:{self.port}[/]  |  [bold white]HTTP:[/] [green]http://localhost:{self.http_port}[/]",
            f"[dim]Uptime: {hours:02d}:{minutes:02d}:{seconds:02d}[/]"
        )

        return Panel(grid, style="bold blue", border_style="bright_blue")

    def render_status(self) -> Panel:
        """Render the status panel."""
        table = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
        table.add_column("Key", style="cyan", width=16)
        table.add_column("Value", style="white")

        table.add_row("Clients", f"[{'green' if self._connected_clients > 0 else 'red'}]{self._connected_clients}[/]")
        table.add_row("Operations", f"[yellow]{self._total_ops}[/]")
        table.add_row("Open Files", f"[blue]{len(self.open_files)}[/]")
        table.add_row("Processes", f"[magenta]{len(self.processes)}[/]")
        table.add_row("Test Running", f"[{'green' if self.test_running else 'dim'}]{self.test_running}[/]")
        table.add_row("Annotations", f"[cyan]{sum(len(v) for v in self.annotations.values())}[/]")

        dirs_text = "\n".join(f"  [dim]{d}[/]" for d in self.allowed_dirs)
        table.add_row("Allowed Dirs", dirs_text or "[dim]none[/]")

        mem = psutil.Process().memory_info().rss / 1024 / 1024
        table.add_row("Memory", f"[dim]{mem:.1f} MB[/]")

        return Panel(table, title="[bold]Status[/]", border_style="green", padding=(0, 0))

    def render_files(self) -> Panel:
        """Render the open files panel."""
        tree = Tree("[bold]Open Files[/]")
        if not self.open_files:
            tree.add("[dim]No files open[/]")
        else:
            for path, hash_val in list(self.open_files.items())[-15:]:
                name = Path(path).name
                ann_count = len(self.annotations.get(path, []))
                ann_badge = f" [yellow]({ann_count} ann)[/]" if ann_count > 0 else ""
                tree.add(f"[green]{name}[/] [dim]{hash_val}[/]{ann_badge}")

        return Panel(tree, title="[bold]Files[/]", border_style="blue", padding=(0, 0))

    def render_operations(self) -> Panel:
        """Render recent operations."""
        table = Table(box=box.SIMPLE_HEAD, show_edge=False, padding=(0, 1))
        table.add_column("Time", style="dim", width=8)
        table.add_column("Op", style="cyan", width=12)
        table.add_column("File", style="white", max_width=30)
        table.add_column("Details", style="dim", max_width=40)

        ops = list(self.file_operations)[-15:]
        for op in reversed(ops):
            time_str = op.timestamp.split("T")[1][:8] if "T" in op.timestamp else op.timestamp[-8:]
            status_color = "green" if op.success else "red"
            file_name = Path(op.file_path).name if op.file_path else ""
            table.add_row(
                time_str,
                f"[{status_color}]{op.op_type}[/]",
                file_name,
                (op.details or "")[:40]
            )

        return Panel(table, title="[bold]Recent Operations[/]", border_style="yellow", padding=(0, 0))

    def render_logs(self) -> Panel:
        """Render recent log entries."""
        text = Text()
        logs = list(self.log_entries)[-12:]
        for entry in logs:
            level = entry.get("level", "info")
            msg = entry.get("message", "")
            ts = entry.get("timestamp", "")
            time_str = ts.split("T")[1][:8] if "T" in ts else ""

            color_map = {"info": "white", "error": "red", "warning": "yellow", "debug": "dim"}
            color = color_map.get(level, "white")

            text.append(f"{time_str} ", style="dim")
            text.append(f"[{level.upper()[:3]}] ", style=f"bold {color}")
            text.append(f"{msg}\n", style=color)

        return Panel(text, title="[bold]Logs[/]", border_style="magenta", padding=(0, 0))

    def render_footer(self) -> Panel:
        """Render the footer."""
        activity = self._last_activity or "Waiting for connections..."
        grid = Table.grid(expand=True)
        grid.add_column(justify="left", ratio=3)
        grid.add_column(justify="right", ratio=1)
        grid.add_row(
            f"[dim]{activity}[/]",
            f"[dim]Press Ctrl+C to stop[/]"
        )
        return Panel(grid, style="dim", border_style="dim")

    def render_tui(self) -> Layout:
        """Render the full TUI."""
        layout = self.build_tui_layout()
        layout["header"].update(self.render_header())
        layout["status"].update(self.render_status())
        layout["files"].update(self.render_files())
        layout["operations"].update(self.render_operations())
        layout["logs"].update(self.render_logs())
        layout["footer"].update(self.render_footer())
        return layout

    # ════════════════════════════════════════════════════════════════════
    # MAIN RUN
    # ════════════════════════════════════════════════════════════════════

    async def run(self):
        """Start the bridge server."""
        self.loop = asyncio.get_event_loop()

        # Start file watcher
        self.observer.start()
        self._log("info", "File watcher started", str(self.allowed_dirs))

        # Start WebSocket server
        ws_server = await websockets.serve(
            self.ws_handler,
            "localhost",
            self.port,
            ping_interval=HEARTBEAT_INTERVAL,
            ping_timeout=HEARTBEAT_INTERVAL * 3,
            max_size=50 * 1024 * 1024,  # 50MB max message
        )
        self._log("info", f"WebSocket server started on ws://localhost:{self.port}")

        # Start HTTP server
        app = aiohttp.web.Application()
        app.router.add_route("*", "/{path:.*}", self.http_handler)
        runner = aiohttp.web.AppRunner(app)
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, "localhost", self.http_port)
        await site.start()
        self._log("info", f"HTTP server started on http://localhost:{self.http_port}")

        # Print startup banner
        console.print()
        console.print(Panel.fit(
            f"[bold green]AI Bridge Server Started![/]\n\n"
            f"  [cyan]WebSocket:[/] ws://localhost:{self.port}\n"
            f"  [cyan]HTTP:[/]      http://localhost:{self.http_port}\n"
            f"  [cyan]Protocol:[/]  http://localhost:{self.http_port}/protocol\n"
            f"  [cyan]Status:[/]    http://localhost:{self.http_port}/status\n\n"
            f"  [yellow]Allowed Dirs:[/]\n" +
            "\n".join(f"    [dim]{d}[/]" for d in self.allowed_dirs) + "\n\n"
            f"  [dim]Waiting for browser connections...[/]",
            title="[bold]AI Bridge[/]",
            border_style="bright_blue",
        ))
        console.print()

        # Run TUI
        self._tui_running = True
        try:
            with Live(self.render_tui(), refresh_per_second=2, console=console, screen=True) as live:
                while self._tui_running:
                    await asyncio.sleep(0.5)
                    live.update(self.render_tui())
        except KeyboardInterrupt:
            pass
        finally:
            self._tui_running = False
            self.observer.stop()
            self.observer.join()
            ws_server.close()
            await ws_server.wait_closed()
            await runner.cleanup()
            console.print("[bold red]Bridge server stopped.[/]")

# ════════════════════════════════════════════════════════════════════════════
# CLI ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(
        description="AI Bridge Server - Connect your browser AI to your filesystem",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run bridge.py
  uv run bridge.py --port 9000 --allowed-dirs ./myproject ./other
  uv run bridge.py --auto-approve
  uv run bridge.py --install
        """
    )
    parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                        help=f"WebSocket port (default: {DEFAULT_PORT})")
    parser.add_argument("--http-port", type=int, default=DEFAULT_HTTP_PORT,
                        help=f"HTTP port (default: {DEFAULT_HTTP_PORT})")
    parser.add_argument("--allowed-dirs", nargs="+", default=None,
                        help="Directories the bridge can access (default: current dir)")
    parser.add_argument("--auto-approve", action="store_true",
                        help="Auto-approve dependency installations")
    parser.add_argument("--install", action="store_true",
                        help="Install bridge as a system service / create launcher")
    parser.add_argument("--no-tui", action="store_true",
                        help="Disable TUI (just log to stdout)")
    return parser.parse_args()

def install_bridge():
    """Create a launcher script and desktop entry."""
    script_path = Path(__file__).resolve()
    launcher_dir = Path.home() / ".local" / "bin"
    launcher_dir.mkdir(parents=True, exist_ok=True)

    launcher_path = launcher_dir / "ai-bridge"
    launcher_content = f"""#!/bin/bash
# AI Bridge Server Launcher
# Auto-generated by bridge.py --install
exec uv run "{script_path}" "$@"
"""
    launcher_path.write_text(launcher_content)
    launcher_path.chmod(0o755)

    console.print(f"[green]Installed launcher:[/] {launcher_path}")
    console.print(f"[dim]Run with:[/] ai-bridge --port 8765 --allowed-dirs ./myproject")

    # Create desktop entry (Linux)
    desktop_dir = Path.home() / ".local" / "share" / "applications"
    desktop_dir.mkdir(parents=True, exist_ok=True)
    desktop_path = desktop_dir / "ai-bridge.desktop"
    desktop_content = f"""[Desktop Entry]
Name=AI Bridge Server
Comment=Local bridge server for AI-controlled file editing
Exec=uv run "{script_path}"
Terminal=true
Type=Application
Categories=Development;
"""
    desktop_path.write_text(desktop_content)
    console.print(f"[green]Desktop entry:[/] {desktop_path}")

def main():
    args = parse_args()

    if args.install:
        install_bridge()
        return

    server = BridgeServer(
        port=args.port,
        http_port=args.http_port,
        allowed_dirs=args.allowed_dirs,
        auto_approve=args.auto_approve,
    )

    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        console.print("\n[bold red]Shutting down...[/]")
