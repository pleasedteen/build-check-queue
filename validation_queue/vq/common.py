"""公共工具：SQLite 连接、HTTP 小服务基类、HTTP 客户端、日志。

仅依赖 Python 标准库，可在 ``python3 -I`` 隔离模式下运行。
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from http.server import HTTPServer


def now_ms() -> int:
    """当前 Unix 时间（毫秒）。"""
    return int(time.time() * 1000)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(name)s] %(levelname)s %(message)s",
                              datefmt="%H:%M:%S")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def connect_sqlite(path: str) -> sqlite3.Connection:
    """打开 SQLite 连接并开启 WAL，适合多进程/多线程访问。

    check_same_thread=False：允许投递线程与 HTTP 工作线程共用连接，
    所有写操作由调用方加锁并在事务内完成，保证线程安全。
    """
    conn = sqlite3.connect(path, timeout=30, isolation_level=None,
                           check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


class HttpError(Exception):
    """HTTP 错误，携带状态码与消息。"""

    def __init__(self, status: int, message: str):
        super().__init__(f"HTTP {status}: {message}")
        self.status = status
        self.message = message


def http_request(url: str, payload: dict | None = None, method: str = "GET",
                 timeout: float = 10.0):
    """发起 JSON HTTP 请求，返回解析后的 JSON（无响应体时返回 None）。"""
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            if not body:
                return None
            return json.loads(body.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(detail)
            detail = parsed.get("error", detail)
        except (ValueError, TypeError):
            pass
        raise HttpError(exc.code, detail) from None


class _ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class Handler(BaseHTTPRequestHandler):
    """轻量 JSON 路由处理器，子类通过 routes 分发。"""

    server_version = "VQueue/1.0"

    def log_message(self, fmt, *args):  # 降低默认访问日志噪音
        logging.getLogger(self.server.log_name).debug(fmt, *args)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise HttpError(400, "请求体不是合法 JSON")
        if not isinstance(data, dict):
            raise HttpError(400, "请求体必须是 JSON 对象")
        return data

    def _send_json(self, status: int, obj) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle(self, method: str) -> None:
        try:
            handler = self.server.routes.get((method, self.path.split("?")[0]))
            if handler is None:
                raise HttpError(404, f"没有 {method} {self.path} 路由")
            payload = self._read_json() if method in ("POST", "PUT") else {}
            result = handler(payload, self)
            if result is None:
                result = {"ok": True}
            status, body = result if isinstance(result, tuple) else (200, result)
            self._send_json(status, body)
        except HttpError as exc:
            self._send_json(exc.status, {"error": exc.message})
        except Exception as exc:  # 服务端兜底，避免连接直接断开
            logging.getLogger(self.server.log_name).exception("处理请求失败")
            self._send_json(500, {"error": f"内部错误: {exc}"})

    def do_GET(self):
        self._handle("GET")

    def do_POST(self):
        self._handle("POST")


def serve(host: str, port: int, routes: dict, log_name: str,
          on_start=None) -> _ThreadingHTTPServer:
    """构造并启动 HTTP 服务（在当前线程内运行）。"""
    httpd = _ThreadingHTTPServer((host, port), Handler)
    httpd.routes = routes
    httpd.log_name = log_name
    logger = get_logger(log_name)
    if on_start is not None:
        on_start(httpd)
    host_name, actual_port = httpd.server_address[:2]
    logger.info("HTTP 服务监听 http://%s:%s", host_name, actual_port)
    try:
        httpd.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        pass
    finally:
        httpd.shutdown()
        httpd.server_close()
        logger.info("HTTP 服务已停止")
    return httpd
