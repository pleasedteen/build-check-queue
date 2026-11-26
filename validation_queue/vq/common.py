"""公共工具：SQLite 连接、HTTP 小服务基类、HTTP 客户端、日志。

仅依赖 Python 标准库，可在 ``python3 -I`` 隔离模式下运行。
"""
from __future__ import annotations

import http.client
import json
import logging
import socket
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from urllib.parse import urlsplit


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
    """发起 JSON HTTP 请求，返回解析后的 JSON（无响应体时返回 None）。

    ``timeout`` 是整次请求（连接 + 发送 + 等待响应 + 读完响应体）的硬上限，
    而不是单次 socket 操作的超时。为避免服务端慢速零星返回数据时读超时被
    不断重置、请求无限拖长，这里叠加一层看门狗：到点直接 shutdown 套接字，
    阻塞中的读写立即失败。
    """
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise ValueError(f"非法 URL: {url}")
    deadline = time.monotonic() + timeout
    path = parts.path or "/"
    if parts.query:
        path += "?" + parts.query

    if parts.scheme == "https":
        conn = http.client.HTTPSConnection(parts.hostname, parts.port,
                                           timeout=timeout)
    else:
        conn = http.client.HTTPConnection(parts.hostname, parts.port,
                                          timeout=timeout)
    expired = threading.Event()

    def _watchdog() -> None:
        # 到点未结束：标记超时并断开套接字，解除阻塞的 connect/send/recv
        expired.set()
        sock = conn.sock
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    timer = threading.Timer(timeout, _watchdog)
    timer.daemon = True
    timer.start()
    try:
        conn.request(method, path, body=data, headers=headers)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("等待响应超过整次请求时限")
        if conn.sock is not None:
            conn.sock.settimeout(remaining)
        resp = conn.getresponse()
        body = _read_body_with_deadline(conn, resp, deadline)
        # 看门狗到点关闭套接字后，读取可能表现为连接断开/空响应：
        # 必须按超时失败处理，绝不能被当成“收妥成功”。
        if expired.is_set():
            raise TimeoutError("整次请求超过时限")
        if resp.status >= 400:
            detail = body.decode("utf-8", errors="replace")
            try:
                parsed = json.loads(detail)
                detail = parsed.get("error", detail)
            except (ValueError, TypeError):
                pass
            raise HttpError(resp.status, detail)
        if not body:
            return None
        return json.loads(body.decode("utf-8"))
    except (TimeoutError, OSError) as exc:
        if expired.is_set() or isinstance(exc, TimeoutError):
            raise HttpError(504, f"请求超时（>{timeout:.1f}s）: {url}") from None
        raise
    finally:
        timer.cancel()
        conn.close()


def _read_body_with_deadline(conn, resp, deadline: float) -> bytes:
    """在整次请求的截止时间前读完响应体；超时按失败处理。

    每次读之前把 socket 超时收紧到“剩余时间”，服务端零星返回数据时不会
    因为每次 read 都重置超时而无限拖长。
    """
    chunks: list[bytes] = []
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("响应体读取超过整次请求时限")
        if conn.sock is not None:
            conn.sock.settimeout(remaining)
        # read1 有多少返回多少，不会为凑满缓冲区而一直等待：
        # 服务端零星/缓慢返回数据时也能在下一轮检查截止时间并及时结束。
        chunk = resp.read1(65536)
        if not chunk:
            if not resp.isclosed() and resp.length:
                # 响应体还没读完就读到 EOF：连接被中断，按失败处理
                raise TimeoutError("响应体未读完连接即中断")
            return b"".join(chunks)
        chunks.append(chunk)


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
