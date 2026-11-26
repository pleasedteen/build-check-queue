"""校验队列服务（SQLite 持久化）。

职责：
- 接收面向指定收件端的校验任务；
- 让多个独立工作进程通过租约（lease）抢占任务；
- 只接受持有当前有效租约的进程交回的结果；
- 后台把已完成结果持续投递到对应收件服务，停机恢复后自动续投；
- 按收件端配置“未收妥任务数上限”，满额时暂停该端任务的 *首次领取*
  （到期换手不受额度限制，提交也始终可用）；
- 投递并发受全局上限约束，每次请求有整次限时；挂起/超时但尚未结束的
  尝试仍占并发名额。同一收件端同时最多一次投递，保证单个故障端的积压
  不会占满并发、阻塞其他收件端。

进程崩溃后数据仍在 SQLite 中，重启即可继续，已完成任务不会重新执行。
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse

# -I 隔离模式下脚本目录不在 sys.path，显式加入以导入同目录公共模块
sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (Handler, HttpError, _ThreadingHTTPServer, connect_sqlite,
                    get_logger, http_request, now_ms)

LOG = get_logger("queue")

# 任务状态机：
# queued    已提交，等待首次领取
# leased    已被某个工作进程凭租约领走
# succeeded 结果已被队列接受（租约校验通过），等待/正在投递
# delivered 收件端已持久收妥，名额释放
VALID_STATES = ("queued", "leased", "succeeded", "delivered")

SCHEMA = """
CREATE TABLE IF NOT EXISTS recipients (
    id           TEXT PRIMARY KEY,
    base_url     TEXT NOT NULL,
    max_inflight INTEGER NOT NULL CHECK (max_inflight >= 0),
    created_at   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS tasks (
    id                 TEXT PRIMARY KEY,
    recipient          TEXT NOT NULL REFERENCES recipients(id),
    artifact           TEXT NOT NULL,
    state              TEXT NOT NULL DEFAULT 'queued',
    lease_token        TEXT,
    leased_at          INTEGER,
    lease_expires_at   INTEGER,
    attempt_count      INTEGER NOT NULL DEFAULT 0,
    result             TEXT,
    completed_at       INTEGER,
    delivered_at       INTEGER,
    delivery_attempts  INTEGER NOT NULL DEFAULT 0,
    last_delivery_error TEXT,
    created_at         INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_dispatch
    ON tasks(state, lease_expires_at, created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_recipient ON tasks(recipient);
"""


class QueueStore:
    """所有写操作在同一把锁内以 BEGIN IMMEDIATE 事务完成，保证跨线程原子性。"""

    def __init__(self, db_path: str):
        self.conn = connect_sqlite(db_path)
        self.conn.executescript(SCHEMA)
        self.lock = threading.RLock()

    # ---------- 收件端管理 ----------
    def upsert_recipient(self, recipient_id: str, base_url: str,
                         max_inflight: int) -> dict:
        parsed = urlparse(base_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise HttpError(400, f"base_url 不合法: {base_url}")
        if max_inflight < 0:
            raise HttpError(400, "max_inflight 必须 >= 0")
        with self.lock:
            row = self.conn.execute(
                "SELECT id FROM recipients WHERE id=?", (recipient_id,)).fetchone()
            if row is None:
                self.conn.execute(
                    "INSERT INTO recipients(id, base_url, max_inflight, created_at)"
                    " VALUES(?,?,?,?)",
                    (recipient_id, base_url.rstrip("/"), max_inflight, now_ms()))
            else:
                self.conn.execute(
                    "UPDATE recipients SET base_url=?, max_inflight=? WHERE id=?",
                    (base_url.rstrip("/"), max_inflight, recipient_id))
        return self.get_recipient(recipient_id)

    def get_recipient(self, recipient_id: str) -> dict:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM recipients WHERE id=?", (recipient_id,)).fetchone()
        if row is None:
            raise HttpError(404, f"未知收件端: {recipient_id}")
        return dict(row)

    def list_recipients(self) -> list[dict]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM recipients ORDER BY id").fetchall()
            out = []
            for row in rows:
                item = dict(row)
                item["inflight"] = self._inflight(item["id"])
                out.append(item)
        return out

    def _inflight(self, recipient: str) -> int:
        """未收妥任务数：首次领出（attempt_count>0）到 delivered 之间都占名额。"""
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM tasks "
            "WHERE recipient=? AND attempt_count > 0 AND state != 'delivered'",
            (recipient,)).fetchone()
        return row["c"]

    # ---------- 任务提交 ----------
    def submit(self, recipient: str, artifact: str) -> dict:
        if not artifact or not isinstance(artifact, str):
            raise HttpError(400, "artifact 必须是非空字符串（产物路径）")
        self.get_recipient(recipient)  # 收件端必须已注册
        task_id = uuid.uuid4().hex
        with self.lock:
            self.conn.execute(
                "INSERT INTO tasks(id, recipient, artifact, created_at)"
                " VALUES(?,?,?,?)",
                (task_id, recipient, artifact, now_ms()))
        LOG.info("任务 %s 已入队 -> 收件端 %s 产物 %s", task_id, recipient, artifact)
        return self.get_task(task_id)

    def get_task(self, task_id: str) -> dict:
        with self.lock:
            row = self.conn.execute("SELECT * FROM tasks WHERE id=?",
                                    (task_id,)).fetchone()
        if row is None:
            raise HttpError(404, f"未知任务: {task_id}")
        return self._task_dict(row)

    def list_tasks(self, recipient: str | None = None) -> list[dict]:
        with self.lock:
            if recipient:
                rows = self.conn.execute(
                    "SELECT * FROM tasks WHERE recipient=? ORDER BY created_at",
                    (recipient,)).fetchall()
            else:
                rows = self.conn.execute(
                    "SELECT * FROM tasks ORDER BY created_at").fetchall()
        return [self._task_dict(r) for r in rows]

    def _task_dict(self, row: sqlite3.Row) -> dict:
        item = dict(row)
        if item.get("result"):
            try:
                item["result"] = json.loads(item["result"])
            except ValueError:
                pass
        return item

    # ---------- 租约领取 ----------
    def lease(self, worker_id: str, lease_ttl_ms: int) -> dict | None:
        """领取一个任务。

        - 新任务（queued）：收件端名额未满才能首次领取；
        - 到期换手任务（leased 且租约过期）：不受名额限制，保证卡住的任务
          永远可以被其他进程接手。
        """
        now = now_ms()
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                candidates = self.conn.execute(
                    "SELECT * FROM tasks WHERE state='queued'"
                    " OR (state='leased' AND lease_expires_at < ?)"
                    " ORDER BY created_at",
                    (now,)).fetchall()
                chosen = None
                for row in candidates:
                    is_first_lease = row["attempt_count"] == 0
                    if is_first_lease and self._inflight(row["recipient"]) >= \
                            self.get_recipient(row["recipient"])["max_inflight"]:
                        continue  # 该收件端满额：暂停首次领取，看下一个候选
                    chosen = row
                    break
                if chosen is None:
                    self.conn.execute("COMMIT")
                    return None
                token = uuid.uuid4().hex
                expires = now + lease_ttl_ms
                self.conn.execute(
                    "UPDATE tasks SET state='leased', lease_token=?,"
                    " leased_at=?, lease_expires_at=?, attempt_count=attempt_count+1"
                    " WHERE id=?",
                    (token, now, expires, chosen["id"]))
                self.conn.execute("COMMIT")
                LOG.info("工作进程 %s 领取任务 %s（第 %d 次尝试，租约 %dms）",
                         worker_id, chosen["id"], chosen["attempt_count"] + 1,
                         lease_ttl_ms)
                task = self.get_task(chosen["id"])
                task["lease_token"] = token
                return task
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    # ---------- 结果交回 ----------
    def complete(self, task_id: str, lease_token: str, result) -> dict:
        if not isinstance(lease_token, str) or not lease_token:
            raise HttpError(400, "缺少 lease_token")
        try:
            result_text = json.dumps(result, ensure_ascii=False)
        except (TypeError, ValueError):
            raise HttpError(400, "result 必须是可 JSON 序列化的对象")
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                row = self.conn.execute(
                    "SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
                if row is None:
                    self.conn.execute("COMMIT")
                    raise HttpError(404, f"未知任务: {task_id}")
                if row["state"] != "leased":
                    self.conn.execute("COMMIT")
                    raise HttpError(
                        409, f"任务 {task_id} 当前状态 {row['state']}，"
                             "不再接受交回（结果已接受或尚未领取）")
                if row["lease_expires_at"] is not None \
                        and row["lease_expires_at"] < now_ms():
                    self.conn.execute("COMMIT")
                    raise HttpError(
                        409, f"任务 {task_id} 的租约已到期，"
                             "旧工作进程的结果被拒绝，等待其他进程接手")
                if row["lease_token"] != lease_token:
                    self.conn.execute("COMMIT")
                    raise HttpError(
                        409, f"租约令牌无效：任务 {task_id} 已换手或租约已更新，"
                             "旧工作进程的结果被拒绝")
                self.conn.execute(
                    "UPDATE tasks SET state='succeeded', result=?,"
                    " completed_at=? WHERE id=?",
                    (result_text, now_ms(), task_id))
                self.conn.execute("COMMIT")
                LOG.info("任务 %s 的结果已接受（succeeded），等待投递", task_id)
                return self.get_task(task_id)
            except HttpError:
                raise
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    # ---------- 投递 ----------
    def deliverable_tasks(self) -> list[sqlite3.Row]:
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM tasks WHERE state='succeeded' ORDER BY completed_at"
            ).fetchall()

    def mark_delivery_attempt(self, task_id: str, error: str | None) -> None:
        with self.lock:
            self.conn.execute(
                "UPDATE tasks SET delivery_attempts=delivery_attempts+1,"
                " last_delivery_error=? WHERE id=?",
                (error, task_id))

    def mark_delivered(self, task_id: str) -> None:
        with self.lock:
            self.conn.execute(
                "UPDATE tasks SET state='delivered', delivered_at=?,"
                " last_delivery_error=NULL WHERE id=?",
                (now_ms(), task_id))

    def close(self) -> None:
        self.conn.close()


def _query_param(req, name: str) -> str:
    query = req.path.split("?", 1)[1] if "?" in req.path else ""
    for pair in query.split("&"):
        key, _, value = pair.partition("=")
        if key == name:
            return value
    raise HttpError(400, f"缺少查询参数 {name}")


def make_routes(store: QueueStore, lease_ttl_ms: int,
                delivery_stats=None) -> dict:
    def register(payload, _req):
        recipient = payload.get("id")
        base_url = payload.get("base_url")
        max_inflight = payload.get("max_inflight")
        if not recipient or not isinstance(recipient, str):
            raise HttpError(400, "缺少收件端 id")
        if not isinstance(max_inflight, int) or isinstance(max_inflight, bool):
            raise HttpError(400, "max_inflight 必须是整数")
        return 200, store.upsert_recipient(recipient, base_url, max_inflight)

    def list_recipients(_payload, _req):
        return 200, {"recipients": store.list_recipients()}

    def submit(payload, _req):
        return 201, store.submit(payload.get("recipient"),
                                 payload.get("artifact"))

    def list_tasks(_payload, req):
        recipient = None
        if "?" in req.path:
            query = req.path.split("?", 1)[1]
            for pair in query.split("&"):
                if pair.startswith("recipient="):
                    recipient = pair.split("=", 1)[1]
        return 200, {"tasks": store.list_tasks(recipient)}

    def get_task_by_query(_payload, req):
        return 200, store.get_task(_query_param(req, "id"))

    def get_task_by_path(_payload, req):
        return 200, store.get_task(_query_param(req, "task_id"))

    def complete_by_path(payload, req):
        task_id = _query_param(req, "task_id")
        return 200, store.complete(
            task_id, payload.get("lease_token"), payload.get("result"))

    def lease(payload, _req):
        worker_id = payload.get("worker_id", "unknown")
        return 200, {"task": store.lease(worker_id, lease_ttl_ms)}

    def health(_payload, _req):
        body = {"ok": True, "time": now_ms()}
        if delivery_stats is not None:
            body["delivery"] = delivery_stats()
        return 200, body

    return {
        ("POST", "/recipients"): register,
        ("GET", "/recipients"): list_recipients,
        ("POST", "/tasks"): submit,
        ("GET", "/tasks"): list_tasks,
        ("GET", "/task"): get_task_by_query,
        ("GET", "/__task_get__"): get_task_by_path,
        ("POST", "/__task_complete__"): complete_by_path,
        ("POST", "/lease"): lease,
        ("GET", "/health"): health,
        ("POST", "/health"): health,
    }


class _RoutedHandler(Handler):
    """支持 /tasks/<id>（GET）与 /tasks/<id>/complete（POST）路径参数。

    把带参数的路径改写成内部路由后交给基类统一处理（含 HttpError 捕获），
    具体参数通过 server.store 与请求体在路由表的处理函数中读取。
    """

    def _handle(self, method: str) -> None:
        path = self.path.split("?")[0]
        parts = path.strip("/").split("/")
        if method == "GET" and len(parts) == 2 and parts[0] == "tasks":
            # 改写成内部路由，任务 id 经查询串传递，复用基类异常处理
            self.path = f"/__task_get__?task_id={parts[1]}"
        elif method == "POST" and len(parts) == 3 and parts[0] == "tasks" \
                and parts[2] == "complete":
            self.path = f"/__task_complete__?task_id={parts[1]}"
        super()._handle(method)


class DeliveryEngine:
    """后台投递调度器。

    - 全局同时进行的投递尝试不超过 ``concurrency`` 个（信号量名额）；一次
      尝试从发起到真正结束（含挂起/超时）都占着名额，只有线程返回才释放。
    - 每次 HTTP 请求受 ``request_timeout`` 整次限时约束（连接、等待响应、
      读取响应体合计），慢速零星返回数据也会在时限内结束。
    - 同一收件端同时只允许一次投递：单个故障端的积压最多占一个名额，
      不会把全局并发占满而阻塞其他收件端。
    - 有多个收件端待投时按收件端轮转（round-robin）挑选，保证在线端不会
      被故障端的长期积压饿死。
    """

    def __init__(self, store: QueueStore, stop_event: threading.Event,
                 interval: float, concurrency: int, request_timeout: float):
        self.store = store
        self.stop_event = stop_event
        self.interval = interval
        self.request_timeout = request_timeout
        self._slots = threading.BoundedSemaphore(concurrency)
        self._cond = threading.Condition()
        self._inflight_recipients: set[str] = set()
        self._cursor = 0
        self.concurrency = concurrency
        self._stats_lock = threading.Lock()
        self._active = 0
        self._max_active = 0
        self._executor = ThreadPoolExecutor(
            max_workers=concurrency, thread_name_prefix="deliver")

    def stats(self) -> dict:
        """当前/历史最大并发投递尝试数，用于观测名额占用（含挂起未结束的）。"""
        with self._stats_lock:
            return {"concurrency": self.concurrency,
                    "active": self._active,
                    "max_active": self._max_active}

    def start(self) -> threading.Thread:
        thread = threading.Thread(target=self._run, name="delivery", daemon=True)
        thread.start()
        return thread

    # ---------- 调度 ----------
    def _run(self) -> None:
        while not self.stop_event.is_set():
            # 先拿一个全局并发名额；名额耗尽时等待而非展开新请求
            while not self._slots.acquire(timeout=0.2):
                if self.stop_event.is_set():
                    return
            if self.stop_event.is_set():
                self._slots.release()
                return
            row, recipient_id = self._pick()
            if row is None:
                self._slots.release()
                self.stop_event.wait(self.interval)
                continue
            try:
                self._executor.submit(self._attempt, row, recipient_id)
            except Exception:
                LOG.exception("提交投递任务失败")
                self._release_recipient(recipient_id)
                self._slots.release()

    def _pick(self):
        """按收件端轮转挑选下一个待投任务；返回 (row, recipient_id)。"""
        rows = self.store.deliverable_tasks()
        if not rows:
            return None, None
        by_recipient: dict[str, list] = {}
        for row in rows:  # deliverable_tasks 已按 completed_at 升序
            by_recipient.setdefault(row["recipient"], []).append(row)
        ids = sorted(by_recipient)
        with self._cond:
            for offset in range(len(ids)):
                index = (self._cursor + offset) % len(ids)
                rid = ids[index]
                if rid in self._inflight_recipients:
                    continue  # 该端已有一次投递在途，交给其他端
                self._cursor = (index + 1) % len(ids)
                self._inflight_recipients.add(rid)
                return by_recipient[rid][0], rid
        return None, None

    def _release_recipient(self, recipient_id: str) -> None:
        with self._cond:
            self._inflight_recipients.discard(recipient_id)
            self._cond.notify_all()

    # ---------- 单次尝试 ----------
    def _attempt(self, row, recipient_id: str) -> None:
        with self._stats_lock:
            self._active += 1
            self._max_active = max(self._max_active, self._active)
        try:
            self._deliver(row, recipient_id)
        except Exception:
            LOG.exception("投递任务异常: %s", row["id"])
        finally:
            with self._stats_lock:
                self._active -= 1
            self._release_recipient(recipient_id)
            self._slots.release()

    def _deliver(self, row, recipient_id: str) -> None:
        recipient = self.store.get_recipient(recipient_id)
        url = recipient["base_url"] + "/results"
        try:
            result = json.loads(row["result"]) if row["result"] else None
            http_request(url, {
                "task_id": row["id"],
                "recipient": row["recipient"],
                "artifact": row["artifact"],
                "result": result,
            }, method="POST", timeout=self.request_timeout)
            self.store.mark_delivered(row["id"])
            LOG.info("任务 %s 已投递并由收件端 %s 收妥",
                     row["id"], recipient_id)
        except Exception as exc:  # 收件端停机/挂起/超时等：稍后重试
            self.store.mark_delivery_attempt(row["id"], str(exc)[:300])
            LOG.warning("任务 %s 投递失败（将重试）: %s", row["id"], exc)

    def shutdown(self) -> None:
        # 单次尝试受整次请求限时约束，等待其结束可避免在连接关闭后再写库
        self._executor.shutdown(wait=True, cancel_futures=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="校验队列服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--db", required=True, help="SQLite 数据库文件路径")
    parser.add_argument("--lease-ttl", type=float, default=10.0,
                        help="租约有效期（秒），到期后任务可被其他进程接手")
    parser.add_argument("--delivery-interval", type=float, default=0.5,
                        help="后台投递轮询间隔（秒）")
    parser.add_argument("--delivery-concurrency", type=int, default=4,
                        help="全局同时进行的投递尝试上限（默认 4）")
    parser.add_argument("--delivery-timeout", type=float, default=5.0,
                        help="单次投递请求的整次限时（秒），挂起/超时的尝试"
                             "在结束前仍占并发名额")
    args = parser.parse_args()
    if args.delivery_concurrency < 1:
        parser.error("--delivery-concurrency 必须 >= 1")
    if args.delivery_timeout <= 0:
        parser.error("--delivery-timeout 必须 > 0")

    store = QueueStore(args.db)
    stop_event = threading.Event()
    engine = DeliveryEngine(store, stop_event, args.delivery_interval,
                            args.delivery_concurrency, args.delivery_timeout)
    routes = make_routes(store, int(args.lease_ttl * 1000), engine.stats)
    engine.start()

    httpd = _ThreadingHTTPServer((args.host, args.port), _RoutedHandler)
    httpd.routes = routes
    httpd.store = store
    httpd.log_name = "queue"
    LOG.info("队列数据库: %s，租约 TTL=%.1fs", args.db, args.lease_ttl)
    LOG.info("HTTP 服务监听 http://%s:%s", args.host, args.port)
    try:
        httpd.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        engine.shutdown()
        httpd.shutdown()
        httpd.server_close()
        store.close()
        LOG.info("队列服务已停止")


if __name__ == "__main__":
    main()
