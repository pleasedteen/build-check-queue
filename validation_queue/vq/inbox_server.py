"""项目收件服务：独立持有本地 SQLite，只负责持久收妥校验结果。

- POST /results   幂等收妥：同一 task_id 只保留一份结果，重复投递只计数；
- GET  /results   列出全部已收妥结果；
- GET  /results?task_id=xxx  查询单个任务的结果（不存在返回 404）；
- GET  /health    健康检查。

服务可独立重启，数据落盘后不丢失。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# -I 隔离模式下脚本目录不在 sys.path，显式加入以导入同目录公共模块
sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (Handler, HttpError, _ThreadingHTTPServer, connect_sqlite,
                    get_logger, now_ms)

LOG = get_logger("inbox")

SCHEMA = """
CREATE TABLE IF NOT EXISTS results (
    task_id        TEXT PRIMARY KEY,
    recipient      TEXT NOT NULL,
    artifact       TEXT NOT NULL,
    result         TEXT NOT NULL,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    received_at    INTEGER NOT NULL
);
"""


class InboxStore:
    def __init__(self, db_path: str, recipient_id: str):
        self.recipient_id = recipient_id
        self.conn = connect_sqlite(db_path)
        self.conn.executescript(SCHEMA)

    def put_result(self, task_id: str, recipient: str, artifact: str,
                   result) -> dict:
        import json
        if not task_id:
            raise HttpError(400, "缺少 task_id")
        result_text = json.dumps(result, ensure_ascii=False)
        self.conn.execute(
            "INSERT INTO results(task_id, recipient, artifact, result,"
            " duplicate_count, received_at) VALUES(?,?,?,?,0,?)"
            " ON CONFLICT(task_id) DO UPDATE SET duplicate_count=duplicate_count+1",
            (task_id, recipient, artifact, result_text, now_ms()))
        row = self.conn.execute("SELECT * FROM results WHERE task_id=?",
                                (task_id,)).fetchone()
        item = dict(row)
        item["result"] = json.loads(item["result"])
        # duplicate_count>0 说明这条结果此前已经收妥过，本次为重复投递
        item["duplicated"] = item["duplicate_count"] > 0
        return item

    def get_result(self, task_id: str) -> dict:
        import json
        row = self.conn.execute("SELECT * FROM results WHERE task_id=?",
                                (task_id,)).fetchone()
        if row is None:
            raise HttpError(404, f"收件端 {self.recipient_id} 没有任务 "
                                 f"{task_id} 的结果")
        item = dict(row)
        item["result"] = json.loads(item["result"])
        return item

    def list_results(self) -> list[dict]:
        import json
        rows = self.conn.execute(
            "SELECT * FROM results ORDER BY received_at").fetchall()
        out = []
        for row in rows:
            item = dict(row)
            item["result"] = json.loads(item["result"])
            out.append(item)
        return out

    def close(self) -> None:
        self.conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="项目收件服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--db", required=True, help="本收件服务的 SQLite 文件")
    parser.add_argument("--id", required=True, help="收件端标识")
    args = parser.parse_args()

    store = InboxStore(args.db, args.id)

    def put(payload, _req):
        if payload.get("recipient") not in (None, store.recipient_id):
            raise HttpError(400, "结果的 recipient 与本收件服务不一致")
        item = store.put_result(payload.get("task_id"),
                                payload.get("recipient", store.recipient_id),
                                payload.get("artifact", ""),
                                payload.get("result"))
        # 幂等：无论首次还是重复，队列都视为已收妥（200 + 同一 task_id）
        return 200, {"ok": True, "task_id": item["task_id"],
                     "duplicated": item["duplicated"]}

    def list_results(_payload, req):
        if "?" in req.path and "task_id=" in req.path:
            task_id = None
            for pair in req.path.split("?", 1)[1].split("&"):
                if pair.startswith("task_id="):
                    task_id = pair.split("=", 1)[1]
            return 200, store.get_result(task_id)
        return 200, {"recipient": store.recipient_id,
                     "results": store.list_results()}

    def health(_payload, _req):
        return 200, {"ok": True, "recipient": store.recipient_id,
                     "time": now_ms()}

    routes = {
        ("POST", "/results"): put,
        ("GET", "/results"): list_results,
        ("GET", "/health"): health,
    }

    httpd = _ThreadingHTTPServer((args.host, args.port), Handler)
    httpd.routes = routes
    httpd.log_name = f"inbox-{args.id}"
    LOG.info("收件端 %s 数据文件 %s，监听 http://%s:%s",
             args.id, args.db, args.host, args.port)
    try:
        httpd.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        pass
    finally:
        httpd.shutdown()
        httpd.server_close()
        store.close()
        LOG.info("收件端 %s 已停止", args.id)


if __name__ == "__main__":
    main()
