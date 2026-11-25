"""命令行客户端：提交校验任务、查询任务进度、在收件端查看结果。

用法示例：
  python3 -I client.py register --queue http://127.0.0.1:8080 \
      --id alpha --inbox http://127.0.0.1:9001 --max-inflight 2
  python3 -I client.py submit --queue ... --recipient alpha \
      --artifact /abs/path/good_artifact.json
  python3 -I client.py status --queue ... [--task-id ID] [--recipient alpha]
  python3 -I client.py inbox --inbox http://127.0.0.1:9001 [--task-id ID]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# -I 隔离模式下脚本目录不在 sys.path，显式加入以导入同目录公共模块
sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import HttpError, http_request


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="校验队列客户端")
    parser.add_argument("--queue", default="http://127.0.0.1:8080",
                        help="队列服务地址")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_reg = sub.add_parser("register", help="注册/更新收件端及其名额上限")
    p_reg.add_argument("--id", required=True)
    p_reg.add_argument("--inbox", required=True, help="收件服务基础地址")
    p_reg.add_argument("--max-inflight", type=int, required=True,
                       help="该收件端未收妥任务数上限")

    p_sub = sub.add_parser("submit", help="提交校验任务")
    p_sub.add_argument("--recipient", required=True)
    p_sub.add_argument("--artifact", required=True)

    p_st = sub.add_parser("status", help="查询任务进度")
    p_st.add_argument("--task-id")
    p_st.add_argument("--recipient")

    p_in = sub.add_parser("inbox", help="在收件端查看已收妥结果")
    p_in.add_argument("--inbox", required=True)
    p_in.add_argument("--task-id")

    p_rec = sub.add_parser("recipients", help="查看收件端及名额占用")

    args = parser.parse_args()
    base = args.queue.rstrip("/")

    try:
        if args.cmd == "register":
            _print(http_request(base + "/recipients", {
                "id": args.id, "base_url": args.inbox,
                "max_inflight": args.max_inflight}, method="POST"))
        elif args.cmd == "submit":
            _print(http_request(base + "/tasks", {
                "recipient": args.recipient,
                "artifact": args.artifact}, method="POST"))
        elif args.cmd == "status":
            if args.task_id:
                _print(http_request(f"{base}/task?id={args.task_id}"))
            else:
                url = base + "/tasks"
                if args.recipient:
                    url += f"?recipient={args.recipient}"
                _print(http_request(url))
        elif args.cmd == "inbox":
            url = args.inbox.rstrip("/") + "/results"
            if args.task_id:
                url += f"?task_id={args.task_id}"
            _print(http_request(url))
        elif args.cmd == "recipients":
            _print(http_request(base + "/recipients"))
    except HttpError as exc:
        raise SystemExit(f"请求失败 [{exc.status}] {exc.message}")


if __name__ == "__main__":
    main()
