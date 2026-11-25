"""校验工作进程：独立进程，多个实例共享同一个队列服务。

循环：
1. POST /lease 凭 worker_id 领一个带租约的任务（没有任务时空转）；
2. 调用项目自带样例校验器（samples/sample_validator.py）对产物做校验；
3. POST /tasks/<id>/complete 凭租约令牌交回结果；
   - 租约已过期并被其他进程接手时，队列返回 409，本进程放弃旧结果；
   - 队列只接受当前有效租约交回的结果。
进程被 SIGTERM/SIGINT 杀死时，未完成任务的租约到期后自动由其他进程接手。
"""
from __future__ import annotations

import argparse
import json
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

# -I 隔离模式下脚本目录不在 sys.path，显式加入以导入同目录公共模块
sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import HttpError, get_logger, http_request

LOG = get_logger("worker")

_shutdown = False


def _handle_signal(_signum, _frame):
    global _shutdown
    _shutdown = True


def run_validator(python_bin: str, validator: Path, artifact: Path,
                  task_id: str, timeout: float) -> dict:
    """以子进程方式运行自带样例校验器，返回结构化结果。"""
    cmd = [python_bin, "-I", str(validator), "--artifact", str(artifact),
           "--task-id", task_id]
    started = time.time()
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": f"校验器超时（>{timeout}s）",
                "duration_sec": round(time.time() - started, 2)}
    duration = round(time.time() - started, 2)
    if proc.returncode != 0:
        return {"ok": False,
                "error": f"校验器退出码 {proc.returncode}: "
                         f"{proc.stderr.strip()[:300]}",
                "duration_sec": duration}
    try:
        result = json.loads(proc.stdout)
    except ValueError:
        return {"ok": False, "error": f"校验器输出不是 JSON: {proc.stdout[:200]}",
                "duration_sec": duration}
    result.setdefault("duration_sec", duration)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="校验工作进程")
    parser.add_argument("--queue", required=True, help="队列服务地址，如 http://127.0.0.1:8080")
    parser.add_argument("--worker-id", default=None, help="工作进程标识，默认自动生成")
    parser.add_argument("--poll-interval", type=float, default=0.3,
                        help="无任务时的轮询间隔（秒）")
    parser.add_argument("--validator", required=True,
                        help="自带样例校验器路径（sample_validator.py）")
    parser.add_argument("--python", default=sys.executable,
                        help="运行校验器的 Python 解释器")
    parser.add_argument("--run-timeout", type=float, default=30.0,
                        help="单次校验的最长执行时间（秒）")
    args = parser.parse_args()

    worker_id = args.worker_id or f"worker-{uuid.uuid4().hex[:8]}"
    validator = Path(args.validator).resolve()
    if not validator.is_file():
        raise SystemExit(f"校验器不存在: {validator}")

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    LOG.info("工作进程 %s 启动，队列=%s，校验器=%s",
             worker_id, args.queue, validator)

    while not _shutdown:
        try:
            resp = http_request(args.queue.rstrip("/") + "/lease",
                                {"worker_id": worker_id}, method="POST")
        except Exception as exc:
            LOG.warning("领取任务失败（%s 后重试）: %s",
                        args.poll_interval, exc)
            _interruptible_sleep(args.poll_interval)
            continue

        task = resp.get("task") if resp else None
        if not task:
            _interruptible_sleep(args.poll_interval)
            continue

        task_id = task["id"]
        token = task["lease_token"]
        artifact = Path(task["artifact"])
        LOG.info("工作进程 %s 开始校验任务 %s（产物 %s）",
                 worker_id, task_id, artifact)
        result = run_validator(args.python, validator, artifact, task_id,
                               args.run_timeout)
        LOG.info("工作进程 %s 完成任务 %s，结果 ok=%s",
                 worker_id, task_id, result.get("ok"))

        try:
            http_request(
                f"{args.queue.rstrip('/')}/tasks/{task_id}/complete",
                {"lease_token": token, "result": result}, method="POST")
            LOG.info("任务 %s 结果已被队列接受", task_id)
        except HttpError as exc:
            if exc.status == 409:
                # 租约已过期、任务已换手：旧结果必须丢弃，由新租约持有者交回
                LOG.warning("任务 %s 的租约已失效（409），丢弃本进程结果: %s",
                            task_id, exc.message)
            else:
                LOG.error("任务 %s 交回失败: %s", task_id, exc)
        except Exception as exc:
            LOG.error("任务 %s 交回时网络错误，租约到期后可被接手: %s",
                      task_id, exc)

    LOG.info("工作进程 %s 已退出", worker_id)


def _interruptible_sleep(seconds: float) -> None:
    """可被信号打断的 sleep。"""
    end = time.time() + seconds
    while not _shutdown and time.time() < end:
        time.sleep(min(0.1, end - time.time()))


if __name__ == "__main__":
    main()
