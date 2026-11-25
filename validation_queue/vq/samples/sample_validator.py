#!/usr/bin/env python3
"""项目自带样例：产物校验器。

工作进程以子进程方式调用本脚本：

    python3 -I sample_validator.py --artifact <产物路径> --task-id <任务id>

校验规则（样例）：
- 产物是 JSON 文件，字段 payload / sha256；
- 重新计算 payload（UTF-8 字节）的 SHA-256，与 sha256 字段比对；
- payload 中出现 "CORRUPTED" 视为损坏产物。

输出一行 JSON 到 stdout：
    {"ok": true/false, "task_id": ..., "artifact": ..., "checksum": ..., ...}

测试钩子（环境变量，均为可选项）：
- VQ_WORK_TIME      校验耗时秒数（模拟慢任务，用于租约换手测试）
- VQ_RUN_LOG        每次运行追加一行记录的文件，统计校验实际执行次数
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time


def main() -> int:
    parser = argparse.ArgumentParser(description="样例产物校验器")
    parser.add_argument("--artifact", required=True)
    parser.add_argument("--task-id", required=True)
    args = parser.parse_args()

    work_time = float(os.environ.get("VQ_WORK_TIME", "0"))
    run_log = os.environ.get("VQ_RUN_LOG")
    if run_log:
        with open(run_log, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({
                "task_id": args.task_id,
                "artifact": args.artifact,
                "pid": os.getpid(),
                "ts": time.time(),
            }, ensure_ascii=False) + "\n")

    if work_time > 0:
        time.sleep(work_time)  # 模拟慢校验：超过租约 TTL 即可制造换手场景

    result = {"task_id": args.task_id, "artifact": args.artifact}
    try:
        with open(args.artifact, "rb") as fh:
            raw = fh.read()
        marker = json.loads(raw.decode("utf-8"))
    except FileNotFoundError:
        result.update(ok=False, error="产物文件不存在")
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except (ValueError, UnicodeDecodeError) as exc:
        result.update(ok=False, error=f"产物不是合法 JSON: {exc}")
        print(json.dumps(result, ensure_ascii=False))
        return 0

    payload = marker.get("payload", "")
    actual = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    result["checksum"] = actual
    result["size_bytes"] = len(payload.encode("utf-8"))

    if "CORRUPTED" in payload:
        result.update(ok=False, error="产物内容已损坏（包含 CORRUPTED 标记）")
    elif marker.get("sha256") != actual:
        result.update(ok=False,
                      error="校验和不匹配",
                      expected=marker.get("sha256"))
    else:
        result.update(ok=True, artifact_name=marker.get("name"))

    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
