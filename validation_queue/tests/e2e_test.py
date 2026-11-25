#!/usr/bin/env python3
"""端到端测试：一条命令覆盖全部关键场景，80 秒内输出可读结果。

覆盖：
  1. 正常流程：提交 -> 多工作进程领取 -> 样例校验 -> 投递 -> 收件端可查；
  2. 租约换手：工作进程卡死/退出，租约到期后另一进程接手，且队列拒绝
     过期租约交回的旧结果（同一任务只执行出一份结果）；
  3. 收件端名额：满额时暂停该端首次领取，其他收件端与到期换手不受影响；
  4. 服务故障恢复：收件服务停机期间结果在队列持久保留，恢复后自动续投，
     队列重启与收件端重启后均不丢结果、不重复执行校验。

直接运行：
    python3 -I tests/e2e_test.py
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VQ = ROOT / "vq"
PYTHON = sys.executable

# 时序参数（秒）：留足余量，保证在慢速 CI 上也稳定，整体 < 80s
LEASE_TTL = 5.0
SLOW_WORK = 2.0
POLL = 0.3
DELIVERY_INTERVAL = 0.4
STEP_TIMEOUT = 25.0

passed = 0
failed = 0
_t0 = time.time()


def log(msg: str) -> None:
    print(f"[{time.time() - _t0:6.2f}s] {msg}", flush=True)


def check(name: str, cond: bool, detail: str = "") -> None:
    global passed, failed
    if cond:
        passed += 1
        log(f"  ✅ {name}")
    else:
        failed += 1
        log(f"  ❌ {name} {detail}")


def wait_until(desc: str, fn, timeout: float = STEP_TIMEOUT,
               interval: float = 0.3):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            value = fn()
            if value:
                return value
        except Exception as exc:
            last = exc
        time.sleep(interval)
    raise AssertionError(f"等待超时: {desc}（最后错误: {last}）")


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def http(url: str, payload=None, method: str = "GET", timeout: float = 8.0):
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            return resp.status, json.loads(body) if body else None
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode() or "{}")


class Process:
    def __init__(self, name: str, cmd: list[str], env: dict | None = None):
        self.name = name
        full_env = dict(os.environ)
        if env:
            full_env.update(env)
        self.log_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=f".{name}.log", delete=False, prefix="vq-")
        self.proc = subprocess.Popen(
            cmd, stdout=self.log_file, stderr=subprocess.STDOUT,
            env=full_env, start_new_session=True)
        log(f"启动 {name} (pid={self.proc.pid})")

    def alive(self) -> bool:
        return self.proc.poll() is None

    def stop(self) -> None:
        if not self.alive():
            return
        try:
            os.killpg(os.getpgid(self.proc.pid), 15)  # 整个进程组，含校验器子进程
        except ProcessLookupError:
            pass
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(self.proc.pid), 9)
            self.proc.wait()
        self.log_file.close()
        log(f"停止 {self.name}")

    def kill9(self) -> None:
        """模拟崩溃：SIGKILL，不给清理机会。"""
        if self.alive():
            os.killpg(os.getpgid(self.proc.pid), 9)
            self.proc.wait()
            log(f"强杀 {self.name}（模拟崩溃）")


def wait_http(url: str, name: str) -> None:
    def up():
        try:
            status, _ = http(url, method="GET")
            return status == 200
        except OSError:
            return False
    wait_until(f"{name} 就绪", up, timeout=15)


def make_artifact(tmp: Path, fname: str, payload: str,
                  corrupt_checksum: bool = False) -> Path:
    import hashlib
    marker = {"name": fname, "payload": payload,
              "sha256": hashlib.sha256(payload.encode()).hexdigest()}
    if corrupt_checksum:
        marker["sha256"] = "0" * 64
    path = tmp / fname
    path.write_text(json.dumps(marker), encoding="utf-8")
    return path


class Harness:
    def __init__(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="vq-e2e-"))
        self.qdb = self.tmp / "queue.db"
        self.alpha_db = self.tmp / "alpha.db"
        self.beta_db = self.tmp / "beta.db"
        self.gamma_db = self.tmp / "gamma.db"
        self.qport = free_port()
        self.alpha_port = free_port()
        self.beta_port = free_port()
        self.gamma_port = free_port()
        self.queue_url = f"http://127.0.0.1:{self.qport}"
        self.alpha_url = f"http://127.0.0.1:{self.alpha_port}"
        self.beta_url = f"http://127.0.0.1:{self.beta_port}"
        self.gamma_url = f"http://127.0.0.1:{self.gamma_port}"
        self.procs: list[Process] = []
        self.validator = str(VQ / "samples" / "sample_validator.py")
        self.run_log = str(self.tmp / "validator_runs.log")
        self.good = make_artifact(self.tmp, "good.json", "normal-artifact")
        self.bad = make_artifact(self.tmp, "bad.json", "tampered",
                                 corrupt_checksum=True)

    # --- 进程启动 ---
    def start_queue(self):
        p = Process("queue", [
            PYTHON, "-I", str(VQ / "queue_server.py"),
            "--port", str(self.qport), "--db", str(self.qdb),
            "--lease-ttl", str(LEASE_TTL),
            "--delivery-interval", str(DELIVERY_INTERVAL)])
        self.procs.append(p)
        wait_http(self.queue_url + "/health", "队列")
        return p

    def start_inbox(self, name: str, port: int, db: Path):
        p = Process(f"inbox-{name}", [
            PYTHON, "-I", str(VQ / "inbox_server.py"),
            "--port", str(port), "--db", str(db), "--id", name])
        self.procs.append(p)
        wait_http(f"http://127.0.0.1:{port}/health", f"收件端 {name}")
        return p

    def start_worker(self, name: str, slow: bool = False):
        env = {"VQ_RUN_LOG": self.run_log}
        if slow:
            env["VQ_WORK_TIME"] = str(SLOW_WORK)
        p = Process(name, [
            PYTHON, "-I", str(VQ / "worker.py"),
            "--queue", self.queue_url, "--worker-id", name,
            "--validator", self.validator,
            "--poll-interval", str(POLL)], env=env)
        self.procs.append(p)
        return p

    def register(self, rid: str, url: str, cap: int):
        status, body = http(self.queue_url + "/recipients", {
            "id": rid, "base_url": url, "max_inflight": cap}, method="POST")
        assert status == 200, body

    def submit(self, rid: str, artifact: Path) -> str:
        status, body = http(self.queue_url + "/tasks", {
            "recipient": rid, "artifact": str(artifact)}, method="POST")
        assert status == 201, body
        return body["id"]

    def task(self, tid: str):
        _, body = http(f"{self.queue_url}/task?id={tid}")
        return body

    def inbox_result(self, url: str, tid: str):
        status, body = http(f"{url}/results?task_id={tid}")
        return status, body

    def inbox_list(self, url: str):
        _, body = http(url + "/results")
        return body["results"]

    def validator_run_count(self, tid: str) -> int:
        if not Path(self.run_log).exists():
            return 0
        return sum(1 for line in Path(self.run_log).read_text().splitlines()
                   if line and json.loads(line)["task_id"] == tid)

    def stop_named(self, name: str) -> Process:
        for p in list(self.procs):
            if p.name == name:
                p.stop()
                self.procs.remove(p)
                return p
        raise KeyError(name)

    def teardown(self):
        for p in list(self.procs):
            p.stop()
        log(f"临时目录保留在: {self.tmp}")


def main() -> int:
    global passed, failed
    h = Harness()
    try:
        # ========== 0. 启动并注册收件端 ==========
        log("== 场景 0：启动队列、3 个收件服务、2 个工作进程 ==")
        h.start_queue()
        h.start_inbox("alpha", h.alpha_port, h.alpha_db)
        h.start_inbox("beta", h.beta_port, h.beta_db)
        h.start_inbox("gamma", h.gamma_port, h.gamma_db)
        h.register("alpha", h.alpha_url, cap=2)
        h.register("beta", h.beta_url, cap=1)
        h.register("gamma", h.gamma_url, cap=10)
        w1 = h.start_worker("worker-1")
        w2 = h.start_worker("worker-2")
        time.sleep(0.5)

        # ========== 1. 正常流程（含失败产物结果也送达） ==========
        log("== 场景 1：正常流程，合格与损坏产物各一个，投递到 alpha ==")
        t1 = h.submit("alpha", h.good)
        t2 = h.submit("alpha", h.bad)

        def delivered(tid):
            return h.task(tid)["state"] == "delivered"

        wait_until("t1 送达", lambda: delivered(t1))
        wait_until("t2 送达", lambda: delivered(t2))

        st1 = h.task(t1)
        check("t1 状态 delivered", st1["state"] == "delivered")
        check("t1 校验成功", st1["result"]["ok"] is True)
        check("t1 只尝试执行 1 次", st1["attempt_count"] == 1,
              f"attempt={st1['attempt_count']}")

        st2 = h.task(t2)
        check("t2 损坏产物也只投递一次且状态 delivered",
              st2["state"] == "delivered" and st2["result"]["ok"] is False)

        status, r1 = h.inbox_result(h.alpha_url, t1)
        check("收件端 alpha 可查 t1 唯一结果",
              status == 200 and r1["task_id"] == t1 and r1["result"]["ok"])
        check("alpha 收件端恰有 2 份结果", len(h.inbox_list(h.alpha_url)) == 2)
        check("校验器对 t1 只执行一次", h.validator_run_count(t1) == 1)

        # ========== 2. 过期租约的结果必须被拒绝（API 确定性验证） ==========
        log("== 场景 2：过期租约交回应被 409 拒绝，任务换手后正常完成 ==")
        w1.stop(); w2.stop()
        t3 = h.submit("gamma", h.good)
        # 没有工作进程，直接通过 API 拿一个租约
        _, lease_resp = http(h.queue_url + "/lease",
                             {"worker_id": "api-holder"}, method="POST")
        leased = lease_resp["task"]
        check("手动租到的正是最早排队的 t3",
              leased is not None and leased["id"] == t3)
        old_token = leased["lease_token"]
        log(f"  等待租约过期（{LEASE_TTL}s）……")
        time.sleep(LEASE_TTL + 1.0)

        # 旧持有者用过期令牌交回：必须拒绝（此时没有任何工作进程在跑）
        status, body = http(
            f"{h.queue_url}/tasks/{t3}/complete",
            {"lease_token": old_token,
             "result": {"ok": True, "stale": True}}, method="POST")
        check("过期租约交回被 409 拒绝", status == 409, str(body))
        check("被拒后任务仍处于 leased 且结果未写入",
              h.task(t3)["state"] == "leased" and h.task(t3)["result"] is None)

        # 新工作进程接手（到期换手不受名额限制）
        h.start_worker("worker-3")
        wait_until("t3 换手后送达", lambda: delivered(t3), timeout=20)
        st3 = h.task(t3)
        check("t3 最终 delivered 且结果来自真实校验（非过期令牌的伪造结果）",
              st3["state"] == "delivered"
              and st3["result"].get("stale") is None
              and st3["result"].get("checksum"))
        check("t3 经历 2 次租约（旧租约 + 换手）", st3["attempt_count"] == 2,
              f"attempt={st3['attempt_count']}")
        check("校验器对 t3 只真正执行 1 次", h.validator_run_count(t3) == 1)

        # ========== 3. 工作进程崩溃/卡死 -> 租约到期换手 ==========
        log("== 场景 3：工作进程崩溃，租约到期后另一进程接手 ==")
        h.stop_named("worker-3")
        t4 = h.submit("gamma", h.good)
        slow = h.start_worker("slow-crasher", slow=True)
        wait_until("t4 被慢工作进程领走",
                   lambda: h.task(t4)["state"] == "leased")
        time.sleep(0.4)
        slow.kill9()  # 校验做到一半，进程直接消失
        log("  慢工作进程已崩溃，等待租约到期换手……")
        h.start_worker("worker-4")
        wait_until("t4 换手并送达", lambda: delivered(t4), timeout=20)
        st4 = h.task(t4)
        check("t4 崩溃换手后 delivered", st4["state"] == "delivered")
        check("t4 租约尝试次数为 2", st4["attempt_count"] == 2,
              f"attempt={st4['attempt_count']}")
        check("t4 校验实际执行 2 次（崩溃前一次 + 接手一次）",
              h.validator_run_count(t4) == 2)

        # ========== 4. 名额上限：满额暂停首次领取，其他端照常 ==========
        log("== 场景 4：beta 名额=1，停机制造满额；gamma 任务照常，恢复后续投 ==")
        h.stop_named("worker-4")
        beta_proc = h.stop_named("inbox-beta")
        h.start_worker("worker-5")
        b1 = h.submit("beta", h.good)
        wait_until("b1 被首次领取（占住 beta 唯一名额）",
                   lambda: h.task(b1)["state"] == "succeeded")
        b2 = h.submit("beta", h.good)
        time.sleep(1.5)
        check("beta 满额：b2 暂停首次领取，保持 queued",
              h.task(b2)["state"] == "queued")

        g1 = h.submit("gamma", h.good)
        wait_until("有空闲进程时 gamma 任务照常执行送达",
                   lambda: delivered(g1), timeout=15)
        check("gamma 不受 beta 满额影响", h.task(g1)["state"] == "delivered")

        # beta 恢复：b1 先续投收妥释放名额，随后 b2 首次领取并送达
        h.start_inbox("beta", h.beta_port, h.beta_db)
        wait_until("b1 恢复后续投收妥", lambda: delivered(b1), timeout=15)
        wait_until("b2 获得名额后完成送达", lambda: delivered(b2), timeout=20)
        check("b1/b2 均 delivered",
              h.task(b1)["state"] == "delivered"
              and h.task(b2)["state"] == "delivered")
        beta_results = h.inbox_list(h.beta_url)
        check("beta 每个任务只留一份结果（共 2 份）",
              len(beta_results) == 2
              and {r["task_id"] for r in beta_results} == {b1, b2})

        # ========== 5. 队列重启：已完成结果继续交付，不重新校验 ==========
        log("== 场景 5：收件端 alpha 停机 + 队列重启，结果不丢、不重跑 ==")
        h.stop_named("inbox-alpha")
        t5 = h.submit("alpha", h.good)
        wait_until("t5 已完成校验等待投递（succeeded）",
                   lambda: h.task(t5)["state"] == "succeeded", timeout=15)
        runs_before = h.validator_run_count(t5)
        h.stop_named("queue")
        time.sleep(1.0)
        h.start_queue()  # 同一数据库文件重启
        # 工作进程仍在，重新连上队列；alpha 仍停机，t5 继续保留
        time.sleep(2.0)
        check("队列重启后 t5 仍为 succeeded（无需重新执行校验）",
              h.task(t5)["state"] == "succeeded"
              and h.validator_run_count(t5) == runs_before)

        h.start_inbox("alpha", h.alpha_port, h.alpha_db)
        wait_until("t5 在队列重启+收件端恢复后自动送达",
                   lambda: delivered(t5), timeout=15)
        check("t5 校验始终只执行 1 次",
              h.validator_run_count(t5) == 1)

        # ========== 6. 收件端重启 + 幂等：只保留一份结果 ==========
        log("== 场景 6：收件端重启后结果仍在，重复投递幂等去重 ==")
        h.stop_named("inbox-gamma")
        time.sleep(0.8)
        h.start_inbox("gamma", h.gamma_port, h.gamma_db)
        status, rg = h.inbox_result(h.gamma_url, t3)
        check("gamma 重启后历史结果仍可查（本地持久化）",
              status == 200 and rg["task_id"] == t3)

        # 手动再投一次 t3，验证收件端幂等
        status, body = http(h.gamma_url + "/results", {
            "task_id": t3, "recipient": "gamma",
            "artifact": str(h.good),
            "result": h.task(t3)["result"]}, method="POST")
        check("重复投递返回成功但标记 duplicated",
              status == 200 and body.get("duplicated") is True)
        gamma_results = h.inbox_list(h.gamma_url)
        check("gamma 中 t3 始终只有一份结果",
              len([r for r in gamma_results if r["task_id"] == t3]) == 1)

        # 名额释放验证：alpha 全部 delivered 后 inflight 归零，可继续接单
        _, recs = http(h.queue_url + "/recipients")
        alpha_rec = next(r for r in recs["recipients"] if r["id"] == "alpha")
        check("全部收妥后 alpha 名额释放（inflight=0）",
              alpha_rec["inflight"] == 0, str(alpha_rec))
        t6 = h.submit("alpha", h.good)
        wait_until("名额释放后新任务可正常领取送达",
                   lambda: delivered(t6), timeout=15)

    except AssertionError as exc:
        failed += 1
        log(f"❌ 测试中断: {exc}")
    except Exception as exc:
        failed += 1
        log(f"❌ 意外异常: {type(exc).__name__}: {exc}")
    finally:
        h.teardown()

    log("=" * 60)
    log(f"结果：通过 {passed} 项，失败 {failed} 项，"
        f"总耗时 {time.time() - _t0:.1f}s（要求 < 80s）")
    if failed == 0:
        log("🎉 全部测试通过")
        return 0
    log("💥 存在失败项")
    return 1


if __name__ == "__main__":
    sys.exit(main())
