"""
ATLAS — Shared Docker Exec Helper
===================================
Provides a robust _docker_exec() utility used by all ATLAS DAGs.

Why not DockerOperator?
  Installing the python `docker` package breaks our Airflow image permissions.
  Instead we call the Docker Unix socket directly via `curl` subprocess calls.

Why detached + polling instead of attached streams?
  Attached mode keeps an HTTP connection open for the ENTIRE job duration.
  For long Spark jobs (10-30 min) this caused the BashOperator to appear
  hung and eventually get killed by Airflow's task timeout — landing in
  "up for retry" even though Spark was still running.
  Detached mode starts the job and returns immediately; we poll every 15s.
"""

import json
import logging
import subprocess
import time

log = logging.getLogger(__name__)

DOCKER_SOCK = "/var/run/docker.sock"
_CURL_BASE = ["curl", "-sf", "--unix-socket", DOCKER_SOCK]
_CT_JSON = ["-H", "Content-Type: application/json"]


def _docker_exec(container: str, cmd: list, timeout_s: int = 3600) -> int:
    """
    Execute `cmd` inside `container` via the Docker Unix socket.

    Steps:
      1. POST /containers/{container}/exec  → creates exec, gets exec_id
      2. POST /exec/{exec_id}/start         → starts detached (returns 204 instantly)
      3. Poll GET /exec/{exec_id}/json      → wait until Running=false & ExitCode is set

    Returns:
      int — exit code of the process (0 = success)

    Raises:
      RuntimeError — if create/start fails, or timeout_s exceeded
    """
    # ── 1. Create exec instance ─────────────────────────────────────────────
    payload = json.dumps({"Cmd": cmd, "AttachStdout": False, "AttachStderr": False})
    create = subprocess.run(
        _CURL_BASE + ["-X", "POST"] + _CT_JSON + [
            "-d", payload,
            f"http://localhost/containers/{container}/exec",
        ],
        capture_output=True, text=True,
    )
    if create.returncode != 0 or not create.stdout.strip():
        raise RuntimeError(
            f"[docker_exec] Create failed for {container}:{cmd!r}\n"
            f"  stdout={create.stdout!r}  stderr={create.stderr!r}"
        )

    try:
        exec_id = json.loads(create.stdout)["Id"]
    except (json.JSONDecodeError, KeyError) as exc:
        raise RuntimeError(
            f"[docker_exec] Could not parse exec Id from response: {create.stdout!r}"
        ) from exc

    log.info("[docker_exec] Created exec %s in container %s", exec_id[:12], container)

    # ── 2. Start detached ───────────────────────────────────────────────────
    start = subprocess.run(
        _CURL_BASE + ["-X", "POST"] + _CT_JSON + [
            "-d", '{"Detach":true}',
            f"http://localhost/exec/{exec_id}/start",
        ],
        capture_output=True, text=True,
    )
    # HTTP 204 = started OK; curl -sf returns 0 on 2xx/3xx
    log.info(
        "[docker_exec] Started exec %s (detached). Polling every 15s (timeout=%ds)...",
        exec_id[:12], timeout_s,
    )

    # ── 3. Poll until done ──────────────────────────────────────────────────
    deadline = time.time() + timeout_s
    poll_interval = 15  # seconds

    while time.time() < deadline:
        time.sleep(poll_interval)
        inspect = subprocess.run(
            _CURL_BASE + [f"http://localhost/exec/{exec_id}/json"],
            capture_output=True, text=True,
        )
        if inspect.returncode != 0:
            log.warning("[docker_exec] Inspect poll failed — retrying: %s", inspect.stderr)
            continue

        try:
            info = json.loads(inspect.stdout)
        except json.JSONDecodeError:
            log.warning("[docker_exec] Non-JSON inspect response — retrying")
            continue

        running = info.get("Running", True)
        exit_code = info.get("ExitCode")

        if not running and exit_code is not None:
            log.info(
                "[docker_exec] Exec %s finished in container %s with exit_code=%d",
                exec_id[:12], container, exit_code,
            )
            return exit_code

        log.debug("[docker_exec] Still running — Running=%s ExitCode=%s", running, exit_code)

    raise RuntimeError(
        f"[docker_exec] Timed out after {timeout_s}s waiting for exec "
        f"{exec_id[:12]} in container {container}"
    )


def docker_exec_or_raise(container: str, cmd: list, timeout_s: int = 3600):
    """Wrapper: calls _docker_exec and raises RuntimeError if exit_code != 0."""
    exit_code = _docker_exec(container, cmd, timeout_s=timeout_s)
    if exit_code != 0:
        raise RuntimeError(
            f"[docker_exec] Command {cmd!r} in {container} "
            f"exited with non-zero code {exit_code}"
        )
