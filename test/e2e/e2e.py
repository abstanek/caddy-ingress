#!/usr/bin/env python3
"""End-to-end smoke test for the Caddy ingress controller.

Creates a kind cluster, builds and loads the controller image, installs the
helm chart, deploys a sample backend + Ingress, and verifies a real HTTP
request routes through the controller to the backend. On-demand TLS is
deliberately not exercised.

Requires: go, docker, kind, kubectl, helm, python3. Stdlib only.
"""

from __future__ import annotations

import argparse
import atexit
import contextlib
import os
import shutil
import signal
import socket
import ssl
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
CHART = REPO / "charts" / "caddy-ingress-controller"
MANIFESTS = HERE / "manifests" / "app.yaml"

IMAGE = "caddy/ingress:e2e"
NAMESPACE = "caddy-system"
RELEASE = "caddy-e2e"
HOST_HEADER = "e2e.localhost"
EXPECTED_BODY = "e2e-ok"

_cleanup_fns: list = []


def log(msg: str) -> None:
    print(f"[e2e] {msg}", flush=True)


def run(cmd: list[str], **kw) -> subprocess.CompletedProcess:
    log("$ " + " ".join(cmd))
    return subprocess.run(cmd, check=True, **kw)


def run_capture(cmd: list[str]) -> str:
    return subprocess.run(
        cmd, check=True, stdout=subprocess.PIPE, text=True
    ).stdout.strip()


def require_tools(tools: list[str]) -> None:
    missing = [t for t in tools if shutil.which(t) is None]
    if missing:
        sys.exit(f"missing required tools: {', '.join(missing)}")


def cluster_exists(name: str) -> bool:
    out = run_capture(["kind", "get", "clusters"])
    return name in out.splitlines()


def pull_and_retag(source: str) -> None:
    # Accepts either a tag (repo:tag) or a digest (repo@sha256:...). We pull it
    # and re-tag to the canonical local name so kind load + helm install work
    # unchanged regardless of the reference form.
    log(f"pulling pre-built image {source}")
    run(["docker", "pull", source])
    run(["docker", "tag", source, IMAGE])


def build_image() -> None:
    log("building controller binary")
    env = {**os.environ, "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": "amd64"}
    bin_path = REPO / "bin" / "ingress-controller"
    bin_path.parent.mkdir(exist_ok=True)
    subprocess.run(
        ["go", "build", "-o", str(bin_path), "./cmd/caddy"],
        cwd=REPO, env=env, check=True,
    )
    # The existing Dockerfile expects ./ingress-controller next to it.
    staged = REPO / "ingress-controller"
    shutil.copy2(bin_path, staged)
    try:
        run(["docker", "build", "-t", IMAGE, "-f", str(REPO / "Dockerfile"), str(REPO)])
    finally:
        with contextlib.suppress(FileNotFoundError):
            staged.unlink()


def create_cluster(name: str) -> None:
    if cluster_exists(name):
        log(f"reusing existing kind cluster {name!r}")
        return
    run(["kind", "create", "cluster",
         "--name", name,
         "--config", str(HERE / "kind-config.yaml"),
         "--wait", "120s"])


def load_image(name: str) -> None:
    run(["kind", "load", "docker-image", IMAGE, "--name", name])


def helm_install() -> None:
    run(["kubectl", "create", "namespace", NAMESPACE,
         "--dry-run=client", "-o", "yaml"], stdout=subprocess.PIPE)
    subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        input=f"apiVersion: v1\nkind: Namespace\nmetadata:\n  name: {NAMESPACE}\n",
        text=True, check=True,
    )
    run([
        "helm", "upgrade", "--install", RELEASE, str(CHART),
        "--namespace", NAMESPACE,
        "--set", f"image.repository={IMAGE.split(':')[0]}",
        "--set", f"image.tag={IMAGE.split(':')[1]}",
        "--set", "image.pullPolicy=IfNotPresent",
        "--set", "replicaCount=1",
        "--set", "loadBalancer.enabled=false",
        "--set", "service.type=NodePort",
        "--set", "ingressController.verbose=true",
        "--set", "podDisruptionBudget.minAvailable=null",
        "--wait", "--timeout", "180s",
    ])


def deploy_app() -> None:
    run(["kubectl", "apply", "-f", str(MANIFESTS)])
    run(["kubectl", "wait", "--for=condition=available",
         "deployment/e2e-echo", "--timeout=120s"])


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_port_forward(svc: str, port: int, target: int = 443) -> subprocess.Popen:
    log(f"port-forwarding svc/{svc}:{target} -> 127.0.0.1:{port}")
    proc = subprocess.Popen(
        ["kubectl", "-n", NAMESPACE, "port-forward",
         f"svc/{svc}", f"{port}:{target}"],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )
    _cleanup_fns.append(lambda: _kill(proc))
    # Wait for the local port to accept connections.
    deadline = time.time() + 30
    while time.time() < deadline:
        if proc.poll() is not None:
            err = proc.stderr.read().decode() if proc.stderr else ""
            raise RuntimeError(f"port-forward exited early: {err}")
        with contextlib.suppress(OSError):
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return proc
        time.sleep(0.5)
    raise RuntimeError("port-forward did not become ready in time")


def _kill(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=5)


def controller_service() -> str:
    return run_capture([
        "kubectl", "-n", NAMESPACE, "get", "svc",
        "-l", f"app.kubernetes.io/instance={RELEASE}",
        "-o", "jsonpath={.items[0].metadata.name}",
    ])


def _https_request(port: int) -> tuple[int, str]:
    # Connect TCP to 127.0.0.1 but use the Ingress hostname for SNI so Caddy
    # routes to the right site. Skip cert verification — Caddy serves its
    # internal CA cert here.
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    sock = socket.create_connection(("127.0.0.1", port), timeout=5)
    try:
        ssock = ctx.wrap_socket(sock, server_hostname=HOST_HEADER)
        try:
            req = (
                f"GET / HTTP/1.1\r\nHost: {HOST_HEADER}\r\n"
                "Connection: close\r\nUser-Agent: caddy-e2e\r\n\r\n"
            )
            ssock.sendall(req.encode())
            buf = b""
            while True:
                chunk = ssock.recv(4096)
                if not chunk:
                    break
                buf += chunk
        finally:
            ssock.close()
    finally:
        sock.close()
    head, _, body = buf.partition(b"\r\n\r\n")
    status_line = head.split(b"\r\n", 1)[0].decode(errors="replace")
    status = int(status_line.split(" ", 2)[1])
    return status, body.decode(errors="replace").strip()


def probe_ingress(port: int) -> None:
    last_err: Exception | None = None
    deadline = time.time() + 90
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        try:
            status, body = _https_request(port)
            log(f"attempt {attempt}: HTTP {status} body={body!r}")
            if status == 200 and EXPECTED_BODY in body:
                log("ingress traffic verified")
                return
            last_err = RuntimeError(f"unexpected response: status={status} body={body!r}")
        except (OSError, ssl.SSLError, ValueError) as e:
            last_err = e
            log(f"attempt {attempt}: {e}")
        time.sleep(2)
    raise SystemExit(f"ingress probe failed after {attempt} attempts: {last_err}")


def dump_diagnostics() -> None:
    log("=== diagnostics ===")
    for cmd in (
        ["kubectl", "get", "pods", "-A"],
        ["kubectl", "-n", NAMESPACE, "describe", "pods"],
        ["kubectl", "-n", NAMESPACE, "logs", "-l",
         f"app.kubernetes.io/instance={RELEASE}", "--tail=200"],
        ["kubectl", "get", "ingress", "-A"],
    ):
        with contextlib.suppress(subprocess.CalledProcessError):
            subprocess.run(cmd)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cluster-name", default="caddy-ingress-e2e")
    ap.add_argument("--keep", action="store_true",
                    help="do not delete the kind cluster on exit")
    ap.add_argument("--skip-build", action="store_true",
                    help="skip building the controller image (assumes IMAGE already exists)")
    ap.add_argument("--image", default="",
                    help="test a pre-built image (tag or repo@sha256 digest) instead of "
                         "building from source; it is pulled and loaded into the cluster")
    args = ap.parse_args()

    building = not args.image and not args.skip_build
    tools = ["docker", "kind", "kubectl", "helm"]
    if building:
        tools.insert(0, "go")
    require_tools(tools)

    def cleanup():
        for fn in reversed(_cleanup_fns):
            with contextlib.suppress(Exception):
                fn()
        if not args.keep and cluster_exists(args.cluster_name):
            with contextlib.suppress(subprocess.CalledProcessError):
                run(["kind", "delete", "cluster", "--name", args.cluster_name])

    atexit.register(cleanup)

    if args.image:
        pull_and_retag(args.image)
    elif not args.skip_build:
        build_image()
    create_cluster(args.cluster_name)
    load_image(args.cluster_name)
    helm_install()
    deploy_app()

    svc = controller_service()
    port = free_port()
    try:
        start_port_forward(svc, port)
        probe_ingress(port)
    except Exception:
        dump_diagnostics()
        raise

    log("PASS")


if __name__ == "__main__":
    main()
