"""
GitOps deployer driver.

Runs inside a one-shot container launched by the gateway's GitOps reconciler.
The container image only needs git and the beta9 SDK. Everything else comes
from the environment:

  ENDPOINTS_REPO_URL   clone URL
  ENDPOINTS_REPO_SHA   commit to apply
  ENDPOINTS_LAST_SHA   previously applied commit ("" on the first run)
  ENDPOINTS_REPO_REF   branch or tag the SHA came from (fallback fetch)
  ENDPOINTS_REPO_PATH  sub-directory holding the endpoint apps ("" = repo root)
  ENDPOINTS_FORCE      "1" to redeploy every stub regardless of the diff
  ENDPOINTS_REDEPLOY   comma-separated app paths to redeploy even if unchanged
  ENDPOINTS_RUN_ID     opaque id echoed back in the report
  ENDPOINTS_DEPLOY_KEY optional SSH private key or https token
  BETA9_TOKEN, BETA9_GATEWAY_HOST[_HTTP], BETA9_GATEWAY_PORT[_HTTP]

Every directory containing an app.py that exports a ManagedEndpoint or
ManagedService is a managed stub. Each app directory is deployed from its own
working directory so only that directory is uploaded with the stub. The run
ends with one POST to /api/v1/endpoints/gitops/report.
"""

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import traceback
import urllib.error
import urllib.request
from pathlib import Path


def env(name, default=""):
    return os.environ.get(name, default).strip()


REPO_URL = env("ENDPOINTS_REPO_URL")
SHA = env("ENDPOINTS_REPO_SHA")
LAST_SHA = env("ENDPOINTS_LAST_SHA")
REF = env("ENDPOINTS_REPO_REF") or "main"
REPO_PATH = env("ENDPOINTS_REPO_PATH").strip("/")
FORCE = env("ENDPOINTS_FORCE") == "1"
REDEPLOY = {p.strip("/") for p in env("ENDPOINTS_REDEPLOY").split(",") if p.strip()}
RUN_ID = env("ENDPOINTS_RUN_ID")
DEPLOY_KEY = os.environ.get("ENDPOINTS_DEPLOY_KEY", "")
WORKDIR = Path(tempfile.mkdtemp(prefix="endpoints-"))


def log(msg):
    print(f"[gitops] {msg}", flush=True)


def git_env():
    e = dict(os.environ)
    e["GIT_TERMINAL_PROMPT"] = "0"
    if DEPLOY_KEY.startswith("-----BEGIN"):
        key_path = WORKDIR / "deploy_key"
        key_path.write_text(DEPLOY_KEY if DEPLOY_KEY.endswith("\n") else DEPLOY_KEY + "\n")
        key_path.chmod(0o600)
        e["GIT_SSH_COMMAND"] = (
            f"ssh -i {key_path} -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new"
        )
    return e


def clone_url():
    if DEPLOY_KEY and not DEPLOY_KEY.startswith("-----BEGIN") and REPO_URL.startswith("https://"):
        return REPO_URL.replace("https://", f"https://x-access-token:{DEPLOY_KEY}@", 1)
    return REPO_URL


def git(*args, check=True, cwd=None):
    return subprocess.run(
        ["git", *args],
        cwd=str(cwd or WORKDIR / "repo"),
        env=git_env(),
        check=check,
        text=True,
        capture_output=True,
    )


def checkout():
    repo = WORKDIR / "repo"
    repo.mkdir(parents=True)
    git("init", "-q", cwd=repo)
    git("remote", "add", "origin", clone_url(), cwd=repo)
    try:
        git("fetch", "-q", "--depth", "1", "origin", SHA, cwd=repo)
    except subprocess.CalledProcessError as exc:
        # Servers that refuse fetch-by-sha: take the ref head and insist it
        # is the commit we were asked to apply.
        log(f"fetch by sha refused ({exc.stderr.strip()}); fetching {REF}")
        git("fetch", "-q", "--depth", "1", "origin", REF, cwd=repo)
        head = git("rev-parse", "FETCH_HEAD", cwd=repo).stdout.strip()
        if not head.startswith(SHA):
            raise RuntimeError(f"ref {REF} is at {head[:8]}, expected {SHA[:8]}")
    git("checkout", "-q", "--detach", "FETCH_HEAD", cwd=repo)
    return repo


def changed_paths(repo):
    """Return the set of changed file paths since LAST_SHA, or None for 'everything'."""
    if FORCE or not LAST_SHA or LAST_SHA == SHA:
        return None
    try:
        git("fetch", "-q", "--depth", "1", "origin", LAST_SHA, cwd=repo)
        out = git("diff", "--name-only", LAST_SHA, SHA, cwd=repo).stdout
    except subprocess.CalledProcessError as exc:
        log(f"diff against {LAST_SHA[:8]} unavailable ({exc.stderr.strip()}); redeploying all")
        return None
    return {line.strip() for line in out.splitlines() if line.strip()}


def discover(root):
    apps = []
    for app in sorted(root.rglob("app.py")):
        rel = app.parent.relative_to(root)
        if any(
            part.startswith(".") or part in {"__pycache__", "node_modules"} for part in rel.parts
        ):
            continue
        apps.append(app)
    return apps


def app_changed(rel_dir, changed, app_dirs):
    if changed is None:
        return True
    rel = str(rel_dir).strip("./")
    if rel in REDEPLOY:
        return True
    prefix = f"{REPO_PATH}/" if REPO_PATH else ""
    for path in changed:
        if not path.startswith(prefix):
            # A change outside the endpoints tree (shared tooling) redeploys everything.
            return True
        inner = path[len(prefix) :]
        if rel and inner.startswith(rel + "/"):
            return True
        if not any(inner.startswith(d + "/") for d in app_dirs if d):
            # Shared file inside the endpoints tree.
            return True
    return False


def load_module(app_path):
    name = "endpoint_app_" + str(abs(hash(str(app_path))))
    spec = importlib.util.spec_from_file_location(name, app_path)
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(app_path.parent))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    return module


def report_url():
    host = env("BETA9_GATEWAY_HOST_HTTP") or env("BETA9_GATEWAY_HOST")
    port = env("BETA9_GATEWAY_PORT_HTTP") or "1994"
    scheme = "https" if port == "443" else "http"
    netloc = host if port in {"80", "443"} else f"{host}:{port}"
    return f"{scheme}://{netloc}/api/v1/endpoints/gitops/report"


def post_report(report):
    body = json.dumps(report).encode()
    req = urllib.request.Request(
        report_url(),
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {env('BETA9_TOKEN')}",
        },
    )
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                log(f"report accepted ({resp.status})")
                return True
        except urllib.error.HTTPError as exc:
            log(f"report rejected: {exc.code} {exc.read().decode(errors='replace')}")
            if 400 <= exc.code < 500:
                return False
        except Exception as exc:  # noqa: BLE001
            log(f"report failed: {exc}")
        import time

        time.sleep(2 * (attempt + 1))
    return False


def main():
    report = {"run_id": RUN_ID, "sha": SHA, "discovered": [], "results": [], "error": ""}
    try:
        from beta9 import ManagedEndpoint, ManagedService

        repo = checkout()
        root = repo / REPO_PATH if REPO_PATH else repo
        if not root.is_dir():
            raise RuntimeError(f"endpoints path {REPO_PATH!r} not found at {SHA[:8]}")

        apps = discover(root)
        app_dirs = [str(a.parent.relative_to(root)) for a in apps]
        changed = changed_paths(repo)
        log(
            f"{len(apps)} app(s) at {SHA[:8]}; changed={'all' if changed is None else len(changed)}"
        )

        for app in apps:
            rel_dir = app.parent.relative_to(root)
            rel = str(rel_dir)
            os.chdir(app.parent)
            try:
                module = load_module(app)
            except Exception as exc:  # noqa: BLE001
                traceback.print_exc()
                report["results"].append(
                    {
                        "path": rel,
                        "id": "",
                        "kind": "",
                        "ok": False,
                        "error": f"import failed: {exc}",
                    }
                )
                continue

            objs = [
                v for v in vars(module).values() if isinstance(v, (ManagedEndpoint, ManagedService))
            ]
            for obj in objs:
                kind = "service" if isinstance(obj, ManagedService) else "endpoint"
                ident = obj.spec_name
                report["discovered"].append({"path": rel, "id": ident, "kind": kind})
                result = {"path": rel, "id": ident, "kind": kind, "ok": False, "skipped": False}
                if not app_changed(rel_dir, changed, app_dirs):
                    result.update(ok=True, skipped=True)
                    report["results"].append(result)
                    continue
                log(f"deploying {kind} {ident} from {rel}")
                try:
                    out, ok = obj.deploy(git_sha=SHA)
                    result.update(
                        ok=bool(ok),
                        stub_id=getattr(obj, "stub_id", "") or "",
                        version=int(out.get("version") or 0) if isinstance(out, dict) else 0,
                    )
                    if not ok:
                        result["error"] = "deploy failed"
                except SystemExit as exc:
                    result["error"] = f"deploy exited: {exc}"
                except Exception as exc:  # noqa: BLE001
                    traceback.print_exc()
                    result["error"] = str(exc)
                report["results"].append(result)
    except Exception as exc:  # noqa: BLE001
        traceback.print_exc()
        report["error"] = str(exc)
    finally:
        os.chdir("/")
        shutil.rmtree(WORKDIR, ignore_errors=True)

    ok = post_report(report)
    failed = [r for r in report["results"] if not r["ok"]]
    log(
        f"done: {len(report['results'])} result(s), {len(failed)} failed, report={'sent' if ok else 'lost'}"
    )
    sys.exit(0 if ok and not failed and not report["error"] else 1)


if __name__ == "__main__":
    main()
