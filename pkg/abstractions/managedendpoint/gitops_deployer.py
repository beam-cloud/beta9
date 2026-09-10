"""
GitOps deployer: a one-shot container (git + the beta9 SDK) launched by the
gateway. Configured by ENDPOINTS_* env (see gitops.go launch): it checks out
REPO_SHA, deploys every app.py under REPO_PATH that changed since LAST_SHA (or
everything with FORCE, plus REDEPLOY paths), and POSTs one report with the
results and the raw config.yaml to /api/v1/endpoints/gitops/report.
"""

import fnmatch
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.error
import urllib.request
from pathlib import Path, PurePosixPath


def env(name, default=""):
    return os.environ.get(name, default).strip()


REPO_URL = env("ENDPOINTS_REPO_URL")
SHA = env("ENDPOINTS_REPO_SHA")
LAST_SHA = env("ENDPOINTS_LAST_SHA")
BRANCH = env("ENDPOINTS_REPO_BRANCH") or "main"
REPO_PATH = env("ENDPOINTS_REPO_PATH").strip("/")
FORCE = env("ENDPOINTS_FORCE") == "1"
REDEPLOY = {p.strip("/") for p in env("ENDPOINTS_REDEPLOY").split(",") if p.strip()}
RUN_ID = env("ENDPOINTS_RUN_ID")
DEPLOY_KEY = os.environ.get("ENDPOINTS_DEPLOY_KEY", "")
SSH_KEY = DEPLOY_KEY.startswith("-----BEGIN")
WORKDIR = Path(tempfile.mkdtemp(prefix="endpoints-"))
REPO = WORKDIR / "repo"
GIT_ENV = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
if SSH_KEY:
    KEY_PATH = WORKDIR / "deploy_key"
    KEY_PATH.write_text(DEPLOY_KEY.rstrip("\n") + "\n")
    KEY_PATH.chmod(0o600)
    SSH_OPTS = "-o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new"
    GIT_ENV["GIT_SSH_COMMAND"] = f"ssh -i {KEY_PATH} {SSH_OPTS}"


def log(msg):
    print(f"[gitops] {msg}", flush=True)


def git(*args):
    return subprocess.run(
        ["git", *args],
        cwd=str(REPO),
        env=GIT_ENV,
        check=True,
        text=True,
        capture_output=True,
    )


def checkout():
    url = REPO_URL
    if DEPLOY_KEY and not SSH_KEY and url.startswith("https://"):
        url = url.replace("https://", f"https://x-access-token:{DEPLOY_KEY}@", 1)
    REPO.mkdir(parents=True)
    git("init", "-q")
    git("remote", "add", "origin", url)
    try:
        git("fetch", "-q", "--depth", "1", "origin", SHA)
    except subprocess.CalledProcessError as exc:
        # Servers that refuse fetch-by-sha: take the ref head and insist it is our commit.
        log(f"fetch by sha refused ({exc.stderr.strip()}); fetching {BRANCH}")
        git("fetch", "-q", "--depth", "1", "origin", "refs/heads/" + BRANCH)
        head = git("rev-parse", "FETCH_HEAD").stdout.strip()
        if head != SHA:
            raise RuntimeError(f"ref {BRANCH} is at {head[:8]}, expected {SHA[:8]}")
    git("checkout", "-q", "--detach", "FETCH_HEAD")


def changed_paths():
    """Set of files changed since LAST_SHA, or None for 'everything'."""
    if FORCE or not LAST_SHA:
        return None
    if LAST_SHA == SHA:
        return set()  # retry run: only REDEPLOY paths are redeployed
    try:
        git("fetch", "-q", "--depth", "1", "origin", LAST_SHA)
        out = git("diff", "--name-only", LAST_SHA, SHA).stdout
    except subprocess.CalledProcessError as exc:
        log(f"diff against {LAST_SHA[:8]} unavailable ({exc.stderr.strip()}); redeploying all")
        return None
    paths = {line.strip() for line in out.splitlines() if line.strip()}
    return {path for path in paths if not ignored_change(path)}


def ignored_change(path):
    """Repository-root globs for files that cannot affect deployed models.

    Model directories remain authoritative: ignore rules cannot suppress an
    endpoint edit. Image build sources may be ignored when apps pin images.
    """
    prefix = f"{REPO_PATH}/" if REPO_PATH else ""
    if path.startswith(prefix):
        return False
    if path == ".gitopsignore":
        return True
    ignore = REPO / ".gitopsignore"
    if not ignore.is_file():
        return False
    patterns = [line.strip() for line in ignore.read_text().splitlines()]
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns if pattern and not pattern.startswith("#"))


def is_app(path):
    skip = {"__pycache__", "node_modules"}
    return path.name == "app.py" and not any(
        p.startswith(".") or p in skip for p in path.parent.parts
    )


def discover(root):
    return [app for app in sorted(root.rglob("app.py")) if is_app(app.relative_to(root))]


def app_directories(root, apps, changed):
    """Keep deleted endpoint files scoped to their old app, using both Git trees."""
    dirs = {"" if app.parent == root else str(app.parent.relative_to(root)) for app in apps}
    if not changed or not LAST_SHA or LAST_SHA == SHA:
        return dirs
    # changed_paths fetched LAST_SHA before diffing. A failed tree read aborts
    # this sync instead of guessing that deleted endpoint files are shared code.
    tree = git("ls-tree", "-r", "--name-only", "-z", LAST_SHA, "--", REPO_PATH or ".").stdout
    prefix = f"{REPO_PATH}/" if REPO_PATH else ""
    for path in tree.split("\0"):
        if not path.startswith(prefix):
            continue
        app = PurePosixPath(path[len(prefix) :])
        if is_app(app):
            dirs.add("" if app.parent == PurePosixPath(".") else str(app.parent))
    return dirs


def app_changed(rel, changed, app_dirs):
    """A change outside the endpoints tree or in a shared file redeploys everything."""
    if changed is None or rel in REDEPLOY:
        return True
    prefix = f"{REPO_PATH}/" if REPO_PATH else ""
    for path in changed:
        if path == "config.yaml":
            continue  # placement only; the gateway applies it without a redeploy
        if not path.startswith(prefix):
            return True
        inner = path[len(prefix) :]
        # The deepest app owns its files when endpoint directories are nested.
        owners = [d for d in app_dirs if not d or inner.startswith(d + "/")]
        if not owners or max(owners, key=len) == rel:
            return True
    return False


_LOADED = 0


def _in_repo(module):
    origin = getattr(module, "__file__", None) or next(iter(getattr(module, "__path__", None) or []), None)
    return bool(origin) and Path(origin).resolve().is_relative_to(REPO.resolve())


def load_module(app_path):
    """Execute app.py as a throwaway module with its directory on sys.path.

    Every repo module it imported is evicted afterwards, so a same-named helper
    in another app directory gets its own copy instead of this one's.
    """
    global _LOADED
    _LOADED += 1
    name = f"endpoint_app_{_LOADED}"
    spec = importlib.util.spec_from_file_location(name, app_path)
    module = importlib.util.module_from_spec(spec)
    before_modules, before_path = set(sys.modules), list(sys.path)
    sys.path.insert(0, str(app_path.parent))
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path[:] = before_path
        for added in set(sys.modules) - before_modules:
            if added == name or _in_repo(sys.modules[added]):
                sys.modules.pop(added, None)
    return module


def post_report(report):
    host = env("BETA9_GATEWAY_HOST_HTTP") or env("BETA9_GATEWAY_HOST")
    port = env("BETA9_GATEWAY_PORT_HTTP") or "1994"
    netloc = host if port in {"80", "443"} else f"{host}:{port}"
    url = f"{'https' if port == '443' else 'http'}://{netloc}/api/v1/endpoints/gitops/report"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {env('BETA9_TOKEN')}",
    }
    body = json.dumps(report).encode()
    req = urllib.request.Request(url, data=body, method="POST", headers=headers)
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
        time.sleep(2 * (attempt + 1))
    return False


def load_config():
    """Raw config.yaml from the repo root (the gateway parses and validates it)."""
    path = REPO / "config.yaml"
    if not path.is_file():
        raise RuntimeError("config.yaml is required at the repository root")
    return path.read_text()


def deploy_app(app, root, changed, app_dirs, report):
    from beta9 import ManagedEndpoint

    rel = str(app.parent.relative_to(root))
    os.chdir(app.parent)
    try:
        module = load_module(app)
    except (Exception, SystemExit) as exc:  # noqa: BLE001  (an app may sys.exit() at import)
        traceback.print_exc()
        detail = f"exited with {exc.code}" if isinstance(exc, SystemExit) else str(exc)
        report["results"].append({"path": rel, "id": "", "ok": False, "error": f"import failed: {detail}"})
        return
    for obj in list(vars(module).values()):
        if not isinstance(obj, ManagedEndpoint):
            continue
        result = {"path": rel, "id": obj.id, "ok": False, "skipped": False}
        report["results"].append(result)
        if not app_changed(rel.strip("./"), changed, app_dirs):
            result.update(ok=True, skipped=True)
            continue
        log(f"deploying {obj.id} from {rel}")
        try:
            out, ok = obj.deploy(git_sha=SHA)
            version = int(out.get("version") or 0)
            result.update(ok=bool(ok), stub_id=obj.stub_id or "", version=version)
            if not ok:
                result["error"] = getattr(obj, "deploy_error", "") or "deploy failed"
        except SystemExit as exc:
            result["error"] = f"deploy exited: {exc}"
        except Exception as exc:  # noqa: BLE001
            traceback.print_exc()
            result["error"] = str(exc)


def main():
    report = {"run_id": RUN_ID, "sha": SHA, "results": [], "error": ""}
    try:
        checkout()
        report["fleet_yaml"] = load_config()
        root = REPO / REPO_PATH if REPO_PATH else REPO
        if not root.is_dir():
            raise RuntimeError(f"endpoints path {REPO_PATH!r} not found at {SHA[:8]}")
        apps = discover(root)
        changed = changed_paths()
        app_dirs = app_directories(root, apps, changed)
        n_changed = "all" if changed is None else len(changed)
        log(f"{len(apps)} app(s) at {SHA[:8]}; changed={n_changed}")
        for app in apps:
            deploy_app(app, root, changed, app_dirs, report)
    except (Exception, SystemExit) as exc:  # noqa: BLE001  (the report must always be posted)
        traceback.print_exc()
        report["error"] = f"exited with {exc.code}" if isinstance(exc, SystemExit) else str(exc)
    finally:
        os.chdir("/")
        shutil.rmtree(WORKDIR, ignore_errors=True)

    sent = post_report(report)
    failed = [r for r in report["results"] if not r["ok"]]
    status = "sent" if sent else "lost"
    log(f"done: {len(report['results'])} result(s), {len(failed)} failed, report={status}")
    sys.exit(0 if sent and not failed and not report["error"] else 1)


if __name__ == "__main__":
    main()
