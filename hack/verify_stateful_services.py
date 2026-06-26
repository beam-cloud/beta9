#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path


APP_PY = r'''
import os
import time
from typing import Dict

import psycopg
import redis
from fastapi import FastAPI, HTTPException

app = FastAPI()


def retry(fn, attempts: int = 30, delay: float = 1.0):
    last = None
    for _ in range(attempts):
        try:
            return fn()
        except Exception as exc:
            last = exc
            time.sleep(delay)
    raise last


def pg_value(key: str, value: str = "") -> str:
    def op():
        with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
            with conn.cursor() as cur:
                cur.execute("create table if not exists stateful_verify (k text primary key, v text not null)")
                if value:
                    cur.execute(
                        "insert into stateful_verify (k, v) values (%s, %s) "
                        "on conflict (k) do update set v = excluded.v",
                        (key, value),
                    )
                cur.execute("select v from stateful_verify where k = %s", (key,))
                row = cur.fetchone()
                if not row:
                    raise KeyError(key)
                return row[0]
    return retry(op)


def redis_client():
    return redis.from_url(
        os.environ["REDIS_URL"],
        decode_responses=True,
        ssl_cert_reqs=None,
        ssl_check_hostname=False,
    )


def redis_value(key: str, value: str = "") -> str:
    def op():
        client = redis_client()
        if value:
            client.set(key, value)
        result = client.get(key)
        if result is None:
            raise KeyError(key)
        return result
    return retry(op)


@app.get("/write")
def write(key: str, value: str) -> Dict[str, str]:
    pg = pg_value(key, value)
    rd = redis_value(key, value)
    if pg != value or rd != value:
        raise HTTPException(status_code=500, detail={"postgres": pg, "redis": rd})
    return {"key": key, "postgres": pg, "redis": rd}


@app.get("/read")
def read(key: str, expected: str) -> Dict[str, str]:
    pg = pg_value(key)
    rd = redis_value(key)
    if pg != expected or rd != expected:
        raise HTTPException(status_code=500, detail={"postgres": pg, "redis": rd})
    return {"key": key, "postgres": pg, "redis": rd}
'''


DOCKERFILE = """\
FROM python:3.11-slim
RUN pip install --no-cache-dir fastapi uvicorn "psycopg[binary]" redis
WORKDIR /app
COPY app.py /app/app.py
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
"""


def run(cmd, cwd=None, check=True):
    print("+", " ".join(cmd), flush=True)
    proc = subprocess.run(
        cmd,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if proc.stdout:
        print(proc.stdout, end="")
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed with exit code {proc.returncode}: {' '.join(cmd)}")
    return proc.stdout


def parse_last_json(output: str):
    for index in range(len(output) - 1, -1, -1):
        if output[index] not in "[{":
            continue
        try:
            return json.loads(output[index:])
        except json.JSONDecodeError:
            continue
    raise ValueError("no JSON object found in command output")


def wait_http(url: str, timeout: int):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=20) as response:
                body = response.read().decode()
                if response.status == 200:
                    print(body)
                    return json.loads(body)
        except Exception as exc:
            last = exc
            time.sleep(2)
    raise RuntimeError(f"timed out waiting for {url}: {last}")


def cli(args, context):
    if context:
        args = args + ["--context", context]
    return args


def main():
    parser = argparse.ArgumentParser(description="Verify local serverless Postgres + Redis persistence.")
    parser.add_argument("--cli", default=os.environ.get("BETA9_CLI", "beta9"))
    parser.add_argument("--context", default=os.environ.get("BETA9_CONTEXT", ""))
    parser.add_argument("--prefix", default=f"stateful-{int(time.time())}")
    parser.add_argument("--pool", default="")
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--keep", action="store_true", help="Do not delete created resources.")
    args = parser.parse_args()

    cli_bin = shutil.which(args.cli)
    if not cli_bin:
        raise SystemExit(f"CLI {args.cli!r} not found")

    pg_name = f"{args.prefix}-pg"
    redis_name = f"{args.prefix}-redis"
    app_name = f"{args.prefix}-app"
    key = f"{args.prefix}-key"
    value = f"value-{int(time.time())}"

    created = {"postgres": pg_name, "redis": redis_name, "app": app_name, "app_id": ""}

    with tempfile.TemporaryDirectory(prefix="beta9-stateful-") as tmp:
        root = Path(tmp)
        (root / "app.py").write_text(APP_PY)
        (root / "Dockerfile").write_text(DOCKERFILE)

        common_db = ["--disk-driver", "dev", "--format", "json", "--min-replicas", "0"]
        if args.pool:
            common_db += ["--pool", args.pool]

        try:
            parse_last_json(
                run(
                    cli(
                        [
                            cli_bin,
                            "postgres",
                            "create",
                            pg_name,
                            "--database",
                            "app",
                            "--username",
                            "app",
                            "--size",
                            "1Gi",
                            *common_db,
                        ],
                        args.context,
                    )
                )
            )
            parse_last_json(
                run(
                    cli(
                        [
                            cli_bin,
                            "redis",
                            "create",
                            redis_name,
                            "--username",
                            "app",
                            "--size",
                            "1Gi",
                            *common_db,
                        ],
                        args.context,
                    )
                )
            )

            app_out = parse_last_json(
                run(
                    cli(
                        [
                            cli_bin,
                            "deployment",
                            "create",
                            "--name",
                            app_name,
                            "--dockerfile",
                            "Dockerfile",
                            "--ports",
                            "8000",
                            "--format",
                            "json",
                        ],
                        args.context,
                    ),
                    cwd=root,
                )
            )
            created["app_id"] = app_out["deployment_id"]
            app_url = app_out["invoke_url"].rstrip("/")

            run(
                cli(
                    [
                        cli_bin,
                        "service",
                        "bind",
                        app_name,
                        "--postgres",
                        pg_name,
                        "--redis",
                        redis_name,
                    ],
                    args.context,
                )
            )

            run(cli([cli_bin, "deployment", "scale", created["app_id"], "--replicas", "0"], args.context))
            run(cli([cli_bin, "deployment", "scale", created["app_id"], "--replicas", "1"], args.context))

            write_url = f"{app_url}/write?key={key}&value={value}"
            read_url = f"{app_url}/read?key={key}&expected={value}"
            wait_http(write_url, args.timeout)
            wait_http(read_url, args.timeout)

            for kind, name in (("postgres", pg_name), ("redis", redis_name)):
                run(cli([cli_bin, kind, "scale", name, "--replicas", "0"], args.context))
            run(cli([cli_bin, "deployment", "scale", created["app_id"], "--replicas", "0"], args.context))

            for kind, name in (("postgres", pg_name), ("redis", redis_name)):
                run(cli([cli_bin, kind, "scale", name, "--replicas", "1"], args.context))
            run(cli([cli_bin, "deployment", "scale", created["app_id"], "--replicas", "1"], args.context))

            wait_http(read_url, args.timeout)
            print("stateful verification passed")
        finally:
            if not args.keep:
                run(cli([cli_bin, "deployment", "delete", created["app_id"]], args.context), check=False)
                run(cli([cli_bin, "postgres", "delete", pg_name], args.context), check=False)
                run(cli([cli_bin, "redis", "delete", redis_name], args.context), check=False)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"stateful verification failed: {exc}", file=sys.stderr)
        raise
