"""Run inside a Beam container with DATABASE_URL and
BAD_HOST=mismatch.${{db.NAME.HOST}} wired through MCP.

Requires psycopg[binary], psycopg2-binary, Node.js and the pg npm package.
No client trust overrides or application CA installation are needed.
"""

import json
import os
import subprocess
from urllib.parse import urlsplit, urlunsplit

import psycopg
import psycopg2

url = os.environ["DATABASE_URL"]
parts = urlsplit(url)
if not parts.hostname:
    raise ValueError("DATABASE_URL must include a hostname")
authority = parts.netloc.rsplit("@", 1)
authority[-1] = os.environ.get("BAD_HOST") or "mismatch." + parts.hostname
if parts.port:
    authority[-1] += f":{parts.port}"
wrong_host = urlunsplit(parts._replace(netloc="@".join(authority)))
for driver in (psycopg, psycopg2):
    with driver.connect(url, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            assert cursor.fetchone() == (1,)
    try:
        driver.connect(wrong_host, connect_timeout=5)
    except Exception as error:
        assert "certificate" in str(error).lower(), type(error).__name__
    else:
        raise AssertionError("accepted the wrong TLS hostname")
    print(
        json.dumps(
            {
                "client": driver.__name__,
                "query": "passed",
                "hostname_verification": "passed",
            }
        ),
        flush=True,
    )

script = """
const { Client } = require("pg");

async function connect(connectionString) {
  const client = new Client({ connectionString, connectionTimeoutMillis: 5000 });
  try {
    await client.connect();
    await client.query("SELECT 1");
  } finally {
    await client.end();
  }
}

(async () => {
  await connect(process.env.DATABASE_URL);
  try {
    await connect(process.env.BAD_DATABASE_URL);
    throw new Error("accepted the wrong TLS hostname");
  } catch (error) {
    if (!/certificate|altname/i.test(error.message)) throw error;
  }
  console.log(JSON.stringify({ client: "node-pg", query: "passed", hostname_verification: "passed" }));
})().catch(error => {
  console.error(error.code || error.name);
  process.exitCode = 1;
});
"""
subprocess.run(
    ["node", "-e", script],
    check=True,
    env={**os.environ, "BAD_DATABASE_URL": wrong_host},
)
