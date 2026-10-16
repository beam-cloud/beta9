# VM startup

Staging verification on 2026-10-16, using Okteto and a temporarily borrowed
Gladiator node (GT13). Default guest: 4 vCPU, 8192 MiB, fresh 50-GiB durable
root, SSH and desktop enabled. The immutable image layers were cached; no
running VM or memory snapshot was reused. Clients ran on macOS against the
public staging endpoints, in a new process for every sample.

| Client | Samples | Median ready | p95 / maximum ready |
| --- | ---: | ---: | ---: |
| Beam CLI, Python 3.12 | 10 | 732 ms | 830 ms |
| SDK, Python 3.13, including preparation | 10 | 786 ms | 909 ms |

All 20 VMs executed a command with verified output and were removed. The SDK
batch's slowest first accepted command was 990 ms. Ready means the existing
server-side process-manager readiness check passed; it does not wait for every
desktop service or command output. The CLI timer excludes interpreter imports
and its version check, as before. Image builds, uncached layer downloads and
capacity queues are outside this cached-image result.

The original traced launch spent 2.21 seconds between hypervisor spawn and
guest init. The changes parallelize page metadata initialization, remove
unneeded guest NUMA setup and the software DMA bounce buffer, and avoid an
OpenSSL process during trust installation. Default creation now combines image
lookup, allocation and exec readiness into one authenticated HTTP request.
The stock Ubuntu base is pinned, and guest assets have a consistent gzip header
across Python versions and operating systems so clients reuse the same image.

Reproduce with the candidate gateway, worker kernel/init and SDK, plus an idle
staging microVM worker:

```sh
CONFIG_PATH="$HOME/.beam/config.ini" uv run --project sdk python benchmarks/vm_startup.py \
  --context staging --mode vm --pool microvm-cpu --cpu 4 --memory 8192 --desktop \
  --count 10 --include-preparation --fresh-client --budget-phase exec-ready \
  --budget-ms 1000 --output /tmp/vm-startup.json
```

Linux runtime validation also passed cold boot, verified exec, IPv4/IPv6,
egress isolation, resource limits/OOM, durable disk and snapshot checks. The
default-size runtime gate verified exec in 392–538 ms across three cold guests.
The borrowed node was returned to its original worker image and pool, and
Okteto was deactivated after testing.
