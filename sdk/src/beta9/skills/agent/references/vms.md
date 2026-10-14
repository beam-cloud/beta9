# Persistent VMs

Use a VM for a real Linux guest with a writable root, SSH, Docker, a desktop,
or a durable coding-agent session. For HTTP apps and stacks, keep the
serverless defaults instead. Keep VM defaults: 4 vCPU, 8 GiB memory, a 50 GiB
root and no idle timeout. VMs keep running until stopped. Ask before starting
paid compute unless the user has already authorized it. GPUs are not a CPU VM
feature.

## First VM

`{{cli}} setup vm --name agent-vm` signs in in the browser, connects local
agents, verifies a command in the guest and starts a browser desktop. Add
`--no-desktop` to skip the desktop.
Re-running resumes the same setup-owned VM. `--skip-agents` skips local
client configuration; `--yes` explicitly approves creation for an already
authenticated and funded unattended context.

Through MCP, prepare a base image with the local `vm_build_image` tool or
reuse a VM template/snapshot, then `create_vm`. CPU in a creation spec is in
millicores (1000 = 1 CPU); memory is MiB. Use a UUID `request_id` to recover
a lost creation response. `get_vm` supplies ID, lifecycle, container ID,
port URLs and terminal/desktop links. Poll until the guest is ready before
announcing success. VM and artifact names are at most 24 lowercase letters,
digits and hyphens, starting with a letter.

## Public IPv6

`{{cli}} vm new`, `{{cli}} vm get NAME` and `{{cli}} setup vm` show the
persistent IPv6 address when one is allocated. `{{cli}} vm ip NAME` prints
just the address for copying or scripts. In Python, use `vm.public_ipv6`
after `VM(...).create()` or `VM.get("NAME")`; it is `None` when no address
has been allocated. Reading the property does not make a network request.

The address survives stop/start and pause/resume. Published TCP ports use
their guest port number; SSH is on 2222. From an IPv6-capable network:

```sh
ssh -6 -i ~/.{{cli}}/ssh/vm_ed25519 -p 2222 root@"$({{cli}} vm ip NAME)"
curl -g "http://[$({{cli}} vm ip NAME)]:8000/"
```

Publish application ports before connecting. Protected HTTP ports still
require traffic authentication; the IPv6 address does not bypass protection.

## Commands and files

`vm_exec { name, command: ["python3", "-c", "print('hello')"], wait: true }`
accepts argv; preserve each argument, and never concatenate untrusted text
into a shell command. Waiting is bounded to 30 seconds. A longer command
returns a PID; follow `vm_process_status` and `vm_process_output`. Preserve
stdout/stderr and treat nonzero exits as failures. Pin `container_id` for
PID operations, especially `vm_kill_process`, so a VM restart cannot target
another boot's process. Process output advances its stream and is not safe
to retry blindly after a lost response.

File tools: `vm_list_files`, `vm_stat_file`, `vm_read_file`, `vm_write_file`,
`vm_create_directory`, `vm_delete_file`, `vm_delete_directory`,
`vm_find_in_files`, `vm_replace_in_files`. Read/write in bounded chunks;
reads return base64 bytes. Writes accept UTF-8 text or base64. Confirm
replacement/deletion according to the user's requested scope.

Local `vm_upload_file`, `vm_download_file`, `vm_sync`, `vm_ssh`,
`vm_port_forward`, and `vm_build_image` require `confirm=true` and a stable
`request_key`. They run durable jobs: poll `deploy_status`; cancel a watch
or port-forward with `cancel_job` to release its process/socket. A finite
operation completes with its parsed result; failed commands are never
silently rerun. Use a new key after investigating the failure. Remote HTTP
clients cannot read local paths or create a local SSH identity.

## Lifecycle and access

`pause_vm` saves RAM; `stop_vm` saves the writable disk; `start_vm` resumes
or cold-starts. `delete_vm` removes its root. Confirm interruptions or
removal before acting unless explicitly authorized. Use `update_vm` for
idle timeout/action, auto-resume, metadata and outbound network policy.
Additional disks and volumes belong in the creation spec. `vm_keep_alive`
renews activity deliberately; do not loop it to defeat an idle policy.

`create_vm_snapshot`, `create_vm_template`, `fork_vm`, and the artifact
list/delete tools manage reusable roots. SSH-enabled forks/template-based
creations need a public key matching your local private identity. Keep
private keys on the client. A fork has an independent writable root.

Terminal (7681) and desktop (8080) require a traffic token by default.
The CLI signs in automatically: `{{cli}} vm terminal NAME` and
`{{cli}} vm desktop NAME` open authenticated browser links. Use
`{{cli}} vm open NAME PORT` for any published HTTP port, or add `--url`
to print the link. Private links expire after 10 minutes; rerun the command
for a fresh one. Creation, `get`, `expose`, and `ports` show usable browser
links and distinguish private application ports from public ones. JSON
output keeps stable URLs without embedding temporary credentials.
For non-browser clients, `{{cli}} vm port-forward NAME PORT` provides an
authenticated localhost connection. `ports` also lists tunnel-only ports
and the SSH command. No traffic token needs to be copied for these flows.

Use `{{cli}} vm new --public-port 7681` or `VM(public_ports=[7681])` to opt out.
After creation, `{{cli}} vm expose --protected NAME 7681` restores protection;
`--public` removes it. SSH (2222) cannot use either setting.

`vm_bind_port` keeps a port private for authenticated tunnels.
`vm_expose_port { protected: true }` requires a traffic credential or a
limited browser link. Access-token/read/rotation/link tools need the
credentials scope. Access links grant anyone holding the link access to
that port until expiry; do not put them in public logs. Use `vm_ports` to
inspect URLs and `vm_unexpose_port` to remove one.

## Desktop, Docker and coding agents

Enable desktop/Docker at creation. Desktop tools cover size/resize, mouse
move/click/drag/scroll, keyboard shortcuts, literal clipboard text, GUI
launch, screenshot and recording. Screenshots return real MCP PNG image
content. Stop a recording with its pinned container/PID before reading its
MP4 file. GUI interaction and guest commands can affect external systems;
normal user authorization still applies.

`vm_docker` accepts Docker argv and returns the same managed process
contract. `vm_prompt` starts an installed Codex or Claude in a durable
systemd session; the agent needs its own sign-in and a suitable working
directory. Read its session with `vm_logs`; launch acceptance is not a
completed model task. Codex checks repository trust. `vm_metrics` and
`vm_logs` provide platform/guest diagnostics without exposing credentials.

## Outbound access and secrets

Sandboxes and VMs receive no automatic workspace token. If an agent needs the
Beam CLI inside its guest, explicitly bind a `BETA9_TOKEN` or `BEAM_TOKEN`
workspace secret; these opt-ins use plaintext.

Network allowlists accept up to 64 hostnames, `*.example.com` patterns, bare
IPs and CIDRs. Hostname rules govern TLS SNI and HTTP Host; other TCP protocols
need a CIDR. The worker resolves allowed hostnames and rejects private,
loopback and link-local destinations. UDP/ICMP are blocked except DNS queries
to the configured resolver. Existing connections close when policy changes.
Proxied TCP connections use the worker's source IP, including CIDR targets;
downstream source-IP access rules see the worker.

Create a brokered credential with `{{cli}} secret create KEY VALUE --host api.openai.com`.
Use `--host` repeatedly for multiple hosts or `*.wildcards`. Bind it with
`{{cli}} vm new --secret KEY`, or pass `secrets=["KEY"]` to VM exec. The guest
receives a stable `beam_secret_...` placeholder. The worker replaces it only
in HTTPS request headers on matching hosts, verifies upstream TLS, and scrubs values
from response headers, bodies and WebSocket messages. Hosts without bindings
retain TLS passthrough. Value rotation takes effect on the next request after
the worker's 15-second cache expires. Plain HTTP to a bound host is blocked.

Brokered secrets require allowed hosts. Serverless workloads retain plaintext
secret envs. With block_network, credential hosts are the only allowed egress;
an explicit allow_list must cover each credential host and is never widened.

Substitution is literal and header-only: use Authorization or x-api-key.
Basic-auth helpers that encode the placeholder will not match; store the
encoded header value as the secret instead. Certificate-pinned clients cannot
use TLS interception. Brokered WebSocket messages are limited to 16 MiB,
and compressed upstream responses are rejected when they cannot be scrubbed.
Docker containers inside a VM do not inherit its CA;
mount `/etc/beam/ca-certificates.crt` and configure their trust explicitly.

Protect an existing VM's terminal with `{{cli}} vm expose --protected NAME 7681`.
Deploy workers with egress enabled before binding brokered secrets; older
workers fail closed. CA bundles are prepared at sandbox startup so exec can
add bindings later; only public certificates enter the guest.
