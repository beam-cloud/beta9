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
