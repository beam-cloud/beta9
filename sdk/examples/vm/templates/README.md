# VM templates

The recipes behind [beam.cloud/templates](https://www.beam.cloud/templates). Each
directory builds one public VM template: a 4 vCPU, 8 GiB VM with a 50 GiB disk and
the software installed, plus an RTX 4090 for Blender. Starting a template gives
you an independent VM in your own workspace:

```sh
curl -fsSL https://www.beam.cloud/install | sh -s -- --template openclaw   # a new machine
beam vm new openclaw --template TEMPLATE_ID                                  # the CLI is set up
beam vm terminal openclaw
```

| Template | Recipe | VM | First run |
| --- | --- | --- | --- |
| OpenClaw | [openclaw](openclaw) | | `openclaw onboard --install-daemon` |
| Hermes Agent | [hermes-agent](hermes-agent) | | `hermes setup` |
| Agent devbox | [agent-devbox](agent-devbox) | desktop, Docker | sign in to `claude`, then `claude-remote` |
| Computer use | [computer-use](computer-use) | desktop | sign in to `claude` or `codex` |
| Video studio | [video-studio](video-studio) | desktop | sign in to `claude` or `codex` |
| Blender | [blender](blender) | kiosk desktop, RTX 4090 | connect an agent to its MCP server |

Every template ships signed out: accounts, API keys and chat apps are yours to
add on your own VM.

## How a recipe works

`setup.sh` runs as root on a fresh VM, installs the software and checks that it
runs; [common.sh](common.sh) has the shared steps. `publish.py` then deletes the
recipe, caches, package downloads and logs, so the template carries only what a
new VM needs. The personal agents, OpenClaw and Hermes, run as the `ubuntu` user
with systemd user services that start at boot; their commands in root's shell
switch to that user. Blender's image is the [kiosk example](../../kiosk/blender)'s
Dockerfile, built on Beam's NVIDIA image because the recipe sets a GPU, and its
MCP server is registered on the build VM's disk.

## Build or change a template

`publish.py` builds a template from a recipe with the SDK, then starts a second
VM from it and waits for the recipe's check to pass:

```sh
python sdk/examples/vm/templates/publish.py openclaw                 # private, in your workspace
python sdk/examples/vm/templates/publish.py openclaw --public        # anyone can start it by ID
python sdk/examples/vm/templates/publish.py openclaw --context staging --keep
```

It prints the template ID. A private template is named after the recipe in your
workspace: `beam vm new my-openclaw --template openclaw`. Build and check VMs and
kiosk image builds count against your workspace's CPU quota, and Blender's need
an RTX 4090 each, so build a few templates at a time.

**A public template shares the build VM's whole root disk and its snapshot history
with every workspace, and that can't be undone.** Deleting the template removes
it from lookups but not from VMs already started from it. Build public templates
in a workspace that has never held personal data, keep secrets and signed-in
accounts out of recipes, and let `publish.py` create the build VM so nothing else
touches it. Secrets, environment values, disks, volumes and SSH access are
dropped from a public template's configuration, but files on the disk are not.

To publish your own template by hand:

```sh
beam vm new my-box --docker-enabled
beam vm scp setup.sh my-box:/root/setup.sh
beam vm exec my-box -- sh /root/setup.sh
beam vm template create my-box my-template --public
```
