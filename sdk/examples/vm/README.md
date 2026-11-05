# Share an installed desktop application

New Ubuntu desktop images use the same Selkies stream and authenticated WebRTC
relay as kiosk sessions. Installations without the shared TCP gateway use the
WebSocket transport. Existing VMs keep their original image and saved transport;
rebuild the image to adopt the new desktop runtime.

Create a clean desktop, then open its protected browser session:

```sh
beam vm new app-base --desktop --no-ssh
beam vm desktop app-base
```

In the VM browser, download the application's Ubuntu `.deb` for its architecture.
Open the completed download to launch Install Package, install it, and open the
application from the desktop menu. Desktop applications run as the
ordinary desktop user. The package installer has a dedicated elevation rule;
other commands do not receive passwordless sudo.
Enabling SSH grants its account the usual administrator access; this example
keeps SSH disabled.

Build shared templates in a fresh VM that has never held personal data. Leave
application and browser accounts signed out: publishing shares the **complete root
disk and its snapshot history**, including home directories. Deleting files
does not sanitize earlier snapshots. Secrets, environment values,
attached disks/volumes, SSH access, kiosk MCP bindings and the publisher's pool binding are excluded
from the template configuration; this does not scrub files inside the root.

```sh
beam vm template create app-base app-desktop --public --json
```

Share the returned immutable template ID. Another workspace can inspect and fork
it using its own credentials:

```sh
beam vm template show TEMPLATE_ID --json
beam vm new my-desktop --template TEMPLATE_ID
beam vm desktop my-desktop
```

The fork belongs to the recipient's workspace and has an independent writable
root, VM identity, traffic credentials and SSH host keys. Installed software is
retained. Template names are local to their owner's workspace; cross-workspace
forks require the public template's ID. Templates are private unless `--public`
is supplied.

Publication makes the root snapshot chain public permanently. Deleting the
template removes its lookup entry; it does not revoke snapshot access or remove
copies that other workspaces have already forked.

The SDK exposes the same operation with
`vm.create_template("app-desktop", public=True)` and
`VM("my-desktop", template=template_id).create()`. When using a custom VM image,
prepare it with `beam vm image build --desktop` before creating the source VM.
