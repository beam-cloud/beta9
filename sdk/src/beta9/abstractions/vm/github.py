"""Interactive GitHub sign-in in a VM's desktop session."""

_SIGN_IN = """\
unset GH_TOKEN GITHUB_TOKEN GH_PROMPT_DISABLED GH_DEBUG
status=0
if ! command -v gh >/dev/null 2>&1; then
    printf '%s\\n' 'Install GitHub CLI in this VM using https://cli.github.com/ and retry.' >&2
    status=127
elif gh auth login --hostname github.com --git-protocol https --web &&
     gh auth setup-git --hostname github.com; then
    printf '%s\\n' 'GitHub is connected for HTTPS Git in this VM.'
else
    status=$?
    printf '%s\\n' 'GitHub setup did not complete. See the output above.' >&2
fi
printf '%s' 'Press Enter to close. '
IFS= read -r _ || true
exit "$status"
"""


def connect(vm):
    """Open a terminal for browser sign-in and HTTPS Git setup as the desktop user.

    GitHub CLI uses the guest's credential store and can fall back to a
    plaintext config file. Credentials on the VM's disk can persist in
    snapshots. The setup configures Git's HTTPS credential helper for
    github.com. Existing remote URLs are preserved, and SSH remotes still
    use SSH authentication.
    """
    vm.refresh()
    spec = vm.info["spec"]
    if (
        not spec.get("desktop")
        or not spec.get("desktop_transport")
        or spec.get("kiosk") is not None
    ):
        raise ValueError("GitHub sign-in requires a VM with the current desktop")
    if 8080 not in (spec.get("protected_ports") or ()):
        raise ValueError("GitHub sign-in requires a private desktop. Protect port 8080 and retry.")
    if vm.info.get("status") in ("stopped", "paused"):
        vm.start()
    return vm.desktop.launch("/usr/bin/gnome-terminal", "--wait", "--", "/bin/sh", "-c", _SIGN_IN)
