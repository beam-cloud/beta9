# Only interactive Bash logins, such as SSH and the browser terminal.
case "$-" in
  *i*)
    if [ -f /run/beam-mirrored-home ] && [ -z "${BEAM_MIRROR_SHELL_STARTED:-}" ]; then
      exec /opt/beam-vm/mirror-shell
    fi
    ;;
esac
