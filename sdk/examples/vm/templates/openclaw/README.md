# OpenClaw

[OpenClaw](https://openclaw.ai), a personal agent you message on WhatsApp,
Telegram, Discord, Slack and more, on a VM that stays on when your laptop
doesn't.

```sh
beam vm new openclaw --template TEMPLATE_ID    # the ID is on beam.cloud/templates
beam vm terminal openclaw
```

The template ships without a model or channels. In the VM's terminal, run:

```sh
openclaw onboard --install-daemon
```

Onboarding signs you in to a model provider, with a ChatGPT or Claude
subscription or an API key, connects your chat apps and installs the gateway as
a systemd user service, so it restarts with the VM. OpenClaw refuses to run as
root, so it runs as the `ubuntu` user; `openclaw` in the web terminal or
`beam vm exec openclaw -- openclaw ...` switches to them.

The gateway listens only inside the VM. Its dashboard is reachable from your
machine through a tunnel:

```sh
beam vm port-forward openclaw 18789
beam vm exec openclaw -- openclaw dashboard --no-open    # a sign-in link for http://127.0.0.1:18789
```

Rebuild the template with `python sdk/examples/vm/templates/publish.py openclaw`.
