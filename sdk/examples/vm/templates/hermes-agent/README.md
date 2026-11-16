# Hermes Agent

[Hermes Agent](https://hermes-agent.nousresearch.com) from Nous Research, a
personal agent with memory, skills and a messaging gateway for Telegram,
Discord, Slack, WhatsApp and more, on a VM that stays on.

```sh
beam vm new hermes --template TEMPLATE_ID    # the ID is on beam.cloud/templates
beam vm terminal hermes
```

The template ships without a model or chat apps. In the VM's terminal, run:

```sh
hermes setup
```

The wizard picks a model provider, such as Nous Portal, OpenRouter or a key of
your own, and can connect your chat apps. `hermes gateway setup` adds chat apps
later, and `hermes gateway install` runs the gateway as a systemd user service,
so it restarts with the VM. `hermes` on its own opens a chat in the terminal.

Hermes runs commands for you, so it runs as the `ubuntu` user rather than root,
under `~/.hermes`; `hermes` in the web terminal or
`beam vm exec hermes -- hermes ...` switches to them. The gateway opens no ports: it polls your chat apps.

Rebuild the template with `python sdk/examples/vm/templates/publish.py hermes-agent`.
