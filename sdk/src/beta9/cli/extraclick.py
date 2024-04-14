import inspect
import textwrap
from gettext import gettext as _

import click


class CustomCommand(click.Command):
    def format_epilog(self, ctx, formatter):
        """
        Writes the epilog text to the formatter if it exists.
        """
        if not self.epilog:
            return

        text = textwrap.dedent(self.epilog)
        formatter.write(text)
        formatter.write("\n")

    def format_help_text(self, ctx, formatter) -> None:
        """
        Writes the help text to the formatter if it exists.
        """
        if self.help is not None:
            # truncate the help text to the first form feed
            text = inspect.cleandoc(self.help).partition("\f")[0]
        else:
            text = ""

        if self.deprecated:
            text = _("(Deprecated) {text}").format(text=text)

        if text:
            formatter.write_paragraph()

            with formatter.indentation():
                text = textwrap.indent(text, " " * formatter.current_indent)
                formatter.write(text)
                formatter.write("\n")


class CommandGroupCollection(click.CommandCollection):
    @property
    def sources_map(self) -> dict[str, click.Group]:
        """
        A dictionary representation of {"command name": click_group}
        """
        r = {}
        for source in self.sources:
            if not isinstance(source, click.Group):
                continue
            for command in source.commands:
                r[command] = source
        return r

    def invoke(self, ctx):
        if ctx.protected_args:
            if group := self.sources_map.get(ctx.protected_args[0]):
                group.invoke(ctx)
        else:
            super().invoke(ctx)

    def format_commands(self, ctx, formatter) -> None:
        """
        Extra format methods for multi methods that adds all the commands after
        the options.
        """
        commands = {}
        commands["common"] = []
        commands["management"] = []

        for subcommand in self.list_commands(ctx):
            cmd = self.get_command(ctx, subcommand)
            if cmd is None:
                continue
            if cmd.hidden:
                continue

            if cmd.help.startswith(("manage", "Manage")):
                commands["management"].append((subcommand, cmd))
            else:
                commands["common"].append((subcommand, cmd))

        for cmdtype, cmds in commands.items():
            if len(cmds):
                limit = formatter.width - 6 - max(len(cmd[0]) for cmd in cmds)

                rows = []
                for subcommand, cmd in cmds:
                    help = cmd.get_short_help_str(limit)
                    rows.append((subcommand, help))

                if rows:
                    with formatter.section(_(cmdtype.title() + " Commands")):
                        formatter.write_dl(rows)
