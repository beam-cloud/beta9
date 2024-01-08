import textwrap

import click


class EpilogFormatter(click.Command):
    """
    Epilog is repurposed for an examples section while fixing
    text formatting.
    """

    def format_epilog(self, ctx, formatter):
        if not self.epilog:
            return

        epilog = self.epilog.lstrip("\n") if len(self.epilog.split("\n")) > 0 else self.epilog

        with formatter.section("Examples"):
            text = textwrap.dedent(epilog)
            for line in text.split("\n"):
                formatter.write_text(line)
