import click

from .. import terminal
from ..abstractions.image import Image
from ..logging import StoredStdoutInterceptor
from ..utils import load_module_spec
from .extraclick import ClickManagementGroup, pass_service_client, selected_context


@click.group(name="image", help="Build and inspect images.", cls=ClickManagementGroup)
def management():
    pass


@management.command("build", help="Build an Image from a Python handler or a Dockerfile.")
@click.argument("handler", required=False)
@click.option("--dockerfile", type=click.Path(exists=True, dir_okay=False))
@click.option("--context-dir", type=click.Path(exists=True, file_okay=False))
@click.option("--format", type=click.Choice(("table", "json")), default="table")
@pass_service_client
def build_image(service, handler, dockerfile, context_dir, format):
    if bool(handler) == bool(dockerfile):
        raise click.UsageError("Specify an Image handler (app.py:image) or --dockerfile.")
    with StoredStdoutInterceptor(capture_logs=format == "json"):
        image = (
            Image.from_dockerfile(dockerfile, context_dir)
            if dockerfile
            else load_module_spec(handler, "image build")[0]
        )
        if not isinstance(image, Image):
            raise click.UsageError("The handler must be an Image.")
        result = image.build()
        if not result.success:
            terminal.error(result.error or "Image build failed.")
    if format == "json":
        terminal.print_json({**result._asdict(), "context": selected_context()})
    else:
        terminal.success(f"Image: {result.image_id}")


@management.command("get", help="Check whether an image is available.")
@click.argument("image_id")
@pass_service_client
def get_image(service, image_id):
    exists, _ = Image.from_id(image_id).exists()
    terminal.print_json({"image_id": image_id, "exists": exists, "context": selected_context()})
    if not exists:
        raise click.exceptions.Exit(1)
