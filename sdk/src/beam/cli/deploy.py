import click

from beam import aio, terminal
from beam.cli.contexts import get_gateway_service
from beam.clients.gateway import GatewayServiceStub, StopTaskResponse


@click.group(
    name="deploy",
    help="List and create deployments",
)
@click.pass_context
def cli(ctx: click.Context):
    ctx.obj = ctx.with_resource(get_gateway_service())


@cli.command(
    name="create",
    help="Create a new deployment",
)
@click.option(
    "--name",
    help="The name the deployment.",
)
@click.pass_obj
def stop_task(service: GatewayServiceStub, name: str):
    response: StopTaskResponse = aio.run_sync(service.stop_task(task_id=task_id))

    if response.ok:
        terminal.detail(f"Stopped task {name}", dim=False)
    else:
        terminal.error(f"{response.err_msg}\nFailed to stop task {task_id}")
