from io import StringIO

from rich.console import Console

from beta9.cli.machine import _machine_list_renderables
from beta9.clients.gateway import Machine, MachineMetrics


def test_machine_list_table_includes_gpu_availability_and_machines():
    output = StringIO()
    console = Console(file=output, force_terminal=False, width=160)
    renderables = _machine_list_renderables(
        {"A10G": True},
        [
            Machine(
                id="machine-one",
                cpu=4000,
                memory=8192,
                gpu="A10G",
                gpu_count=1,
                status="available",
                pool_name="gpu-pool",
                machine_metrics=MachineMetrics(free_gpu_count=1),
            )
        ],
    )

    for renderable in renderables:
        console.print(renderable)

    text = output.getvalue()
    assert "GPU Type" in text
    assert "A10G" in text
    assert "machine-one" in text
    assert "available" in text
    assert "gpu-pool" in text
