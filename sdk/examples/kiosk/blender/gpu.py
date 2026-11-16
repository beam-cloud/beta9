"""Render Cycles on the VM's NVIDIA GPU, with OptiX where it's available.

Blender only lists GPUs once something asks for them, usually its Preferences
window, so without this a render started any other way, such as over MCP, runs
on the CPU. On a VM without a GPU, nothing changes.
"""

import bpy
from bpy.app.handlers import persistent


def use_gpu() -> bool:
    cycles = bpy.context.preferences.addons["cycles"].preferences
    for kind in ("OPTIX", "CUDA"):
        if any(device.type == kind for device in cycles.get_devices_for_type(kind)):
            cycles.compute_device_type = kind
            for device in cycles.devices:
                device.use = device.type == kind
            return True
    return False


@persistent
def render_on_gpu(*_args) -> None:
    if use_gpu():
        for scene in bpy.data.scenes:
            scene.cycles.device = "GPU"


def register() -> None:
    bpy.app.handlers.load_post.append(render_on_gpu)


def unregister() -> None:
    bpy.app.handlers.load_post.remove(render_on_gpu)
