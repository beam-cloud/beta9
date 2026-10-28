FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends blender python3-venv python3-requests ca-certificates \
    && python3 -m venv /opt/blender-mcp \
    && /opt/blender-mcp/bin/pip install --no-cache-dir mcp-for-blender==2.2.6
ENV BLENDER_USER_SCRIPTS=/opt/blender-scripts BLENDERMCP_NO_UPDATE_CHECK=1
RUN BLENDERMCP_ADDONS_DIR=/opt/blender-scripts/addons /opt/blender-mcp/bin/mcp-for-blender install-addon
RUN mkdir -p /workspace && blender --python-exit-code 1 --background --factory-startup --python-expr \
    "import bpy; bpy.ops.preferences.addon_enable(module='blender_mcp'); bpy.ops.wm.save_userpref(); bpy.ops.wm.save_as_mainfile(filepath='/workspace/project.blend')"
