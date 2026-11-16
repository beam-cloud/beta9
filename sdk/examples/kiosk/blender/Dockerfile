# Ubuntu 24.04 for a CPU VM; for a GPU VM, Beam's NVIDIA image, which publish.py passes in.
ARG BASE_IMAGE=ubuntu:24.04
FROM ${BASE_IMAGE}
ENV DEBIAN_FRONTEND=noninteractive
# Blender's own build, unlike Ubuntu's package, renders Cycles on NVIDIA GPUs with CUDA and OptiX.
ARG BLENDER_SERIES=4.5
ARG BLENDER_VERSION=4.5.15
ARG BLENDER_SHA256=0d2b8bb2efe97e6c8eb57de6b7322f8800e1984e3125b60decca5f6771e4773e
# download.blender.org turns some build networks away; its mirrors serve the same checksummed file.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl xz-utils python3-venv python3-requests \
    libegl1 libgl1 libice6 libsm6 libx11-6 libxext6 libxfixes3 libxi6 libxkbcommon0 libxrender1 libxxf86vm1 \
    && rm -rf /var/lib/apt/lists/* \
    && for mirror in https://download.blender.org/release https://mirrors.ocf.berkeley.edu/blender/release \
        https://ftp.nluug.nl/pub/graphics/blender/release; do \
        curl -fsSL "$mirror/Blender${BLENDER_SERIES}/blender-${BLENDER_VERSION}-linux-x64.tar.xz" \
        -o /tmp/blender.tar.xz && break; \
    done \
    && echo "${BLENDER_SHA256}  /tmp/blender.tar.xz" | sha256sum -c - \
    && mkdir /opt/blender && tar -xJf /tmp/blender.tar.xz -C /opt/blender --strip-components=1 \
    && rm /tmp/blender.tar.xz && ln -s /opt/blender/blender /usr/local/bin/blender \
    && python3 -m venv /opt/blender-mcp \
    && /opt/blender-mcp/bin/pip install --no-cache-dir mcp-for-blender==2.2.6
ENV BLENDER_USER_SCRIPTS=/opt/blender-scripts BLENDERMCP_NO_UPDATE_CHECK=1
RUN BLENDERMCP_ADDONS_DIR=/opt/blender-scripts/addons /opt/blender-mcp/bin/mcp-for-blender install-addon
COPY gpu.py /opt/blender-scripts/startup/beam_gpu.py
RUN mkdir -p /workspace && blender --python-exit-code 1 --background --factory-startup --python-expr \
    "import bpy; bpy.ops.preferences.addon_enable(module='blender_mcp'); bpy.ops.wm.save_userpref(); bpy.ops.wm.save_as_mainfile(filepath='/workspace/project.blend')"
