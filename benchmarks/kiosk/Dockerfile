FROM ubuntu:24.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    python3-gi python3-gi-cairo python3-cairo gir1.2-gtk-3.0 \
    python3-pyqt5 python3-opengl python3-pyqt5.qtopengl mesa-utils
COPY probe.py mcp_probe.py /workspace/
