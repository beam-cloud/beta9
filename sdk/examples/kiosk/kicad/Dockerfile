ARG BASE_IMAGE=ubuntu:24.04
FROM public.ecr.aws/docker/library/node:24-bookworm-slim@sha256:c86ecead9e55a5210ac1727de38a4ca840bf03dcaf34dc9749addbf383b1920c AS node
FROM ${BASE_IMAGE}
ENV DEBIAN_FRONTEND=noninteractive
ARG KICAD_VERSION=10.0.8~ubuntu24.04.1
RUN apt-get update && apt-get install -y --no-install-recommends software-properties-common ca-certificates git libatomic1 libstdc++6 python3-venv \
    && add-apt-repository -y ppa:kicad/kicad-10.0-releases \
    && apt-get install -y --no-install-recommends "kicad=${KICAD_VERSION}" kicad-footprints kicad-symbols
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/LICENSE /usr/local/share/doc/node/LICENSE
COPY --from=node /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/npm
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
    && ln -s ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx \
    && node --version && npm --version
ARG MCP_REVISION=1d204fe3384eb73c890bbe531538a6e735c40fe0
RUN git init /opt/kicad-mcp && cd /opt/kicad-mcp \
    && git remote add origin https://github.com/mixelpixx/KiCAD-MCP-Server.git \
    && git fetch --depth 1 origin "$MCP_REVISION" && git checkout --detach FETCH_HEAD \
    && npm ci --engine-strict --no-audit --no-fund && test -s dist/index.js \
    && python3 -m venv --system-site-packages .venv \
    && .venv/bin/pip install --no-cache-dir -r requirements.txt
COPY config/ /root/.config/kicad/10.0/
RUN mkdir -p /workspace \
    && cp /usr/share/kicad/template/*lib-table /root/.config/kicad/10.0/
RUN python3 -c "import pcbnew; pcbnew.SaveBoard('/workspace/project.kicad_pcb', pcbnew.BOARD())"
