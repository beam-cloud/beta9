# Build with `beam vm image build docker/ci-compaction` and set
# abstractions.vm.githubRunners.compactionImageID to the printed ID.
FROM ubuntu:24.04@sha256:534baea6a22c03a63003dbc8dbe78fe34bc0d7e595d9a9dc9834884ff530eb55

# The Beam VM build adds Git, curl, Python and Node.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential python3-venv systemd \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --user-group --shell /bin/bash compaction

COPY beam-ci-compaction.service /etc/systemd/system/
RUN systemctl enable beam-ci-compaction.service
