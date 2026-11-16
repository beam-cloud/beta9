# The VM that runs one GitHub Actions job. Build it with
#
#     beam vm image build docker/github-runner
#
# and set abstractions.vm.githubRunners.imageID to the image ID it prints.
# That build adds what every Beam VM has (systemd, Docker, git, Node, Python)
# after these steps, so this file adds only the runner.
FROM ubuntu:24.04@sha256:534baea6a22c03a63003dbc8dbe78fe34bc0d7e595d9a9dc9834884ff530eb55

# Versions and checksums: https://github.com/actions/runner/releases
ARG RUNNER_VERSION=2.338.2
ARG RUNNER_SHA256_AMD64=baebf204307bac6193480852732f0aba2ae97b6c5597c244f4d5558390147c40
ARG RUNNER_SHA256_ARM64=670adb2f268fe21dcbb58b3db0e0aa9d5b3bddfe2c651481336e561ff86de945
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential ca-certificates curl jq sudo unzip zip zstd \
    # Jobs run as a user with sudo and Docker, as they do on GitHub's runners.
    # Docker's package, which the VM build installs, uses this group.
    && groupadd --system docker \
    && useradd --create-home --shell /bin/bash --groups docker runner \
    && echo 'runner ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/runner \
    && chmod 0440 /etc/sudoers.d/runner \
    && case "$(dpkg --print-architecture)" in \
        amd64) arch=x64 sha256=$RUNNER_SHA256_AMD64 ;; \
        arm64) arch=arm64 sha256=$RUNNER_SHA256_ARM64 ;; \
        *) echo "GitHub has no runner for $(dpkg --print-architecture)" >&2; exit 1 ;; \
    esac \
    && curl -fsSL --retry 3 -o /tmp/runner.tgz \
        "https://github.com/actions/runner/releases/download/v$RUNNER_VERSION/actions-runner-linux-$arch-$RUNNER_VERSION.tar.gz" \
    && echo "$sha256  /tmp/runner.tgz" | sha256sum --check - \
    && mkdir /home/runner/actions-runner \
    && tar -xzf /tmp/runner.tgz -C /home/runner/actions-runner \
    && /home/runner/actions-runner/bin/installdependencies.sh \
    && chown -R runner:runner /home/runner/actions-runner \
    && rm -rf /tmp/runner.tgz /var/lib/apt/lists/*

# What systemctl enable would do; systemd is installed after these steps.
COPY beam-github-runner.service /etc/systemd/system/
RUN mkdir -p /etc/systemd/system/multi-user.target.wants \
    && ln -s ../beam-github-runner.service /etc/systemd/system/multi-user.target.wants/
