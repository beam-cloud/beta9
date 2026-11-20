# The VM that runs one GitHub Actions job. Build it with
#
#     beam vm image build docker/github-runner
#
# and set abstractions.vm.githubRunners.imageID to the image ID it prints.
# That build adds what every Beam VM has (git, Node, Python) after these
# steps, so this file adds the runner and what a job written for GitHub's
# ubuntu-24.04 runners expects of the machine. What GitHub does is in
# https://github.com/actions/runner-images/tree/ubuntu24/20261111.370/images/ubuntu
FROM ubuntu:24.04@sha256:534baea6a22c03a63003dbc8dbe78fe34bc0d7e595d9a9dc9834884ff530eb55

# Versions and checksums: https://github.com/actions/runner/releases
ARG RUNNER_VERSION=2.338.2
ARG RUNNER_SHA256_AMD64=baebf204307bac6193480852732f0aba2ae97b6c5597c244f4d5558390147c40
ARG RUNNER_SHA256_ARM64=670adb2f268fe21dcbb58b3db0e0aa9d5b3bddfe2c651481336e561ff86de945
# Docker and Compose as GitHub pins them (docker in toolsets/toolset-2404.json).
# https://github.com/docker/compose/releases
ARG DOCKER_VERSION=28.0.4
ARG COMPOSE_VERSION=2.38.2
ARG COMPOSE_SHA256_AMD64=486b3ffc0f806ca2efbc430cef89955386011662f0c76bad17c103d059cfa9cf
ARG COMPOSE_SHA256_ARM64=4d0f7678dd3338452beba4518e36a8e22b20cad79ba2535c687da554dc3997fb
# https://github.com/mikefarah/yq/releases
ARG YQ_VERSION=4.54.3
ARG YQ_SHA256_AMD64=129534601871538efe0ba4c5e62a34fb1d5f469bcb975c4fddeb467d82974973
ARG YQ_SHA256_ARM64=89f69310b61b68d1245065a1241d1fc843f7a36cd6103ffd77ad987613bd1b67
# PostgreSQL and pipx as GitHub pins them (postgresql in
# toolsets/toolset-2404.json, pipx in scripts/build/install-python.sh).
ARG POSTGRESQL_VERSION=16
ARG PIPX_VERSION=1.16.7
ARG DEBIAN_FRONTEND=noninteractive

# A job installs packages without -y or prompts (scripts/build/configure-apt.sh
# and configure-dpkg.sh).
COPY apt.conf /etc/apt/apt.conf.d/90github-runner

# In order:
# - The tools of the Ubuntu server install that GitHub's image starts from
#   (ubuntu-minimal, ubuntu-standard and ubuntu-server), without the packages
#   that manage a machine.
# - The apt packages GitHub installs before any toolchain (vital_packages,
#   common_packages and cmd_packages in toolsets/toolset-2404.json).
# - Its compilers, the system Python's pip, venv and headers, GitHub CLI, Git LFS
#   and kmod, which has modprobe.
# - Docker. The VM build installs the newest release only where there is none.
# The package indexes stay, for a job that installs without updating them.
RUN apt-get update \
    && apt-get install --no-install-recommends ca-certificates curl \
    && curl -fsSL -o /etc/apt/keyrings/github-cli.gpg https://cli.github.com/packages/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/github-cli.gpg] https://cli.github.com/packages stable main" \
        > /etc/apt/sources.list.d/github-cli.list \
    && curl -fsSL -o /etc/apt/keyrings/docker.asc https://download.docker.com/linux/ubuntu/gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu noble stable" \
        > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install --no-install-recommends \
        apt-utils bash-completion bc bsdextrautils busybox-static cpio dirmngr dmidecode dosfstools ed \
        eject ethtool fdisk fuse3 gawk gdisk gettext-base hdparm htop info iputils-tracepath less \
        lsb-release lshw lsof media-types mtr-tiny nano netbase nftables numactl parted patch pciutils \
        psmisc screen software-properties-common strace tcpdump tmux uuid-runtime usbutils vim whiptail \
        \
        bzip2 curl g++ gcc make jq tar unzip wget \
        autoconf automake dbus dnsutils dpkg dpkg-dev fakeroot fonts-noto-color-emoji gnupg2 iproute2 \
        iputils-ping libyaml-dev libtool libssl-dev libicu-dev libsqlite3-dev locales mercurial \
        openssh-client p7zip-rar pkg-config python-is-python3 rpm texinfo tk tree tzdata upx xvfb \
        xz-utils zsync \
        acl aria2 binutils bison brotli libnss3-tools coreutils file findutils flex ftp haveged lz4 m4 \
        mediainfo netcat-openbsd net-tools p7zip-full parallel patchelf pigz pollinate rsync shellcheck \
        sphinxsearch sqlite3 ssh sshpass sudo systemd-coredump swig telnet time zip \
        \
        build-essential gh git-lfs kmod python3-dev python3-pip python3-venv zstd \
        \
        containerd.io docker-buildx-plugin \
        "docker-ce-cli=5:$DOCKER_VERSION-1~ubuntu.24.04~noble" "docker-ce=5:$DOCKER_VERSION-1~ubuntu.24.04~noble" \
    # Daemons from those packages that a machine living for one job never needs,
    # and apt's timers, which would hold the lock a job's apt-get waits for.
    && systemctl mask haveged.service pollinate.service \
    && systemctl disable apt-daily.timer apt-daily-upgrade.timer \
    # GitHub's image has this locale besides C.UTF-8, and a job may ask for it.
    && locale-gen en_US.UTF-8 \
    # Ubuntu lets users ping and trace through file capabilities, which Beam
    # images do not keep. This is what the packages do where they cannot set one.
    && chmod u+s /usr/bin/ping /usr/bin/mtr-packet \
    # A job writes here without sudo (scripts/build/configure-system.sh).
    && chmod -R 777 /usr/share \
    && chmod 777 /usr/local/bin \
    && case "$(dpkg --print-architecture)" in \
        amd64) yq=$YQ_SHA256_AMD64 compose=$COMPOSE_SHA256_AMD64 ;; \
        arm64) yq=$YQ_SHA256_ARM64 compose=$COMPOSE_SHA256_ARM64 ;; \
    esac \
    && curl -fsSL --retry 3 -o /usr/bin/yq \
        "https://github.com/mikefarah/yq/releases/download/v$YQ_VERSION/yq_linux_$(dpkg --print-architecture)" \
    && curl -fsSL --retry 3 -o /usr/libexec/docker/cli-plugins/docker-compose \
        "https://github.com/docker/compose/releases/download/v$COMPOSE_VERSION/docker-compose-linux-$(uname -m)" \
    && printf '%s  %s\n' "$yq" /usr/bin/yq "$compose" /usr/libexec/docker/cli-plugins/docker-compose | sha256sum --check - \
    && chmod 0755 /usr/bin/yq /usr/libexec/docker/cli-plugins/docker-compose

# The libraries that the browsers of Playwright load and the fonts they draw
# with: every package that "npx playwright install-deps" installs on Ubuntu
# 24.04, for Chromium, Firefox and WebKit. A job need not run install-deps,
# which finds nothing to install if it does. It still downloads the browsers,
# in the versions its Playwright asks for. The lists are those of Playwright
# 1.65.1, in the order of
# https://github.com/microsoft/playwright/blob/v1.65.1/packages/playwright-core/src/server/registry/nativeDeps.ts
RUN apt-get update \
    && apt-get install --no-install-recommends \
        # tools
        xvfb fonts-noto-color-emoji fonts-unifont libfontconfig1 libfreetype6 xfonts-cyrillic \
        xfonts-scalable fonts-liberation fonts-ipafont-gothic fonts-wqy-zenhei fonts-tlwg-loma-otf \
        fonts-freefont-ttf \
        # chromium
        libasound2t64 libatk-bridge2.0-0t64 libatk1.0-0t64 libatspi2.0-0t64 libcairo2 libcups2t64 \
        libdbus-1-3 libdrm2 libgbm1 libglib2.0-0t64 libnspr4 libnss3 libpango-1.0-0 libx11-6 \
        libxcb1 libxcomposite1 libxdamage1 libxext6 libxfixes3 libxkbcommon0 libxrandr2 \
        # firefox, less what is above
        libavcodec60 libcairo-gobject2 libgdk-pixbuf-2.0-0 libgtk-3-0t64 libpangocairo-1.0-0 \
        libx11-xcb1 libxcb-shm0 libxcursor1 libxi6 libxrender1 \
        # webkit, less what is above
        gstreamer1.0-libav gstreamer1.0-plugins-bad gstreamer1.0-plugins-base \
        gstreamer1.0-plugins-good libicu74 libatomic1 libenchant-2-2 libepoxy0 libevent-2.1-7t64 \
        libflite1 libgles2 libgstreamer-gl1.0-0 libgstreamer-plugins-bad1.0-0 \
        libgstreamer-plugins-base1.0-0 libgstreamer1.0-0 libgtk-4-1 libharfbuzz-icu0 libharfbuzz0b \
        libhyphen0 libjpeg-turbo8 liblcms2-2 libmanette-0.2-0 libopus0 libpng16-16t64 \
        libsecret-1-0 libvpx9 libwayland-client0 libwayland-egl1 libwayland-server0 libwebp7 \
        libwebpdemux2 libwoff1 libxml2 libxslt1.1 libx264-164 libavif16

# PostgreSQL's server and client programs, and the headers of libpq, from
# PostgreSQL's own repository, which is not left for a job's apt-get to read.
# A job runs createdb or psql against a database of its own, makes a cluster
# with the initdb in /usr/lib/postgresql, or starts the one made here, which
# does not start with the machine (scripts/build/install-postgresql.sh). That
# cluster takes its encoding from the locale it is installed in: UTF8, as on
# GitHub's image.
RUN curl -fsSL -o /etc/apt/keyrings/pgdg.asc https://www.postgresql.org/media/keys/ACCC4CF8.asc \
    && echo "deb [signed-by=/etc/apt/keyrings/pgdg.asc] https://apt.postgresql.org/pub/repos/apt noble-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && LANG=C.UTF-8 apt-get install --no-install-recommends "postgresql-$POSTGRESQL_VERSION" libpq-dev \
    && systemctl disable postgresql.service \
    && rm /etc/apt/sources.list.d/pgdg.list /etc/apt/keyrings/pgdg.asc \
    && apt-get update

# Ubuntu's Python refuses "pip install" outside a virtual environment. GitHub
# lets it through, and has pipx for tools that want an environment each
# (scripts/build/install-python.sh).
COPY pip.conf /etc/pip.conf
RUN python3 -m pip install --no-cache-dir "pipx==$PIPX_VERSION" \
    && pipx --version

# What every process of a job starts with, and the limits it runs under
# (scripts/build/configure-environment.sh and configure-limits.sh).
COPY environment /etc/environment
COPY sysctl.conf /etc/sysctl.d/60-github-runner.conf
COPY limits.conf /etc/security/limits.d/github-runner.conf

# Jobs run as GitHub's user, with sudo and Docker, who can write to /opt.
RUN useradd --uid 1001 --create-home --shell /bin/bash --groups adm,users,docker,systemd-journal runner \
    && echo 'runner ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/runner \
    && chmod 0440 /etc/sudoers.d/runner \
    && install -d -o runner -g runner /home/runner/work /home/runner/actions-runner /opt/hostedtoolcache \
    && chmod -R 777 /opt \
    && case "$(dpkg --print-architecture)" in \
        amd64) arch=x64 sha256=$RUNNER_SHA256_AMD64 ;; \
        arm64) arch=arm64 sha256=$RUNNER_SHA256_ARM64 ;; \
        *) echo "GitHub has no runner for $(dpkg --print-architecture)" >&2; exit 1 ;; \
    esac \
    && curl -fsSL --retry 3 -o /tmp/runner.tgz \
        "https://github.com/actions/runner/releases/download/v$RUNNER_VERSION/actions-runner-linux-$arch-$RUNNER_VERSION.tar.gz" \
    && echo "$sha256  /tmp/runner.tgz" | sha256sum --check - \
    && tar -xzf /tmp/runner.tgz -C /home/runner/actions-runner \
    && /home/runner/actions-runner/bin/installdependencies.sh \
    && chown -R runner:runner /home/runner/actions-runner \
    && rm /tmp/runner.tgz

# Keepalive for the connections the machine opens, which the runner's unit
# turns on. A job otherwise ends half a minute after its last step: the reason
# is in tcp-keepalive.c.
COPY tcp-keepalive.c /tmp/
RUN gcc -O2 -Wall -Werror -o /usr/local/sbin/beam-tcp-keepalive /tmp/tcp-keepalive.c \
    && rm /tmp/tcp-keepalive.c

COPY beam-github-runner.service /etc/systemd/system/
RUN systemctl enable beam-github-runner.service
