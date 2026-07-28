FROM nvidia/cuda:12.8.1-devel-ubuntu24.04

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install --no-install-recommends -y \
    build-essential \
    ca-certificates \
    ffmpeg \
    git \
    libgl1 \
    libglib2.0-0 \
    python3.12 \
    python3.12-dev \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3.12 /usr/local/bin/python \
    && ln -sf /usr/bin/pip3 /usr/local/bin/pip

# Match AI Toolkit's current CUDA/PyTorch stack.
RUN python -m pip install --no-cache-dir --break-system-packages \
    torch==2.9.1 \
    torchvision==0.24.1 \
    torchaudio==2.9.1 \
    --index-url https://download.pytorch.org/whl/cu128

# Pin the trainer so upstream changes cannot silently alter a reproducible run.
ARG AI_TOOLKIT_COMMIT=e00f3791e221eb65d87351224a7788906b361b89
RUN git clone --filter=blob:none https://github.com/ostris/ai-toolkit.git /opt/ai-toolkit \
    && cd /opt/ai-toolkit \
    && git checkout "${AI_TOOLKIT_COMMIT}" \
    && python -m pip install --no-cache-dir --break-system-packages -r requirements.txt \
    && python -m pip install --no-cache-dir --break-system-packages setuptools==69.5.1

COPY export.py run.py train.yaml /app/example/

WORKDIR /opt/ai-toolkit
CMD ["python", "/app/example/run.py"]
