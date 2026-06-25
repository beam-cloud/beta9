FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/beta9
COPY sdk /opt/beta9/sdk
RUN pip install --no-cache-dir /opt/beta9/sdk
COPY sdk/src/beta9 /opt/beta9/current-sdk/beta9
ENV PYTHONPATH=/opt/beta9/current-sdk

COPY deploy/github-builder/beta9_github_deploy.py /usr/local/bin/beta9-github-deploy
RUN chmod +x /usr/local/bin/beta9-github-deploy

ENTRYPOINT ["sleep", "infinity"]
