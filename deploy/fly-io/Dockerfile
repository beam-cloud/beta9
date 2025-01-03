FROM ubuntu:24.04

RUN apt-get update && apt-get install -y curl
RUN curl -fsSL https://d.juicefs.com/install | sh -
RUN curl -fsSL https://tailscale.com/install.sh | sh -

WORKDIR /app
COPY . .

ENTRYPOINT ["/app/start.sh"]
