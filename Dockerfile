ARG BUILD_ENV=release

# Post-build stage (okteto)
FROM --platform=linux/x86_64 golang:latest as build_okteto

ONBUILD COPY .secrets /workspace/.secrets

# Post-build stage (release)
FROM --platform=linux/x86_64 golang:latest as build_release

WORKDIR /workspace

# Build stage
FROM build_${BUILD_ENV} as build

WORKDIR /workspace

ARG GITHUB_TOKEN
RUN echo "machine github.com login beam-cloud password ${GITHUB_TOKEN}" > ~/.netrc
ENV GOPRIVATE=github.com/beam-cloud/*

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN apt-get update && apt-get install -y curl git
RUN curl -sSL https://d.juicefs.com/install | sh -

RUN go build -o /workspace/bin/beam /workspace/cmd/beam/main.go
RUN go build -o /workspace/bin/worker /workspace/cmd/worker/main.go

CMD ["tail", "-f", "/dev/null"]
