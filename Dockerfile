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

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /workspace/bin/beam /workspace/cmd/main.go

CMD ["tail", "-f", "/dev/null"]
