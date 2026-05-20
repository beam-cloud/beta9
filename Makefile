SHELL := /bin/bash
tag := latest
workerTag := latest
runnerTag := latest
runnerPlatform := linux/$(shell uname -m | sed 's/x86_64/amd64/' | sed 's/aarch64/arm64/')
BENCH_NAMESPACE ?= beta9
BENCH_GATEWAY_URL ?= http://127.0.0.1:11994
BENCH_GRPC_ADDR ?= 127.0.0.1:11993
BENCH_TOKEN ?= $(BETA9_TOKEN)
BENCH_TOKEN_CACHE ?= /tmp/beta9-startup-benchmark-$(BENCH_NAMESPACE).token
BENCH_STUB_ID ?= $(STUB_ID)
BENCH_IMAGE_ID ?= $(IMAGE_ID)
BENCH_CHECKPOINT_ID ?= $(CHECKPOINT_ID)
BENCH_BOOTSTRAP_STUB ?= 1
BENCH_BOOTSTRAP_LOCAL_IMAGE ?= 1
BENCH_BOOTSTRAP_IMAGE_URI ?= k3d-registry.localhost:5000/beta9-bench-alpine:latest
BENCH_BOOTSTRAP_SOURCE_IMAGE ?= alpine:latest
BENCH_BOOTSTRAP_ENTRYPOINT ?= sh -c 'sleep 3600'
BENCH_ITERATIONS ?= 10
BENCH_WARMUP ?= 1
BENCH_INSTALL ?= 1
BENCH_PORT_FORWARD ?= 1
BENCH_RESET_WORKERS ?= 0
BENCH_RESET_WORKERS_EACH ?= 0
BENCH_TIMEOUT_SECONDS ?= 240
BENCH_POLL_INTERVAL_MS ?= 50
BENCH_SLEEP_SECONDS ?= 1
BENCH_CLEANUP_TTL_SECONDS ?= 5
BENCH_POD_PROBE_PORT ?=
BENCH_POD_PROBE_PATH ?= /
BENCH_OUTPUT ?= /tmp/beta9-startup-benchmark.json
BENCH_SANDBOX_COUNT ?= 100
BENCH_SANDBOX_PARALLELISM ?= $(BENCH_SANDBOX_COUNT)
BENCH_SANDBOX_PREWARM_COUNT ?= 0
BENCH_SANDBOX_PREWARM_PARALLELISM ?= $(BENCH_SANDBOX_PREWARM_COUNT)
BENCH_SANDBOX_PREWARM_COOLDOWN_SECONDS ?= 5
BENCH_SANDBOX_PREWARM_WAIT_IDLE ?= 1
BENCH_SANDBOX_PREWARM_IDLE_TIMEOUT_SECONDS ?= 180
BENCH_SANDBOX_WARMUP ?= 1
BENCH_SANDBOX_CPU ?= 0.1
BENCH_SANDBOX_MEMORY ?= 192
BENCH_SANDBOX_KEEP_WARM_SECONDS ?= 600
BENCH_SANDBOX_IMAGE_URI ?= $(BENCH_BOOTSTRAP_IMAGE_URI)
BENCH_SANDBOX_EXEC ?= true
BENCH_SANDBOX_EXEC_CWD ?= /
BENCH_SANDBOX_PREPARE ?= 1
BENCH_SANDBOX_WAIT_RUNNING ?= 0
BENCH_SANDBOX_WAIT_EXEC_COMPLETE ?= 0
BENCH_SANDBOX_CREATE_RETRIES ?= 20
BENCH_SANDBOX_READY_TIMEOUT_SECONDS ?= 180
BENCH_SANDBOX_READY_RETRY_MS ?= 500
BENCH_SANDBOX_OUTPUT ?= /tmp/beta9-sandbox-parallel-benchmark.json
BENCH_SDK_PYTHON ?= uv run --project ./sdk --no-sync python

.PHONY: startup-benchmark startup-benchmark-build sandbox-parallel-benchmark

setup:
	bash bin/setup.sh
	make k3d-up runner worker gateway
	# helm install beta9 deploy/charts/beta9 --create-namespace --values deploy/charts/beta9/values.local.yaml
	kustomize build --enable-helm manifests/kustomize/overlays/cluster-dev | kubectl apply -f-

startup-benchmark:
	BENCH_NAMESPACE="$(BENCH_NAMESPACE)" \
	BENCH_GATEWAY_URL="$(BENCH_GATEWAY_URL)" \
	BENCH_GRPC_ADDR="$(BENCH_GRPC_ADDR)" \
	BENCH_TOKEN="$(BENCH_TOKEN)" \
	BENCH_TOKEN_CACHE="$(BENCH_TOKEN_CACHE)" \
	BENCH_STUB_ID="$(BENCH_STUB_ID)" \
	BENCH_IMAGE_ID="$(BENCH_IMAGE_ID)" \
	BENCH_CHECKPOINT_ID="$(BENCH_CHECKPOINT_ID)" \
	BENCH_BOOTSTRAP_STUB="$(BENCH_BOOTSTRAP_STUB)" \
	BENCH_BOOTSTRAP_LOCAL_IMAGE="$(BENCH_BOOTSTRAP_LOCAL_IMAGE)" \
	BENCH_BOOTSTRAP_IMAGE_URI="$(BENCH_BOOTSTRAP_IMAGE_URI)" \
	BENCH_BOOTSTRAP_SOURCE_IMAGE="$(BENCH_BOOTSTRAP_SOURCE_IMAGE)" \
	BENCH_BOOTSTRAP_ENTRYPOINT="$(BENCH_BOOTSTRAP_ENTRYPOINT)" \
	BENCH_ITERATIONS="$(BENCH_ITERATIONS)" \
	BENCH_WARMUP="$(BENCH_WARMUP)" \
	BENCH_INSTALL="$(BENCH_INSTALL)" \
	BENCH_PORT_FORWARD="$(BENCH_PORT_FORWARD)" \
	BENCH_RESET_WORKERS="$(BENCH_RESET_WORKERS)" \
	BENCH_RESET_WORKERS_EACH="$(BENCH_RESET_WORKERS_EACH)" \
	BENCH_TIMEOUT_SECONDS="$(BENCH_TIMEOUT_SECONDS)" \
	BENCH_POLL_INTERVAL_MS="$(BENCH_POLL_INTERVAL_MS)" \
	BENCH_SLEEP_SECONDS="$(BENCH_SLEEP_SECONDS)" \
	BENCH_CLEANUP_TTL_SECONDS="$(BENCH_CLEANUP_TTL_SECONDS)" \
	BENCH_POD_PROBE_PORT="$(BENCH_POD_PROBE_PORT)" \
	BENCH_POD_PROBE_PATH="$(BENCH_POD_PROBE_PATH)" \
	BENCH_OUTPUT="$(BENCH_OUTPUT)" \
	python3 hack/startup_benchmark.py

startup-benchmark-build:
	docker build . --target build -f ./docker/Dockerfile.gateway -t localhost:5001/beta9-gateway:$(tag)
	docker push localhost:5001/beta9-gateway:$(tag)
	docker build . --target final --build-arg BASE_STAGE=dev -f ./docker/Dockerfile.worker -t localhost:5001/beta9-worker:$(workerTag)
	docker push localhost:5001/beta9-worker:$(workerTag)
	$(MAKE) startup-benchmark BENCH_INSTALL=1

sandbox-parallel-benchmark:
	BENCH_NAMESPACE="$(BENCH_NAMESPACE)" \
	BENCH_GATEWAY_URL="$(BENCH_GATEWAY_URL)" \
	BENCH_GRPC_ADDR="$(BENCH_GRPC_ADDR)" \
	BENCH_TOKEN="$(BENCH_TOKEN)" \
	BENCH_TOKEN_CACHE="$(BENCH_TOKEN_CACHE)" \
	BENCH_INSTALL="$(BENCH_INSTALL)" \
	BENCH_PORT_FORWARD="$(BENCH_PORT_FORWARD)" \
	BENCH_RESET_WORKERS="$(BENCH_RESET_WORKERS)" \
	BENCH_TIMEOUT_SECONDS="$(BENCH_TIMEOUT_SECONDS)" \
	BENCH_POLL_INTERVAL_MS="$(BENCH_POLL_INTERVAL_MS)" \
	BENCH_CLEANUP_TTL_SECONDS="$(BENCH_CLEANUP_TTL_SECONDS)" \
	BENCH_BOOTSTRAP_LOCAL_IMAGE="$(BENCH_BOOTSTRAP_LOCAL_IMAGE)" \
	BENCH_BOOTSTRAP_SOURCE_IMAGE="$(BENCH_BOOTSTRAP_SOURCE_IMAGE)" \
	BENCH_SANDBOX_COUNT="$(BENCH_SANDBOX_COUNT)" \
	BENCH_SANDBOX_PARALLELISM="$(BENCH_SANDBOX_PARALLELISM)" \
	BENCH_SANDBOX_PREWARM_COUNT="$(BENCH_SANDBOX_PREWARM_COUNT)" \
	BENCH_SANDBOX_PREWARM_PARALLELISM="$(BENCH_SANDBOX_PREWARM_PARALLELISM)" \
	BENCH_SANDBOX_PREWARM_COOLDOWN_SECONDS="$(BENCH_SANDBOX_PREWARM_COOLDOWN_SECONDS)" \
	BENCH_SANDBOX_PREWARM_WAIT_IDLE="$(BENCH_SANDBOX_PREWARM_WAIT_IDLE)" \
	BENCH_SANDBOX_PREWARM_IDLE_TIMEOUT_SECONDS="$(BENCH_SANDBOX_PREWARM_IDLE_TIMEOUT_SECONDS)" \
	BENCH_SANDBOX_WARMUP="$(BENCH_SANDBOX_WARMUP)" \
	BENCH_SANDBOX_CPU="$(BENCH_SANDBOX_CPU)" \
	BENCH_SANDBOX_MEMORY="$(BENCH_SANDBOX_MEMORY)" \
	BENCH_SANDBOX_KEEP_WARM_SECONDS="$(BENCH_SANDBOX_KEEP_WARM_SECONDS)" \
	BENCH_SANDBOX_IMAGE_URI="$(BENCH_SANDBOX_IMAGE_URI)" \
	BENCH_SANDBOX_EXEC="$(BENCH_SANDBOX_EXEC)" \
	BENCH_SANDBOX_EXEC_CWD="$(BENCH_SANDBOX_EXEC_CWD)" \
	BENCH_SANDBOX_PREPARE="$(BENCH_SANDBOX_PREPARE)" \
	BENCH_SANDBOX_WAIT_RUNNING="$(BENCH_SANDBOX_WAIT_RUNNING)" \
	BENCH_SANDBOX_WAIT_EXEC_COMPLETE="$(BENCH_SANDBOX_WAIT_EXEC_COMPLETE)" \
	BENCH_SANDBOX_CREATE_RETRIES="$(BENCH_SANDBOX_CREATE_RETRIES)" \
	BENCH_SANDBOX_READY_TIMEOUT_SECONDS="$(BENCH_SANDBOX_READY_TIMEOUT_SECONDS)" \
	BENCH_SANDBOX_READY_RETRY_MS="$(BENCH_SANDBOX_READY_RETRY_MS)" \
	BENCH_OUTPUT="$(BENCH_SANDBOX_OUTPUT)" \
	PYTHONPATH="$(CURDIR)/sdk/src:$(PYTHONPATH)" \
	$(BENCH_SDK_PYTHON) "$(CURDIR)/hack/sandbox_parallel_benchmark.py"

setup-sdk:
	@if ! command -v uv &> /dev/null; then \
		echo "uv is not installed, installing..."; \
		pip install uv; \
	fi
	@uv sync --directory ./sdk
	@exec $${SHELL} -c "source ./sdk/.venv/bin/activate && exec $${SHELL}"

k3d-up:
	bash bin/k3d.sh up

k3d-down:
	bash bin/k3d.sh down

k3d-rebuild:
	make k3d-down
	make k3d-up
	kustomize build --enable-helm manifests/kustomize/overlays/cluster-dev | kubectl apply -f-

gateway:
	docker build . --target build -f ./docker/Dockerfile.gateway -t localhost:5001/beta9-gateway:$(tag)
	docker push localhost:5001/beta9-gateway:$(tag)

worker:
	docker build . --target final --build-arg BASE_STAGE=dev -f ./docker/Dockerfile.worker -t localhost:5001/beta9-worker:$(workerTag)
	docker push localhost:5001/beta9-worker:$(workerTag)
	BENCH_NAMESPACE="$(BENCH_NAMESPACE)" bin/delete_workers.sh

runner:
	for target in py312 py311 py310 py39 py38; do \
		docker build . --target $$target --platform=$(runnerPlatform) -f ./docker/Dockerfile.runner -t localhost:5001/beta9-runner:$$target-$(runnerTag) --progress=plain; \
		docker push localhost:5001/beta9-runner:$$target-$(runnerTag); \
	done
	for version in "3.12" "3.11" "3.10" "3.9" "3.8"; do \
		docker build . --build-arg PYTHON_VERSION=$$version --target micromamba --platform=$(runnerPlatform) -f ./docker/Dockerfile.runner -t localhost:5001/beta9-runner:micromamba$$version-$(runnerTag) --progress=plain; \
		docker push localhost:5001/beta9-runner:micromamba$$version-$(runnerTag); \
	done

start:
	@if [ -f config.yaml ]; then \
		cd hack && okteto up --file okteto.yaml --env CONFIG_PATH=/workspace/config.yaml; \
	else \
		cd hack && okteto up --file okteto.yaml; \
	fi

clear-ports:
	@echo "Killing processes on ports 1993, 1994, and 8008..."
	@lsof -t -i :1993,1994,8008 | xargs -r sudo kill -9 2>/dev/null || true

stop:
	cd hack && okteto down --file okteto.yaml

protocol:
	uv run ./bin/gen_proto.sh

openapi:
	@echo "Generating OpenAPI schemas..."
	@mkdir -p docs/openapi
	protoc -I ./googleapis -I ./pkg/types -I ./pkg/abstractions/pod/ --openapiv2_out=./docs/openapi --openapiv2_opt logtostderr=true ./pkg/abstractions/pod/pod.proto
	protoc -I ./googleapis -I ./pkg/abstractions/image/ --openapiv2_out=./docs/openapi --openapiv2_opt logtostderr=true ./pkg/abstractions/image/image.proto
	protoc -I ./googleapis -I ./pkg/types -I ./pkg/gateway/ --openapiv2_out=./docs/openapi --openapiv2_opt logtostderr=true ./pkg/gateway/gateway.proto
	@echo "OpenAPI schemas generated in docs/openapi/"

verify-protocol:
	./bin/verify_proto.sh

test-pkg:
	go test -v ./pkg/... -bench=./pkg/..

# build-test can be run with "local" to run extra tests when pointing to your local
# dev setup. It will also exclude custom image tests due to arm64 issues on mac.
build-test:
	uv run python e2e/build_tests/app.py $(MODE)

load-test:
	cd e2e/load_tests && k6 run --env URL=$(URL) --env TOKEN=$(TOKEN) throughput.js

sdk-init:
	make -C sdk init

sdk-docs:
	make -C sdk docs

sdk-tests:
	make -C sdk tests

sdk-format:
	make -C sdk format

sdk-build:
	make -C sdk build

sdk-clean:
	make -C sdk clean

sdk-publish:
	make -C sdk publish
