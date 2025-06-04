SHELL := /bin/bash
tag := latest
workerTag := latest
runnerTag := latest
runnerPlatform := linux/$(shell uname -m | sed 's/x86_64/amd64/' | sed 's/aarch64/arm64/')

setup:
	bash bin/setup.sh
	make k3d-up runner worker gateway
	# helm install beta9 deploy/charts/beta9 --create-namespace --values deploy/charts/beta9/values.local.yaml
	kustomize build --enable-helm manifests/kustomize/overlays/cluster-dev | kubectl apply -f-

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
	bin/delete_workers.sh

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

