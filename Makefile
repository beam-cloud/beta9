SHELL := /bin/bash
tag := latest
workerTag := latest
runnerTag := latest
runnerPlatform := linux/arm64

setup:
	bash bin/setup.sh
	make k3d-up runner worker gateway proxy
	# helm install beta9 deploy/charts/beta9 --create-namespace --values deploy/charts/beta9/values.local.yaml
	kustomize build --enable-helm manifests/kustomize/overlays/cluster-dev | kubectl apply -f-

# Note: you may need https://github.com/python-poetry/poetry-plugin-shell as of poetry v2.0.0
setup-sdk:
	curl -sSL https://install.python-poetry.org | python3 -
	export PATH="$$HOME/.local/bin:$$PATH"
	poetry config virtualenvs.in-project true
	poetry install -C sdk
	poetry shell -C sdk

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
	docker build . --target final --build-arg BASE_STAGE=dev -f ./docker/Dockerfile.worker -t localhost:5001/beta9-worker:$(workerTag) --build-arg CEDANA_BASE_URL=$(CEDANA_URL) --build-arg CEDANA_TOKEN=$(CEDANA_AUTH_TOKEN)
	docker push localhost:5001/beta9-worker:$(workerTag)
	bin/delete_workers.sh

proxy:
	docker build . --target build -f ./docker/Dockerfile.proxy -t localhost:5001/beta9-proxy:$(tag)
	docker push localhost:5001/beta9-proxy:$(tag)

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
	cd hack && okteto up --file okteto.yaml

stop:
	cd hack && okteto down --file okteto.yaml

protocol:
	poetry install --directory ./sdk
	poetry run --no-interaction --directory ./sdk bin/gen_proto.sh

verify-protocol:
	./bin/verify_proto.sh

test-pkg:
	go test -v ./pkg/... -bench=./pkg/..

# build-test can be run with "local" to run extra tests when pointing to your local
# dev setup. It will also exclude custom image tests due to arm64 issues on mac.
build-test:
	poetry config virtualenvs.in-project true
	poetry install -C sdk
	poetry shell -C sdk
	cd e2e/build_tests && python app.py $(MODE)

load-test:
	cd e2e/load_tests && k6 run --env URL=$(URL) --env TOKEN=$(TOKEN) throughput.js
