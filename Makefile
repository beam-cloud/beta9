SHELL := /bin/bash
tag := latest
workerTag := latest
runnerTag := latest
cedanaTag := 0.9.228

setup:
	bash bin/setup.sh
	make k3d-up runner worker gateway proxy
	# helm install beta9 deploy/charts/beta9 --create-namespace --values deploy/charts/beta9/values.local.yaml
	kustomize build --enable-helm manifests/kustomize/overlays/cluster-dev | kubectl apply -f-

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
	docker build . --build-arg CEDANA_VERSION=$(cedanaTag) --target final --build-arg BASE_STAGE=dev -f ./docker/Dockerfile.worker -t localhost:5001/beta9-worker:$(workerTag)
	docker push localhost:5001/beta9-worker:$(workerTag)
	bin/delete_workers.sh

proxy:
	docker build . --target build -f ./docker/Dockerfile.proxy -t localhost:5001/beta9-proxy:$(tag)
	docker push localhost:5001/beta9-proxy:$(tag)

runner:
	for target in py312 py311 py310 py39 py38; do \
		docker build . --no-cache --target $$target --platform=linux/amd64 -f ./docker/Dockerfile.runner -t localhost:5001/beta9-runner:$$target-$(runnerTag); \
		docker push localhost:5001/beta9-runner:$$target-$(runnerTag); \
	done

start:
	cd hack && okteto up --file okteto.yml

stop:
	cd hack && okteto down --file okteto.yml

protocol:
	poetry install --directory ./sdk
	poetry run --no-interaction --directory ./sdk bin/gen_proto.sh

verify-protocol:
	./bin/verify_proto.sh

test-pkg:
	go test -v ./pkg/... -bench=./pkg/..
