SHELL := /bin/bash
tag := latest
workerTag := latest
runnerTag := latest

setup:
	bash bin/setup.sh
	make k3d-up runner worker gateway proxy
	# helm install beta9 deploy/charts/beta9 --create-namespace --values deploy/charts/beta9/values.local.yaml
	kustomize build --enable-helm manifests/kustomize/overlays/cluster-dev | k apply -f-

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

gateway:
	docker build . --target build -f ./docker/Dockerfile.gateway -t localhost:5001/beta9-gateway:$(tag)
	docker push localhost:5001/beta9-gateway:$(tag)

worker:
	docker build . --target final --build-arg BASE_STAGE=dev -f ./docker/Dockerfile.worker -t localhost:5001/beta9-worker:$(workerTag)
	docker push localhost:5001/beta9-worker:$(workerTag)
	bin/delete_workers.sh

proxy:
	docker build . --target build -f ./docker/Dockerfile.proxy -t localhost:5001/beta9-proxy:$(tag)
	docker push localhost:5001/beta9-proxy:$(tag)

runner:
	for target in py311 py310 py39 py38; do \
		docker build . --target $$target --platform=linux/amd64 -f ./docker/Dockerfile.runner -t localhost:5001/beta9-runner:$$target-$(runnerTag); \
		docker push localhost:5001/beta9-runner:$$target-$(runnerTag); \
	done

start:
	cd hack && okteto up --file okteto.yml

stop:
	cd hack && okteto down --file okteto.yml

protocol:
	cd proto && ./gen.sh

test-internal:
	go test -v ./internal/... -bench=./internal/..
