SHELL := /bin/bash
tag := latest
workerTag := latest
runnerTag := latest

setup:
	make k3d-up beam-runner beam-worker beam
	kubectl delete pod -l app=beam

setup-sdk:
	poetry install -C sdk

k3d-up:
	k3d cluster create --config hack/k3d.yaml
	kubectl config set contexts.k3d-beam.namespace beam
	okteto context use k3d-beam --namespace beam

k3d-down:
	k3d cluster delete --config hack/k3d.yaml

beam:
	docker build . --target build -f ./docker/Dockerfile.beam -t localhost:5001/beam:$(tag)
	docker push localhost:5001/beam:$(tag)

beam-worker:
	docker build . --target final --build-arg BASE_STAGE=dev -f ./docker/Dockerfile.worker -t localhost:5001/beam-worker:$(workerTag)
	docker push localhost:5001/beam-worker:$(workerTag)
	bin/delete_workers.sh

beam-runner:
	for target in py312 py311 py310 py39 py38; do \
		docker build . --target $$target --platform=linux/amd64 -f ./docker/Dockerfile.runner -t localhost:5001/beam-runner:$$target-$(runnerTag); \
		docker push localhost:5001/beam-runner:$$target-$(runnerTag); \
	done

start:
	cd hack && okteto up --file okteto.yml

stop:
	cd hack && okteto down --file okteto.yml

protocol:
	cd proto && ./gen.sh

test-internal:
	go test -v ./internal/... -bench=./internal/..

prometheus:
	cd charts/kube-prometheus-stack && helm upgrade --install prometheus prometheus-community/kube-prometheus-stack --values values.yml