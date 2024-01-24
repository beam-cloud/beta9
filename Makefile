SHELL := /bin/bash
tag := latest
workerTag := latest
runnerTag := latest

setup:
	bash bin/setup.sh
	make k3d-up runner worker gateway
	kubectl delete pod -l app=gateway

setup-sdk:
	poetry install -C sdk

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

loki:
	cd charts/loki && helm install --values values.yml loki grafana/loki -n monitoring --create-namespace

fluentbit:
	cd charts/fluentbit && helm upgrade --install --values values.yml fluent-bit fluent/fluent-bit

victoria:
	cd charts/victoriametrics && helm upgrade --install vm vm/victoria-metrics-k8s-stack -f values.yaml -n monitoring --create-namespace
