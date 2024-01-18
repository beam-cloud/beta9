SHELL := /bin/bash
tag := latest
workerTag := latest
runnerTag := latest

setup:
	bash bin/setup.sh
	make k3d-up beam-runner beam-worker beam
	kubectl delete pod -l app=beam

setup-sdk:
	poetry install -C sdk

k3d-up:
	bash bin/k3d.sh up

k3d-down:
	bash bin/k3d.sh down

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
