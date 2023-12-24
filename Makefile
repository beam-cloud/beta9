SHELL := /bin/bash
imageVersion := latest

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
	docker build . --target build --secret id=github-token,src=<(echo -n ${GITHUB_TOKEN}) -f ./docker/Dockerfile.beam -t localhost:5000/beam:$(imageVersion)
	docker push localhost:5000/beam:$(imageVersion)

beam-worker:
	docker build . --target final --build-arg BASE_STAGE=dev --secret id=github-token,src=<(echo -n ${GITHUB_TOKEN}) -f ./docker/Dockerfile.worker -t localhost:5000/beam-worker:$(imageVersion)
	docker push localhost:5000/beam-worker:latest
	bin/delete_workers.sh

beam-runner:
	for target in py312 py311 py310 py39 py38; do \
		docker build . --target $$target --secret id=github-token,src=<(echo -n ${GITHUB_TOKEN}) -f ./docker/Dockerfile.runner -t localhost:5000/beam-runner:$$target-latest; \
		docker push localhost:5000/beam-runner:$$target-latest; \
	done

start:
	cd hack && okteto up --file okteto.yml

stop:
	cd hack && okteto down --file okteto.yml

secrets:
	unset GITHUB_TOKEN && OKTETO_NAMESPACE=beam python3 bin/populate_secrets.py

protocol:
	cd proto && ./gen.sh
