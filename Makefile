imageVersion := latest

build:
	okteto build --build-arg BUILD_ENV=okteto --build-arg GITHUB_TOKEN=${GITHUB_TOKEN} -f ./docker/Dockerfile.beam -t okteto.dev/beam:$(imageVersion)

start:
	cd hack; okteto up --file okteto.yml

stop:
	cd hack; okteto down --file okteto.yml

secrets:
	unset GITHUB_TOKEN && python3 bin/populate_secrets.py

protocol:
	cd proto && ./gen.sh

worker:
	okteto build --build-arg BUILD_ENV=okteto --build-arg GITHUB_TOKEN=${GITHUB_TOKEN} -f ./docker/Dockerfile.worker -t okteto.dev/beam-worker:$(imageVersion)
	make delete-workers

delete-workers:
	./bin/delete_workers.sh

runner:
	okteto build --build-arg GITHUB_TOKEN=${GITHUB_TOKEN} -f ./docker/runner/Dockerfile.py38 --target py38 -t okteto.dev/beam-runner:py38-latest
	okteto build --build-arg GITHUB_TOKEN=${GITHUB_TOKEN} -f ./docker/runner/Dockerfile.py39 --target py39 -t okteto.dev/beam-runner:py39-latest
	okteto build --build-arg GITHUB_TOKEN=${GITHUB_TOKEN} -f ./docker/runner/Dockerfile.py310 --target py310 -t okteto.dev/beam-runner:py310-latest