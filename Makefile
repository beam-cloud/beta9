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
