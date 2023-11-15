imageVersion := latest

build:
	okteto build --build-arg BUILD_ENV=okteto --build-arg GITHUB_TOKEN=${GITHUB_TOKEN} -f ./Dockerfile -t okteto.dev/beam:$(imageVersion)

start:
	cd hack; okteto up --file okteto.yml

stop:
	cd hack; okteto down --file okteto.yml

secrets:
	unset GITHUB_TOKEN && python3 bin/populate_secrets.py

protocol:
	cd proto && ./gen.sh