.PHONY: docs tests

init:
	git submodule update --init --recursive

docs:
	uv run ./bin/docgen.py 2>/dev/null

tests:
	uv run --with pytest-cov pytest -s 
	# open .htmlcov/index.html

format:
	uv ruff check .

build: 
	uv build

clean:
	rm dist/*

publish: clean build
	uv publish --token ${PYPI_TOKEN}
