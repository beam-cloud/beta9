.PHONY: docs tests sandbox-log-streaming-integration build

init:
	git submodule update --init --recursive

docs:
	uv run ./bin/docgen.py 2>/dev/null

tests:
	uv run --with pytest-cov pytest -s 
	# open .htmlcov/index.html

sandbox-log-streaming-integration:
	BETA9_SANDBOX_INTEGRATION=1 uv run pytest -s tests/test_sandbox_log_streaming_integration.py

format:
	uv ruff check .

build: 
	uv build

clean:
	rm -f dist/*

publish: clean build
	uv publish --token ${PYPI_TOKEN}
