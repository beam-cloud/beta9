.PHONY: docs tests

init:
	git submodule update --init --recursive

docs:
	@poetry run ./bin/docgen.py 2>/dev/null

tests:
	BEAM_SERIALIZE_MODE=start poetry run pytest -s

format:
	poetry run black .
	poetry run ruff check .

publish:
	poetry publish --build
