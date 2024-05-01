.PHONY: docs tests

init:
	git submodule update --init --recursive

docs:
	@poetry run ./bin/docgen.py 2>/dev/null

tests:
	poetry run pytest -s
	# open .htmlcov/index.html

format:
	poetry run ruff check .

publish:
	poetry publish --build
