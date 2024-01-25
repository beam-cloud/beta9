# docgen

Use this tool to generate markdown for beta9's SDK Reference documentation page.

## How it works

It imports the classes defined in the project, then uses the inspect module to get the object's attributes. From there we access each object's `__doc__` attribute and use the `docstring_parser` library to parse the docstring.

If you need to add a new class, consider importing it in docgen.py and adding it to the `object_attributes` dict.

## Getting set up

You must have the minimum version of Python specified in [pyproject.toml](/pyproject.toml) and [Poetry](https://python-poetry.org/) the Python dependency/package management module.

Next, install all dependencies. This includes the "dev" dependencies.

```sh
poetry install
```

## Generate markdown

Run docgen to generate markdown.

```sh
# Generates markdown
./bin/docgen.py

# Generates markdown and copies to your clipboard (on macOS)
./bin/docgen.py | pbcopy
```
