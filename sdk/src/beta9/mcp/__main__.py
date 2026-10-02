# Helper processes run as `python -m beta9.mcp`. Run as `-m beta9.mcp.tools`,
# the package imports tools before runpy executes it again, and Python's
# warning about that lands in the job logs agents read.
from .tools import main

main()
