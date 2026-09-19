"""Allows `python -m etl_craft ...` as an alternative to the installed `etl-craft` script."""

import sys

from etl_craft import main

if __name__ == "__main__":
    sys.exit(main())
